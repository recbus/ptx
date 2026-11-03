# ptx — Partial Transactions for Datomic

`ptx` lets you split a complex Datomic transaction into an ordered sequence of
*partial transactions* (ptxs) that compose. Everything is evaluated inside a single
transaction function, `com.recbus.ptx/|`, and the result is one ordinary
transaction.

## Why

Plain Datomic transaction functions don't compose. Their execution order isn't
guaranteed, and they can't see each other's output. Partial transactions fix
both problems:

* **Ordering**: ptxs are evaluated in sequence.
* **Communication**: each ptx is evaluated against a speculative database
  (`d/with`) containing the tx-data of all earlier ptxs. A ptx sees the tx-data
  proposed by earlier ptxs the same way it sees all other facts: in the database.

The speculative database is built by *replaying* the accumulated tx-data of all
earlier ptxs, as a single transaction, against the original database. Tempids
therefore unify across ptxs exactly as they do within one transaction, and each
ptx sees the same semantics the final transaction will have. When every ptx has
been evaluated, the tx-data they produced is returned and transacted as a single
transaction.

## Installation

`ptx` is distributed as a git dependency:

```clojure
io.github.recbus/ptx {:git/sha "<sha>"}
```

`ptx` does **not** bring in a Datomic client. Your application must supply one
that provides `datomic.client.api`, such as `com.datomic/client-cloud`,
`com.datomic/local`, or the Datomic Ions runtime.

### Datomic Cloud / Ions

The entry point is a classpath transaction function, so Datomic Cloud requires
it to be allow-listed in your `resources/datomic/ion-config.edn`:

```clojure
{:allow [com.recbus.ptx/|
         ;; ...
         ]}
```

Your ptx-fns (see below) are resolved by `ptx` itself, so they must be on the
classpath but do **not** need to be allow-listed.

## Usage

Call `com.recbus.ptx/|` in your tx-data, passing one or more ptxs:

```clojure
(require '[datomic.client.api :as d])

(d/transact conn {:tx-data [(list 'com.recbus.ptx/| ptx0 ptx1 ptx2)]})
```

A **ptx** is a collection of tx-data elements. Each element can be:

| Element | Example | Meaning |
|---|---|---|
| ptx-fn invocation | `(my.ns/enumerate-lines "order" ["l0" "l1"] 10)` | A list (or other seq) with a fully qualified symbol in function position. Calls the ptx-fn and recursively expands the ptx it returns. |
| Anything else | `{:db/id "order" :order/id 1}`, `[:db/add "order" :order/id 1]`, `[:db/cas …]` | Passed through unchanged and interpreted by Datomic, with Datomic's own semantics for map forms, nesting, lookup refs, tuples, retractions, etc. |

Sibling elements in a ptx are evaluated independently against the same
database. Each ptx is evaluated against the original database plus the tx-data
of all the ptxs before it.

Because each replay has the semantics of the final transaction, problems surface
at the ptx that causes them. For example, asserting a second value for a
cardinality-one attribute in a later ptx fails with a datom conflict, and a
`:db/cas` in a later ptx is compared against the *original* database, not
against values asserted by earlier ptxs.

### Writing ptx-fns

A ptx-fn is an ordinary Clojure function. It takes a `context` as its first
argument, followed by the arguments from the invocation, and returns a ptx.
The context contains:

* `:$`: the speculative database, including all facts from earlier ptxs.
* `:resolve`: a function that maps a tempid from an earlier ptx to its
  speculative entity id. Values it doesn't recognize are returned unchanged.
* `:unresolve`: the inverse of `:resolve`. It maps the speculative entity id of
  an entity created by an earlier ptx back to its tempid. Values it doesn't
  recognize are returned unchanged.
* `:tempids`: the tempid → speculative entity id map.

```clojure
(defn enumerate-lines
  "Number the lines of an order, continuing from its highest existing line number."
  [{:keys [$ resolve]} order line-tempids step]
  (let [order (resolve order)
        max-ln (or (ffirst (d/q {:query '{:find [(max ?ln)]
                                          :in [$ ?o]
                                          :where [[?o :order/lines ?l]
                                                  [?l :order.line/number ?ln]]}
                                 :args [$ order]}))
                   0)]
    (into []
          (map-indexed (fn [i l] [:db/add (resolve l) :order.line/number (+ max-ln (* step (inc i)))]))
          line-tempids)))
```

```clojure
(def ptx0 [{:db/id "order"
            :order/id 1000
            :order/lines [{:db/id "l0" :order.line/part-number "A"}
                          {:db/id "l1" :order.line/part-number "B"}]}])
(def ptx1 [(list `enumerate-lines "order" ["l0" "l1"] 10)])

(d/transact conn {:tx-data [(list 'com.recbus.ptx/| ptx0 ptx1)]})
```

Because `ptx1` runs after `ptx0`, the order and its lines are already in
`:$`, and `resolve` maps `"order"`, `"l0"` and `"l1"` to their speculative
entity ids.

#### Speculative entity ids in ptx-fn output

A speculative entity id is only valid in the speculative database it came from;
the final transaction has to refer to the same entity by its tempid. `ptx`
handles this automatically: every speculative entity id of a *new* entity in a
ptx-fn's output is replaced with its tempid. So `enumerate-lines` above may emit
`(resolve l)`, as shown, or just `l`; both produce `[:db/add "l0" …]`. Calling
`unresolve` yourself is optional, but makes the output explicit.

The replacement walks the output without regard to its structure. That is
almost always safe: a speculative entity id is only meaningful where an entity
is expected, and in any other position a coincidental match would turn a number
into a tempid string, which Datomic rejects wherever the schema types the value.
The exception is untyped data, such as the arguments of a classic tx-fn invoked
in vector form, where a (vanishingly unlikely) coincidental match would go
unnoticed.

Entity ids of entities that existed before the transaction (including upsert
targets) are valid everywhere and are left alone. Replacing them would break,
for example, a `:db/cas` on an upserted entity: Datomic evaluates `:db/cas`
before it resolves tempids by upsert.

### Wrapping classic tx-fns: `|+`

Use `com.recbus.ptx/|+` to call an existing Datomic-style transaction function
(`(f db & args)`) from inside a ptx. It is called with the current speculative
database. Tempids in its arguments are **not** resolved.

```clojure
[(list 'com.recbus.ptx/|+ `my.ns/autoinc "order" :order/id)]
```

### Errors

If a ptx fails, `ptx` throws an `ex-info` with the message
`"Partial transaction failed."`. Its cause is the underlying error, either from
a ptx-fn or from Datomic. Its data contains:

* `:com.recbus.ptx/ptx-index`: the (zero-based) position of the failing ptx.
* `:com.recbus.ptx/tx-data`: the expanded tx-data of the failing ptx, when the
  failure happened while applying it (absent if expansion itself failed).

### Limitations

* Each ptx replays the tx-data of all earlier ptxs, so the work grows
  quadratically with the number of ptxs. This is negligible for a handful of
  ptxs, but avoid long sequences of ptxs following a large one.
* A list (or other seq) with a qualified symbol in function position is always
  treated as a ptx-fn invocation. Classic Datomic tx-fns can be called with `|+`,
  or written as vectors so they pass through to Datomic.
* A ptx-fn can only refer to a new entity from an earlier ptx if that entity has
  a tempid. Entities created by maps without a `:db/id` (including nested
  component maps) have none, so emitting their speculative entity ids fails with
  Datomic's `:db.error/invalid-entity-id`. Give such entities a `:db/id`.

## Development

Run the tests (they use an in-memory Datomic Local database):

```shell
clojure -X:test:test-runner
```

## License

MIT. See [LICENSE](LICENSE).



## Developer Notes
(comment ["Decision Tree"
          ["Composition of partial transactions..."
           ["...implies tempid unification across partials..."
            ["...which requires arbitrary tx-fn re-writing..."
             ["...which requires data available only in the context..."
              ["...which amortizes a path to recursive partials."]]]]]])

(comment [["Goals:"
           ["Construct derivative transaction deltas (tx-data) that are functions of previously proposed tx-data and the current database."
            "Allow complex tx-data functions to be decomposed and recomposed."
            "Preserve the semantic model of existing transactions: monoidal accretion with applicative expansion of tx-data deltas."]]
          ["Anti-Goals:"
           ["* generalized (Turing Complete?) DSL for constructing datoms."
            "* detect & \"correct\" conflicting facts in a single transaction (leave that to transactor)."
            ["* improve transaction performance by pre-resolving transaction functions to primitive tx-data..."
             "...but it is a nice side-effect and could play very nicely with hints."]]]
          ["Semantic Preservation Restrictions: "
           ["a partial transaction is a collection of tx-data elements."
            "a partial transaction supports all Datomic tx-data (:db/add et al)."
            "a partial transaction is composed of either primitive Datomic assertions or expressions that evaluate to primitives."
            "sibling partials are evaluated independently."
            "sibling partial expressions have (read-only) access to the same with-db database."
            "partial transactions can only accrete elements onto their containing tx-data."]]
          ["Processing Model extension: recursively nested scopes."
           ["expressions produce proposed datoms."
            "partial transactions support functions whose first parameter is a context that includes the cumulatively accreted speculative database of its ancestors (and a tempid cross-reference map)."
            "an expression in a child scope has read access to the cumulative datoms proposed by ancestor scopes."
            "an expression cannot modify the contents of ancestor scopes."]]
          ["Restrictions:"
           ["Partial transactions must be ordered topologically by dependency."
            "Traditional transaction functions must be wrapped to forward-resolve tempid arguments."
            "The ptx evaluation environment is opt-in and must be entered by tx-data that invokes the `st.datomic.ptx/|` tx-fn with partial transaction (ptx) varargs."]]])

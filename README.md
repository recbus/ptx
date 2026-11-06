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

Each ptx is written as **ptx-data enclosed in a set**:

```clojure
(def ptx0 #{{:db/id "order" :order/id 1000}
            [:db/add "order" :order/description "Rush"]})
```

The set is required; any other argument to `|` is rejected. It expresses that
the elements of a ptx are independent of each other: they are evaluated against
the same database, in no particular order. Each element of ptx-data can be:

| Element | Example | Meaning |
|---|---|---|
| ptx-fn invocation | `(my.ns/enumerate-lines [:db/id "order"] [[:db/id "l0"] [:db/id "l1"]] 10)` | A list (or other seq) with a fully qualified symbol in function position. Calls the ptx-fn and recursively expands the tx-data it returns. |
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

A ptx-fn has the same signature as a classic Datomic tx-fn: `(f db & args)`.
`db` is the speculative database, including all facts from earlier ptxs, and
the ptx-fn returns tx-data (any collection, which may itself contain ptx-fn
invocations). Any classic tx-fn can therefore be invoked as a ptx-fn, and a
ptx-fn needs no knowledge of `ptx`.

```clojure
(defn enumerate-lines
  "Number the lines of an order, continuing from its highest existing line number."
  [db order lines step]
  (let [max-ln (or (ffirst (d/q {:query '{:find [(max ?ln)]
                                          :in [$ ?o]
                                          :where [[?o :order/lines ?l]
                                                  [?l :order.line/number ?ln]]}
                                 :args [db order]}))
                   0)]
    (into []
          (map-indexed (fn [i l] [:db/add l :order.line/number (+ max-ln (* step (inc i)))]))
          lines)))
```

#### Referring to tempids of earlier ptxs: `[:db/id "tempid"]`

The speculative database contains entity ids, not tempids, so a ptx-fn that
reads the database needs the speculative entity ids of entities created by
earlier ptxs. The author of the tx-data, who knows which arguments are tempids,
marks them at the call site as `[:db/id "tempid"]`:

```clojure
(def ptx0 #{{:db/id "order"
             :order/id 1000
             :order/lines [{:db/id "l0" :order.line/part-number "A"}
                           {:db/id "l1" :order.line/part-number "B"}]}})
(def ptx1 #{(list `enumerate-lines [:db/id "order"] [[:db/id "l0"] [:db/id "l1"]] 10)})

(d/transact conn {:tx-data [(list 'com.recbus.ptx/| ptx0 ptx1)]})
```

Before calling the ptx-fn, `ptx` replaces each mark with the speculative entity
id of its tempid, so `enumerate-lines` receives entity ids it can query with.
The rules:

* A mark is a two-element vector whose first element is `:db/id` and whose
  second element is a string. Other values, including bare tempid strings,
  `[:db/id :some/attr]` (e.g. in a pull pattern) and `:db/id` entries of map
  arguments, are left alone.
* Marks are resolved anywhere in the arguments of a ptx-fn invocation, however
  deeply nested, including in invocations emitted by other ptx-fns.
* A mark whose tempid was not used by an earlier ptx resolves to the bare
  tempid. (Sibling elements of the same ptx cannot see each other.)
* Marks are only resolved in ptx-fn arguments. Datomic has no meaning for
  `[:db/id …]`, so a mark in literal tx-data is rejected.

#### Speculative entity ids in ptx-fn output

A speculative entity id is only valid in the speculative database it came from;
the final transaction has to refer to the same entity by its tempid. `ptx`
handles this automatically: every speculative entity id of a *new* entity in a
ptx-fn's output is replaced with its tempid. So when `enumerate-lines` above
emits the entity ids it received, the result is `[:db/add "l0" …]`.

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

### Errors

If a ptx fails, `ptx` throws an `ex-info` with the message
`"Partial transaction failed."`. Its cause is the underlying error, either from
a ptx-fn, from Datomic, or `"ptx-data must be enclosed in a set."` for an
argument to `|` that is not a set. Its data contains:

* `:com.recbus.ptx/ptx-index`: the (zero-based) position of the failing ptx.
* `:com.recbus.ptx/tx-data`: the expanded tx-data of the failing ptx, when the
  failure happened while applying it (absent if expansion itself failed).

### Limitations

* Each ptx replays the tx-data of all earlier ptxs, so the work grows
  quadratically with the number of ptxs. This is negligible for a handful of
  ptxs, but avoid long sequences of ptxs following a large one.
* A list (or other seq) with a qualified symbol in function position is always
  treated as a ptx-fn invocation, evaluated by `ptx` against the speculative
  database. To have Datomic call a tx-fn instead (against the original
  database, without mark resolution), write the invocation as a vector.
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

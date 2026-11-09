(ns com.recbus.tdq
  "Transaction Delta Queries: Datalog queries optimized for querying the
  changes made by a transaction.

  A transaction delta query, or tdq, is evaluated against the three databases
  returned explicitly and implicitly by a successful call to either `d/with` or
  `d/transact`:

  * `$0`     -- The `db-before` of the tx result map.
  * `$delta` -- the history of the changes: `(d/history (d/since db-after t))`,
                where `t` is the basis of `db-before`.  This is equivalent to
                the `tx-data` of the tx result map.
  * `$`      -- The `db-after` of the tx result map.

  Datomic resolves idents (in entity, attribute and value position) against all
  three, so most of the work is Datomic's.

  `(q tx-result query)` or `(q tx-result query args)`

  `query` is a Datomic query in map form with one extra key, `:tdq/where`, and
  `args` are the values of its `:in` inputs, positionally.  tdq rewrites the
  query and returns exactly what `d/q` returns:

  * `:in` is prefixed with `$0 $delta $`: the caller's `:in` lists only its own
    (non-database) inputs.
  * `:tdq/where` is a vector of *sugared clauses* `[op e a v tx]`, matched
    against `$delta`, where `op` is `:db/add` (assertions), `:db/retract`
    (retractions), `_` (either) or a variable (bound to the added boolean).
    Trailing elements default to `_`.  A lookup ref in entity position is
    resolved against the database holding the matched datom: `$` for `:db/add`,
    `$0` for `:db/retract`.
  * `:where` is plain Datalog, passed through: unprefixed clauses run against
    `$` (`db-after`).  The rewritten `:tdq/where` clauses precede it.
  * Every other key (`:find`, `:keys`, `:with`, ...) is passed through.
  * An lrtid, `[:db/id \"tempid\"]`, anywhere in `query` or `args` is replaced
    by the tempid's entity id.

  For example, the VINs and previous colors of cars whose color was retracted:

      (q tx-result '{:find [?vin ?old]
                     :tdq/where [[:db/retract ?e :car/color ?old]]
                     :where [[?e :car/vin ?vin]]})

  Anything a sugared clause cannot express is written in `:where` against
  `$delta` explicitly, e.g. `(not [$delta ?e :car/color :red _ true])`.
  Generated variables start with `??`, which is reserved."
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [datomic.client.api :as d]))

(defn- fail [message data] (throw (ex-info message data)))

(defn- qvar? [x] (and (symbol? x) (str/starts-with? (name x) "?")))

(defn- generated-qvar? [x] (and (symbol? x) (str/starts-with? (name x) "??")))

(defn- src-var? [x] (and (symbol? x) (str/starts-with? (name x) "$")))

(defn- lookup-ref?
  "Is `x` a lookup ref?"
  [x]
  (and (vector? x)
       (= 2 (count x))
       (not (map-entry? x)) ; map entries can't be lookup refs and are cheap to exclude
       (keyword? (first x))))

(defn- lrtid?
  "Is `x` an lrtid, `[:db/id \"tempid\"]`?"
  [x]
  (and (lookup-ref? x)
       (= :db/id (first x))
       (string? (second x))))

(defn- resolve-lrtids
  "Replace the lrtids in `form` with their entity ids from `tempids`.  Throws on
  an unknown tempid."
  [tempids form]
  (walk/postwalk (fn [x]
                   (if (lrtid? x)
                     (let [[_ tempid] x]
                       (if-let [eid (tempids tempid)]
                         eid
                         (fail "Unresolvable tempid." {::tempid tempid})))
                     x))
                 form))

(defn- parse-sugared
  "Return the elements of the sugared clause `[op e a v tx]` as a map, with the
  op's added value (`true`, `false`, `_` or a variable) at `:added`.  Throws on
  an invalid clause."
  [[op & more :as clause]]
  (when (< 4 (count more))
    (fail "A sugared clause has at most five elements: [op e a v tx]." {::clause clause}))
  (let [added (case op
                :db/add true
                :db/retract false
                (if (or (= '_ op) (qvar? op))
                  op
                  (fail "A sugared clause must start with :db/add, :db/retract, _ or a variable."
                        {::clause clause})))
        [e a v tx] (concat more (repeat '_))]
    {:op op :added added :e e :a a :v v :tx tx}))

(defn- desugar
  "Reducing function (for `transduce`) desugaring parsed sugared clauses into
  `:where` clauses, joining each lookup ref once."
  ([] [{} []])
  ([[_ where]] where)
  ([[lrs where] {:keys [op added e a v tx] :as parsed-sugared-clause}]
   (if (lookup-ref? e)
     (let [db (case op
                :db/add '$
                :db/retract '$0
                (fail "A lookup ref requires :db/add or :db/retract." {::parsed-sugared-clause parsed-sugared-clause}))
           pattern (fn [lr] ['$delta lr a v tx added])]
       (if-let [lr (lrs [db e])]
         [lrs (conj where (pattern lr))]
         (let [lr (symbol (str "??lr" (count lrs)))]
           [(assoc lrs [db e] lr) (conj where (into [db lr] e) (pattern lr))])))
     [lrs (conj where ['$delta e a v tx added])])))

(defn rewrite
  "Return the Datomic query for the delta query `query`, resolving lrtids
  against `tempids`.  Throws on an invalid query."
  [query tempids]
  (when-not (map? query)
    (fail "A delta query must be a map." {::query query}))
  (let [{:keys [in where] tdq-where :tdq/where :as query} (resolve-lrtids tempids query)]
    (when-not (or (nil? tdq-where) (vector? tdq-where))
      (fail "The :tdq/where clauses must be a vector." {::clauses tdq-where}))
    (when-let [v (first (filter generated-qvar? (tree-seq coll? seq query)))]
      (fail "Variables starting with ?? are reserved." {::variable v}))
    (when-let [input (first (filter src-var? in))]
      (fail "Database inputs are not supported." {::input input}))
    (-> query
        (dissoc :tdq/where)
        (assoc :in (into '[$0 $delta $] in)
               :where (into (transduce (comp (map (fn [sugared-clause]
                                                    (if (vector? sugared-clause)
                                                      sugared-clause
                                                      (fail "A sugared clause must be a vector: [op e a v tx]." {::sugared-clause sugared-clause}))))
                                             (map parse-sugared))
                                       desugar
                                       tdq-where) where)))))

(defn- delta-db
  "The history database of the changes made by the transaction of `tx-result`."
  [{:keys [db-before db-after]}]
  (d/history (d/since db-after (:t db-before))))

(defn q
  "Evaluate the delta query `query`, with positional `args`, against the
  transaction result `tx-result` and return the result of `d/q`."
  ([tx-result query] (q tx-result query []))
  ([{:keys [db-before db-after tempids] :as tx-result} query args]
   (d/q {:query (rewrite query tempids)
         :args (into [db-before (delta-db tx-result) db-after] (resolve-lrtids tempids args))})))

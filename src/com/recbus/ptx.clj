(ns com.recbus.ptx
  "A partial transaction feature implemented within a single Datomic tx-fn.

  Partial transactions allow the decomposition of complex transactions into a
  sequence of _partial transactions_ (ptxs).  The power of ptxs is most apparent
  when multiple transaction functions need to be composed.  In a plain Datomic
  transaction, transaction functions have no means of being composed: there is
  no guarantee about the execution order and there is no communications channel
  between them.  In contrast, ptxs do guarantee ordering and do have a means
  of communicating: the database itself.

  The name `ptx` could as well mean \"pseudo-transaction\" since a ptx is only
  ever transacted in isolation speculatively.  The resulting with-db database
  forms the input to the next ptx, and so on.  A ptx exists only to produce
  tx-data and only sees the tx-data of a preceding ptx the same way it sees
  *all* facts: the database.

  Collectively, the tx-data from the execution of a sequence of ptxs is
  collected into a single tx-data result which can then be properly transacted."
  (:require [clojure.set]
            [datomic.client.api :as d]))

(defn- resolve-v-tempids
  [{:keys [refs trefs]} resolve a v]
  (cond
    (refs a) (resolve v)
    (trefs a) (let [i (trefs a)
                    v' (v i)] ; TODO: double-t-nested refs?
                (assoc v i (resolve v')))
    :else v))

(defn- resolve-primitive-datom
  [schema resolve [cmd e a v :as x]]
  (if (= 3 (count x)) ; support short-form :db/retract without a v
    [cmd (resolve e) a]
    [cmd (resolve e) a (resolve-v-tempids schema resolve a v)]))

(defmulti resolve-tx-datum
  "Transform the tx-datum by replacing all tempids with their realized db ids."
  (fn [_schema _resolve [cmd]] cmd))
(defmethod resolve-tx-datum :db/add [& args] (apply resolve-primitive-datom args))

;; Do not resolve retractions. Retractions on tempids are no-ops, even if the entity exists prior to
;; the transaction, so there's no possible utility to resolving a :db/id in a retraction.
(defmethod resolve-tx-datum :db/retract [_ _ assertion] assertion)
(defmethod resolve-tx-datum :db/retractEntity [_ _ [cmd e]] [cmd e])

(defmethod resolve-tx-datum :db/cas
  [schema resolve [cmd e a v0 v1]]
  [cmd (resolve e) a (resolve-v-tempids schema resolve a v0) (resolve-v-tempids schema resolve a v1)])

(defn- lookup-ref?
  [v]
  (and (vector? v)
       (= 2 (count v))
       (-> v first qualified-keyword?)
       (-> v second some?)))

(declare reduce-entity)
(defn reduce-map-entry
  [{:keys [tuples refs cardinality-many] :as schema} peid [k v]]
  (cond
    (map? v) (reduce-entity schema peid k v)
    (coll? v) (cond (tuples k) [[:db/add peid k v]] ; no support for composite tuples with refs
                    (refs k) (cond
                               (lookup-ref? v) [[:db/add peid k v]]
                               (cardinality-many k) (into [] (mapcat (fn [v] (reduce-entity schema peid k v)) v))
                               :else (throw (ex-info "Unhandled ref value!" {:peid peid :k k :v v :schema schema})))
                    (cardinality-many k) (into [] (map (fn [v] [:db/add peid k v]) v))
                    :else (throw (ex-info "Unhandled collection!" {:peid peid :k k :v v :schema schema})))
    :else [[:db/add peid k v]]))

(def eid? (some-fn nat-int? keyword? lookup-ref? string?))

(defn reduce-entity
  [schema peid pk v]
  (cond
    (map? v) (let [{eid :db/id :or {eid (str (gensym "$$auto-tempid-"))} :as m} v
                   binds (if peid [[:db/add peid pk eid]] [])]
               (into binds (mapcat (fn [me] (reduce-map-entry schema eid me)) (dissoc m :db/id))))
    (eid? v) [[:db/add peid pk v]]))

(defmulti expand (fn [_context x] (class x)))
(defmethod expand clojure.lang.IPersistentMap [{:keys [schema]} m] (reduce-entity schema nil nil m))
(defmethod expand clojure.lang.IPersistentVector [_context v] [v]) ; primitive
(defmethod expand clojure.lang.IPersistentList
  [context [op & args :as expression]]
  (if-let [f (requiring-resolve op)]
    (let [ptx (apply f context args)]
      (into [] (mapcat (partial expand context)) ptx))
    (throw (ex-info (format "The ptx transaction function %s was not resolved." op) {:expression expression}))))
(defmethod expand clojure.lang.LazySeq ; the tx processor in Cloud turns tx-data lists into lazy seqs apparently.
  [context args]
  (expand context (apply list args)))

(defn |+
  "A ptx-fn that wraps standard Datomic classpath tx-fns that have no dependency
  on tempids."
  [{:keys [$]} f & args]
  (let [f (requiring-resolve f)]
    (apply f $ args)))

(defn ||
  "A ptx-fn that generates Datomic transaction data (tx-data) from a sequence of
  partial transactions `ptxs`."
  [context & ptxs]
  (try (let [apply-ptx (fn apply-ptx ; TODO: virtual attributes
                         [[{:keys [$ schema tempids resolve] :as context} tx-data] ptx]
                         (let [tx-data' (sequence (mapcat (partial expand context)) ptx) ; recursive!
                               resolve-tx-datum (partial resolve-tx-datum schema resolve)
                               resolved-tx-data (sequence (map resolve-tx-datum) tx-data')
                               tx-report (try (d/with $ {:tx-data resolved-tx-data})
                                              (catch Exception e
                                                (throw (ex-info "Partial transaction failed."
                                                                {::tx-data tx-data'
                                                                 ::resolved-tx-data resolved-tx-data}
                                                                e))))
                               tempids (merge tempids (:tempids tx-report))]
                           [(assoc context
                                   :$ (:db-after tx-report)
                                   :tempids tempids
                                   :resolve (fn resolve-eid [eid] (tempids eid eid)))
                            (concat tx-data tx-data')]))
             [{:keys [tempids schema]} tx-data] (reduce
                                                 apply-ptx
                                                 [context []]
                                                 ptxs)
             reverse-resolve (fn reverse-resolve [eid] ((clojure.set/map-invert tempids) eid eid))]
         (into [] (map (partial resolve-tx-datum schema reverse-resolve)) tx-data))
       (catch Exception e
         (throw (Exception. "Partial transaction application failed." e)))))

(defn- schema
  "Return a summary of the database schema essential for the evaluation of
  partial transactions.  It is computed once per invocation of `|`."
  [db]
  (let [attrs (d/q {:query '{:find [?type ?attr ?card]
                             :in [$]
                             :where [[?a :db/valueType ?t]
                                     [?a :db/cardinality ?c]
                                     [?c :db/ident ?card]
                                     [?t :db/ident ?type]
                                     [?a :db/ident ?attr]]}
                    :args [db]})
        trefs (into {}
                    (map (fn [[tattr attr tattrs]] [tattr (.indexOf tattrs attr)]))
                    (d/q {:query '{:find [?tattr ?a ?as]
                                   :in [$]
                                   :where [[?ta :db/valueType :db.type/tuple]
                                           [?ta :db/tupleAttrs ?as]
                                           [?ta :db/ident ?tattr]
                                           [(untuple ?as) [?a ...]]
                                           [?a :db/valueType :db.type/ref]
                                           [?a :db/ident ?attr]]}
                          :args [db]}))
        by-type (group-by first attrs)
        by-card (group-by (fn [[_ _ c]] c) attrs)]
    {:refs (into #{} (map second) (:db.type/ref by-type))
     :tuples (into #{} (map second) (:db.type/tuple by-type))
     :cardinality-many (into #{} (map second) (:db.cardinality/many by-card))
     :cardinality-one (into #{} (map second) (:db.cardinality/one by-card))
     :trefs trefs}))

;; The sole entry point.
(defn |
  "A Datomic tx-fn that generates tx-data from `ptxs`, a sequence of partial
  transactions."
  [$ & ptxs]
  (let [context {:$ $
                 :schema (schema $) ; performance optimization; a function of $ only.
                 :tempids {}
                 :resolve identity}]
    (apply || context ptxs)))

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
            "The ptx evaluation environment is opt-in and must be entered by tx-data that invokes the `com.recbus.ptx/|` tx-fn with partial transaction (ptx) varargs."]]])

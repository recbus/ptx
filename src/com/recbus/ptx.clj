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
  ever transacted speculatively.  Each ptx is evaluated against the database
  produced by *replaying* the tx-data of all preceding ptxs, as a single
  transaction, against the original database.  A ptx exists only to produce
  tx-data and only sees the tx-data of a preceding ptx the same way it sees
  *all* facts: the database.

  Because every replay applies the accumulated tx-data as one transaction
  against the original database, tempids unify natively and the speculative
  database seen by each ptx has exactly the semantics of the final transaction.

  Each ptx is written as ptx-data enclosed in a set: tx-data elements and ptx-fn
  invocations.  The set expresses that sibling elements are independent; they
  are evaluated against the same database, in no particular order.

  A ptx-fn has the same signature as a classic Datomic tx-fn, `(f db & args)`,
  where `db` is the speculative database.  Arguments that refer to tempids of
  earlier ptxs are marked at the call site as `[:db/id \"tempid\"]` and are
  resolved to speculative entity ids before the ptx-fn is called.

  Collectively, the tx-data from the evaluation of a sequence of ptxs is
  collected into a single tx-data result which can then be properly transacted."
  (:require [clojure.walk :as walk]
            [datomic.client.api :as d]))

(defn- invocation?
  "Is `x` a ptx-fn invocation: a list (or other seq, e.g. the lazy seqs produced
  by the Datomic Cloud tx processor) with a qualified symbol in function position?"
  [x]
  (and (seq? x) (qualified-symbol? (first x))))

(defn- mark?
  "Is `x` a tempid mark: a two-element vector `[:db/id \"tempid\"]`?  Map entries
  are excluded so that map arguments containing a `:db/id` are left alone, and
  only strings qualify so that, e.g., pull patterns are left alone."
  [x]
  (and (vector? x)
       (not (map-entry? x))
       (= 2 (count x))
       (= :db/id (first x))
       (string? (second x))))

(defn- resolve-marks
  "Replace every tempid mark in `args` with the speculative entity id of its
  tempid, or with the bare tempid if no earlier ptx used it."
  [tempids args]
  (walk/postwalk (fn [x] (if (mark? x) (let [tempid (second x)] (get tempids tempid tempid)) x))
                 args))

(declare expand)

(defn- invoke
  "Call the ptx-fn of `expression` with the speculative database of `state` and
  the expression's arguments (marks resolved), and recursively expand the ptx it
  returns."
  [{:keys [db tempids] :as state} [op & args :as expression]]
  (let [f (or (requiring-resolve op)
              (throw (ex-info (format "The ptx-fn %s was not resolved." op)
                              {::expression expression})))]
    (into [] (mapcat (partial expand state)) (apply f db (resolve-marks tempids args)))))

(defn- expand
  "Expand the ptx element `x` into tx-data.  ptx-fn invocations are evaluated;
  every other element passes through unchanged for Datomic to interpret."
  [state x]
  (if (invocation? x) (invoke state x) [x]))

(defn- unresolve-all
  "Replace every speculative eid in the tx-data `x` with its tempid.

  The walk is deliberately blind to the structure of `x`: every integer is
  checked, regardless of position.  A speculative eid is only meaningful in an
  entity position, where replacing it with its tempid is exactly right.  In any
  other position a (vanishingly unlikely) collision becomes a tempid string where
  Datomic expects a non-entity value, which Datomic rejects loudly wherever the
  schema types the value.  The exception is untyped data, such as the arguments
  of a classic tx-fn invoked in vector form, where a collision would go unnoticed.

  Only the speculative eids of *new* entities are replaced.  The eids of
  pre-existing entities (including upsert targets) are valid everywhere and are
  left alone; replacing them would break, e.g., `:db/cas`, which Datomic
  evaluates before resolving tempids by upsert."
  [tempid-of x]
  (if (empty? tempid-of)
    x
    (walk/postwalk (fn [v] (if (integer? v) (get tempid-of v v) v)) x)))

(defn- expand-ptx
  "Expand `ptx` into tx-data.  The output of each ptx-fn invocation has its
  speculative eids mapped back to tempids; literal tx-data cannot contain
  speculative eids and is passed through untouched."
  [{:keys [tempid-of] :as state} ptx]
  (into []
        (mapcat (fn [x] (if (invocation? x)
                          (unresolve-all tempid-of (invoke state x))
                          [x])))
        ptx))

(defn- existing-entity?
  "Does entity `eid` have any datoms in `db`?"
  [db eid]
  (boolean (seq (d/datoms db {:index :eavt :components [eid] :limit 1}))))

(defn- replay-state
  "Build the state for evaluating the next ptx from the speculative transaction
  report of a replay: the speculative database, the tempid -> speculative eid map,
  and `tempid-of`, the map from the speculative eids of *new* entities to their
  tempids.  `existing?` is a predicate identifying entities that exist in the
  original database."
  [existing? {:keys [db-after tempids]}]
  {:db db-after
   :tempids tempids
   :tempid-of (into {}
                    (keep (fn [[tempid eid]] (when-not (existing? eid) [eid tempid])))
                    tempids)})

(defn- fail
  [index tx-data cause]
  (throw (ex-info "Partial transaction failed."
                  (cond-> {::ptx-index index} tx-data (assoc ::tx-data tx-data))
                  cause)))

(defn- apply-ptxs
  "Evaluate `ptxs` in order against `db` and return the accumulated tx-data.  Each
  ptx is evaluated against `db` with the tx-data of all preceding ptxs applied as
  a single (speculative) transaction."
  [db ptxs]
  (let [existing? (memoize (partial existing-entity? db))]
    (:tx-data (reduce (fn [{:keys [tx-data] :as state} [index ptx]]
                        (let [tx-data' (try (when-not (set? ptx)
                                              (throw (ex-info "ptx-data must be enclosed in a set." {::ptx ptx})))
                                            (expand-ptx state ptx)
                                            (catch Exception e (fail index nil e)))
                              tx-data (into tx-data tx-data')
                              report (try (d/with db {:tx-data tx-data})
                                          (catch Exception e (fail index tx-data' e)))]
                          (assoc (replay-state existing? report) :tx-data tx-data)))
                      (assoc (replay-state existing? {:db-after db :tempids {}}) :tx-data [])
                      (map-indexed vector ptxs)))))

;; The sole entry point.
(defn |
  "A Datomic tx-fn that generates tx-data from `ptxs`, a sequence of partial
  transactions.  Each ptx is ptx-data enclosed in a set."
  [db & ptxs]
  (apply-ptxs db ptxs))

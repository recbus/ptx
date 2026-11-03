(ns com.recbus.ptx-test
  (:require [backtick :refer [template]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [com.recbus.ptx :as sut]
            [datomic.client.api :as d]
            [matcher-combinators.test]
            [matcher-combinators.matchers :as m]))

(def ^:dynamic *db*
  "An empty with-db database value, bound by `with-db-fixture`."
  nil)

(defn- with-db-fixture
  "Create an ephemeral in-memory Datomic Local database and bind a with-db
  value of it to `*db*` for the duration of the tests."
  [f]
  (let [client (d/client {:server-type :datomic-local
                          :system "ptx-test"
                          :storage-dir :mem})
        db-name (str "ptx-test-" (random-uuid))]
    (d/create-database client {:db-name db-name})
    (try
      (binding [*db* (d/with-db (d/connect client {:db-name db-name}))]
        (f))
      (finally
        (d/delete-database client {:db-name db-name})))))

(use-fixtures :once with-db-fixture)

;;; Fixtures

(def ^:private schema
  [{:db/ident :order/id :db/valueType :db.type/long :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}
   {:db/ident :order/supplier :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :order/origin :db/valueType :db.type/tuple :db/tupleTypes [:db.type/string :db.type/string] :db/cardinality :db.cardinality/one}
   {:db/ident :order/supplier+rank :db/valueType :db.type/tuple :db/tupleTypes [:db.type/ref :db.type/long] :db/cardinality :db.cardinality/one}
   {:db/ident :order/tags :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   {:db/ident :order/related :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   {:db/ident :order/description :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :order/lines :db/valueType :db.type/ref :db/isComponent true :db/cardinality :db.cardinality/many}
   {:db/ident :order.line/order :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :order.line/part-number :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :supplier/id :db/valueType :db.type/long :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}])

(def ^:private data
  [{:db/id "supplierA" :supplier/id 1 :db/ident :test/supplierA}
   {:db/id "supplierB" :supplier/id 2 :db/ident :test/supplierB}
   {:db/ident :related/A}
   {:db/ident :related/B}
   {:order/id 0 :order/description "Test Order 0" :order/supplier "supplierA"}])

(defn- with-tx [db tx-data] (:db-after (d/with db {:tx-data tx-data})))

(defn- test-db [] (-> *db* (with-tx schema) (with-tx data)))

(defn- eid [db lookup-ref] (:db/id (d/pull db [:db/id] lookup-ref)))

(defn- transact-ptxs
  "Transact `ptxs` through the `|` tx-fn against `db` and return the db-after."
  [db & ptxs]
  (with-tx db [(apply list `sut/| ptxs)]))

(defn- failure
  "Evaluate `ptxs` against `db` and return a summary of the resulting failure."
  [db & ptxs]
  (try (apply sut/| db ptxs)
       nil
       (catch clojure.lang.ExceptionInfo e
         {:message (ex-message e)
          :data (ex-data e)
          :cause (some-> e ex-cause ex-message)})))

;;; ptx-fns used by the tests

(defn f0 [_context tx] tx)

(defn emit-resolved
  "Emit an assertion using the resolved (speculative) eids of `e` and `v`."
  [{:keys [resolve]} e a v]
  [[:db/add (resolve e) a (resolve v)]])

(defn emit-resolved-map
  "Like `emit-resolved`, but in map form."
  [{:keys [resolve]} e a v]
  [{:db/id (resolve e) a (resolve v)}])

(defn emit-resolved-tuple
  "Emit a tuple assertion with the resolved (speculative) eid of `s` as a ref component."
  [{:keys [resolve]} e s rank]
  [[:db/add (resolve e) :order/supplier+rank [(resolve s) rank]]])

(defn emit-resolved-cas
  "Emit a :db/cas using the resolved eid of `e`."
  [{:keys [resolve]} e a old new]
  [[:db/cas (resolve e) a old new]])

(defn describe
  "Read the :order/id of `e` from the speculative database and describe it."
  [{:keys [$ resolve]} e]
  (let [{:order/keys [id]} (d/pull $ [:order/id] (resolve e))]
    [[:db/add e :order/description (str "Order " id)]]))

(defn describe-by-lookup
  "Find the eid of the order identified by `order-id` and emit a description against it."
  [{:keys [$]} order-id]
  [[:db/add (eid $ [:order/id order-id]) :order/description "found by lookup"]])

(def ^:private captured (atom nil))

(defn capture-unresolve
  [{:keys [resolve unresolve]} tempid existing-eid]
  (reset! captured {:new (unresolve (resolve tempid))
                    :existing (unresolve existing-eid)
                    :other (unresolve :some/ident)})
  [])

(defn boom [_context] (throw (ex-info "boom" {})))

;;; Tests

(deftest literal-tx-data-passes-through
  (let [db (test-db)]
    (testing "primitives"
      (let [ptx0 [[:db/add "order" :order/id 1000]
                  [:db/retract [:order/id 0] :order/description "Test Order 0"]
                  [:db/cas [:order/id 0] :order/description "Test Order 0" "Changed"]]]
        (is (= ptx0 (sut/| db ptx0)))))
    (testing "map forms, including nesting, cardinality-many, tuples and lookup refs"
      (let [ptx0 [{:db/id "order"
                   :order/id 1000
                   :order/origin ["US" "TX"]
                   :order/tags [:order/big :order/important]
                   :order/related [[:supplier/id 1] [:supplier/id 2]]
                   :order/lines [{:order.line/part-number "A"}]}]]
        (is (= ptx0 (sut/| db ptx0)))))
    (testing "the tx-data of multiple ptxs is concatenated"
      (let [ptx0 [{:db/id "order0" :order/id 1000}]
            ptx1 [{:db/id "order1" :order/id 1001}]]
        (is (= (into ptx0 ptx1) (sut/| db ptx0 ptx1)))))))

(deftest ptx-fn-expansion
  (let [db (test-db)]
    (testing "invocation as a list"
      (is (= [{:db/id "order" :order/id 1000}
              [:db/add "order" :order/description "Test"]]
             (sut/| db [{:db/id "order" :order/id 1000}
                        (list `f0 [[:db/add "order" :order/description "Test"]])]))))
    (testing "invocation as a lazy seq, as produced by the Datomic Cloud tx processor"
      (is (= [[:db/add "order" :order/description "Test"]]
             (sut/| db [(lazy-seq (list `f0 [[:db/add "order" :order/description "Test"]]))]))))
    (testing "nested invocations are expanded recursively"
      (is (= [[:db/add "order" :order/description "Test"]]
             (sut/| db [(list `f0 [(list `f0 [[:db/add "order" :order/description "Test"]])])]))))
    (testing "an unresolvable ptx-fn fails"
      (is (match? {:message "Partial transaction failed."
                   :data {::sut/ptx-index 0}
                   :cause #"ptx-fn com.recbus.ptx-test/no-such-fn was not resolved"}
                  (failure db [(list `no-such-fn)]))))))

(deftest datomic-interprets-tx-data
  (let [db (test-db)]
    (testing "a cardinality-many ref of idents is not mistaken for a lookup ref"
      (let [db' (transact-ptxs db [{:order/id 1000 :order/related [:related/A :related/B]}])]
        (is (= #{:related/A :related/B}
               (into #{} (map :db/ident) (:order/related (d/pull db' [{:order/related [:db/ident]}] [:order/id 1000])))))))
    (testing "reverse refs are interpreted by Datomic"
      (let [db' (transact-ptxs db [{:db/id "line" :order.line/part-number "X"}
                                   {:db/id "order" :order/id 1000 :order.line/_order "line"}])]
        (is (= [{:order.line/part-number "X"}]
               (:order.line/_order (d/pull db' [{:order.line/_order [:order.line/part-number]}] [:order/id 1000]))))))
    (testing "invalid values are rejected, not silently dropped"
      (is (match? {:data {::sut/ptx-index 0} :cause #"invalid-lookup-ref"}
                  (failure db [{:order/id 1000 :order/related [[:related/A]]}]))))
    (testing "classic tx-fns written as vectors pass through to Datomic"
      (let [ptx0 [{:db/id "o" :order/description "classic"}]
            ptx1 [[`autoinc "o" :order/id]]]
        (is (= (into ptx0 ptx1) (sut/| db ptx0 ptx1)))
        (is (= "classic" (:order/description (d/pull (transact-ptxs db ptx0 ptx1) [:order/description] [:order/id 1]))))))
    (testing "a nested non-component entity without :db/id is rejected"
      (is (match? {:data {::sut/ptx-index 0} :cause #"invalid-nested-entity"}
                  (failure db [{:order/id 1000 :order/supplier {:order/description "orphan"}}]))))))

(deftest composition
  (let [db (test-db)]
    (testing "tempids unify across ptxs in E position"
      (let [db' (transact-ptxs db [{:db/id "o" :order/id 1000}] [{:db/id "o" :order/description "Later"}])]
        (is (= "Later" (:order/description (d/pull db' [:order/description] [:order/id 1000]))))))
    (testing "tempids unify across ptxs in V position"
      (let [db' (transact-ptxs db [{:db/id "s" :supplier/id 3}] [{:order/id 1000 :order/supplier "s"}])]
        (is (= {:order/supplier {:supplier/id 3}}
               (d/pull db' [{:order/supplier [:supplier/id]}] [:order/id 1000])))))
    (testing "a ptx sees the tx-data of preceding ptxs in its database"
      (let [db' (transact-ptxs db [{:db/id "o" :order/id 1000}] [(list `describe "o")])]
        (is (= "Order 1000" (:order/description (d/pull db' [:order/description] [:order/id 1000]))))))
    (testing "sibling elements see the same database"
      (is (= [[:db/add "o" :order/description "Order "]
              [:db/add "o" :order/id 1000]]
             (sut/| db [(list `describe "o") [:db/add "o" :order/id 1000]]))))
    (testing "conflicting datoms fail at the ptx that introduces the conflict"
      (is (match? {:data {::sut/ptx-index 1
                          ::sut/tx-data [{:db/id "o" :order/description "Second"}]}
                   :cause #"conflict"}
                  (failure db
                           [{:db/id "o" :order/id 1000 :order/description "First"}]
                           [{:db/id "o" :order/description "Second"}]))))
    (testing ":db/cas is evaluated against the original database, as in the final transaction"
      (is (match? {:data {::sut/ptx-index 1} :cause #"cas-failed|Compare failed"}
                  (failure db
                           [[:db/add [:order/id 0] :order/description "a"]]
                           [[:db/cas [:order/id 0] :order/description "a" "b"]])))
      (let [db' (transact-ptxs db
                               [{:order/id 1000}]
                               [[:db/cas [:order/id 0] :order/description "Test Order 0" "Changed"]])]
        (is (= "Changed" (:order/description (d/pull db' [:order/description] [:order/id 0]))))))
    (testing "short-form and long-form retractions of pre-existing data"
      (let [db' (transact-ptxs db
                               [[:db/retract [:order/id 0] :order/description]]
                               [[:db/retract [:order/id 0] :order/supplier :test/supplierA]])]
        (is (= {:order/id 0} (d/pull db' [:order/id :order/description :order/supplier] [:order/id 0])))))
    (testing "lookup refs in a cardinality-many ref"
      (let [db' (transact-ptxs db [{:order/id 1000 :order/related [[:supplier/id 1] [:supplier/id 2]]}])]
        (is (= #{1 2}
               (into #{} (map :supplier/id) (:order/related (d/pull db' [{:order/related [:supplier/id]}] [:order/id 1000])))))))))

(deftest speculative-eids
  (let [db (test-db)
        ptx0 [{:db/id "o" :order/id 1000} {:db/id "s" :supplier/id 3}]]
    (testing "speculative eids emitted by ptx-fns are mapped back to tempids (E and ref V)"
      (let [ptx1 [(list `emit-resolved "o" :order/supplier "s")]]
        (is (= [:db/add "o" :order/supplier "s"] (last (sut/| db ptx0 ptx1))))
        (is (= {:order/supplier {:supplier/id 3}}
               (d/pull (transact-ptxs db ptx0 ptx1) [{:order/supplier [:supplier/id]}] [:order/id 1000])))))
    (testing "... in map form"
      (is (= {:db/id "o" :order/supplier "s"}
             (last (sut/| db ptx0 [(list `emit-resolved-map "o" :order/supplier "s")])))))
    (testing "... in ref components of tuples"
      (let [ptx1 [(list `emit-resolved-tuple "o" "s" 1)]
            db' (transact-ptxs db ptx0 ptx1)]
        (is (= [:db/add "o" :order/supplier+rank ["s" 1]] (last (sut/| db ptx0 ptx1))))
        (is (= [(eid db' [:supplier/id 3]) 1]
               (:order/supplier+rank (d/pull db' [:order/supplier+rank] [:order/id 1000]))))))
    (testing "non-entity values are left alone"
      (is (= [:db/add "o" :order/description "s"]
             (last (sut/| db ptx0 [(list `emit-resolved "o" :order/description "s")])))))
    (testing "eids of pre-existing entities (including upsert targets) are left alone"
      (let [e0 (eid db [:order/id 0])]
        (is (= [:db/add e0 :order/description "x"]
               (last (sut/| db [{:db/id "o" :order/id 0}] [(list `emit-resolved "o" :order/description "x")]))))))
    (testing "... which keeps :db/cas on an upsert target working"
      (let [e0 (eid db [:order/id 0])
            ptx0 [{:db/id "o" :order/id 0}]
            ptx1 [(list `emit-resolved-cas "o" :order/description "Test Order 0" "Changed")]]
        (is (= [:db/cas e0 :order/description "Test Order 0" "Changed"] (last (sut/| db ptx0 ptx1))))
        (is (= "Changed" (:order/description (d/pull (transact-ptxs db ptx0 ptx1) [:order/description] [:order/id 0]))))))
    (testing ":unresolve is available to ptx-fns"
      (let [e0 (eid db [:order/id 0])]
        (sut/| db ptx0 [(list `capture-unresolve "o" e0)])
        (is (= {:new "o" :existing e0 :other :some/ident} @captured))))
    (testing "a new entity without a tempid cannot be referenced by a speculative eid"
      (is (match? {:data {::sut/ptx-index 1} :cause #"invalid-entity-id|Invalid entity id"}
                  (failure db [{:order/id 1000}] [(list `describe-by-lookup 1000)]))))))

(deftest distinguishable-exceptions
  (let [db (test-db)
        ptx0 [{:db/id "order0" :order/id 1000}]
        ptx1 [[:db/add [:order/id 1001] :order/tags :order/big]]]
    (testing "failures are attributed to the responsible ptx"
      (try
        (d/with db {:tx-data [(list `sut/| ptx0 ptx1)]})
        (is false "expected an exception")
        (catch Exception e
          (is (match?
               (m/via Throwable->map
                      {:data {:cognitect.anomalies/category :cognitect.anomalies/incorrect
                              :cognitect.anomalies/message #"Unable to resolve entity.*"
                              :db/error :db.error/not-an-entity}
                       :via [{:message "Partial transaction failed."
                              :data {::sut/ptx-index 1
                                     ::sut/tx-data ptx1}}
                             {:data {:db/error :db.error/not-an-entity}}]})
               e)))))
    (testing "ptx-fn failures are attributed to the responsible ptx"
      (is (match? {:message "Partial transaction failed."
                   :data (m/equals {::sut/ptx-index 1})
                   :cause "boom"}
                  (failure db ptx0 [(list `boom)]))))))

;; Functions used to exercise ptx with real-world requirements
(defn autoinc
  "Return tx-data that asserts a value for the `attr` of `eid`
  that is one higher than the highest existing value in the
  database `db`."
  [db eid attr]
  (let [max-v (ffirst (d/q {:query '{:find [(max ?v)]
                                     :in [$ ?attr]
                                     :where [[_ ?attr ?v]]}
                            :args [db attr]}))]
    [[:db/add eid attr (inc (or max-v -1))]]))

(defn enumerate-lines
  "Return tx-data that asserts a unique (in the conxtext of the order
  identified by `order-eid`) sequential value for `:order.line/number`
  for each line identified by `line-eids` starting at `step` and
  incrementing by `step`."
  [{:keys [$ resolve] :as context} order-eid line-eids step]
  (let [order-eid (resolve order-eid)
        max-ln (or (ffirst (d/q {:query '{:find [(max ?ln)]
                                          :in [$ ?order]
                                          :where [[?order :order/lines ?l]
                                                  [?l :order.line/number ?ln]]}
                                 :args [$ order-eid]}))
                   0)]
    (into []
          (map-indexed (fn [i e] [:db/add (resolve e) :order.line/number (+ max-ln (* step (inc i)))]))
          line-eids)))

(defn identify-line
  "Return tx-data that asserts the tuple `:order.line/order-id+number` for
  the line identified by `line` belonging to the order identified by `order`."
  [{:keys [$ resolve] :as context} order line]
  (let [order (resolve order)
        line (resolve line)
        tuple (first (d/q {:query '{:find [?order-id ?line-number]
                                    :in [$ [?o ?l]]
                                    :where [[?o :order/id ?order-id]
                                            [?l :order.line/number ?line-number]]}
                           :args [$ [order line]]}))]
    [[:db/add line :order.line/order-id+number tuple]]))

(deftest datomic-transaction-putting-it-all-together
  (as-> *db* %
    ;; install some test attributes
    (d/with % {:tx-data [{:db/ident :order/id
                          :db/valueType :db.type/long
                          :db/unique :db.unique/identity
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :order/supplier
                          :db/valueType :db.type/ref
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :order/origin
                          :db/valueType :db.type/tuple
                          :db/tupleTypes [:db.type/string :db.type/string]
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :order/tags
                          :db/valueType :db.type/keyword
                          :db/cardinality :db.cardinality/many}
                         {:db/ident :order/related
                          :db/valueType :db.type/ref
                          :db/cardinality :db.cardinality/many}
                         {:db/ident :order/description
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :order/created-by
                          :db/valueType :db.type/ref
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :order/lines
                          :db/valueType :db.type/ref
                          :db/isComponent true
                          :db/cardinality :db.cardinality/many}
                         {:db/ident :order.line/part-number
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :order.line/number
                          :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :order.line/order-id+number
                          :db/valueType :db.type/tuple
                          :db/tupleTypes [:db.type/long :db.type/long]
                          :db/unique :db.unique/identity
                          :db/cardinality :db.cardinality/one}]})
    (d/with (:db-after %) {:tx-data [[:db/add "supplier" :db/ident :test/supplierA]]})
    (let [ptx0 (template [{:db/id "order0"
                           :db/ident :test/orderA
                           :order/supplier :test/supplierA
                           :order/description "Test Order 0"
                           :order/lines [{:db/id "line00"
                                          :order.line/part-number "A"}
                                         {:db/id "line01"
                                          :order.line/part-number "B"}]}
                          [:db/add "order0" :order/created-by "datomic.tx"]
                          [:db/add "datomic.tx" :db/ident :test/transaction]
                          (com.recbus.ptx/|+ ~`autoinc "order0" :order/id)])
          ptx1 (template #{(~`enumerate-lines "order0" ["line00" "line01"] 10)})
          ptx2 (template #{(~`identify-line "order0" "line00")
                           (~`identify-line "order0" "line01")})
          tx-data (template [(com.recbus.ptx/| ~ptx0 ~ptx1 ~ptx2)])
          {{o "order0"} :tempids :keys [db-after]} (d/with (:db-after %) {:tx-data tx-data})
          selector '[*
                     {:order/supplier [:db/ident]}
                     {[:order/lines :xform clojure.core/set] [:order.line/number :order.line/part-number :order.line/order-id+number]}
                     {:order/created-by [:db/ident]}]
          order (d/pull db-after {:eid o :selector selector})]
      (is (= {:db/id o
              :db/ident :test/orderA
              :order/id 0
              :order/supplier {:db/ident :test/supplierA}
              :order/description "Test Order 0"
              :order/lines #{{:order.line/part-number "A"
                              :order.line/number 10
                              :order.line/order-id+number [0 10]}
                             {:order.line/part-number "B"
                              :order.line/number 20
                              :order.line/order-id+number [0 20]}}
              :order/created-by {:db/ident :test/transaction}}
             order)))))

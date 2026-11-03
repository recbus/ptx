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

(defn f0 [context tx] tx)

(deftest expansion
  (let [wdb (as-> *db* %
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
                                    :db/cardinality :db.cardinality/one}]})
              (:db-after %))
        selector [:order/id :order/description :order/origin :order/parent :order/tags :order/related]]
    (testing "baseline"
      (let [ptx0 (template [[:db/add "order" :order/id 1000]])]
        (is (= [[:db/add "order" :order/id 1000]]
               (sut/| wdb ptx0)))))
    (testing "simple map expansion"
      (let [ptx0 (template [{:order/id 1000
                             :order/description "Test"}])
            result (sut/| wdb ptx0)]
        (is (= 2 (count result)))
        (let [auto-tid (get-in result [0 1])]
          (is (= #{[:db/add auto-tid :order/id 1000]
                   [:db/add auto-tid :order/description "Test"]}
                 (set result))))))
    (testing "simple map expansion with explicit :db/id"
      (let [ptx0 (template [{:db/id "order"
                             :order/id 1000
                             :order/description "Test"}])]
        (is (= #{[:db/add "order" :order/id 1000]
                 [:db/add "order" :order/description "Test"]}
               (set (sut/| wdb ptx0))))))
    (testing "recursive map expansion"
      (let [ptx0 (template [{:db/id "order"
                             :order/id 1000
                             :order/supplier {:db/id "supplier"
                                              :db/ident :supplier/A}
                             :order/description "Test"}])]
        (is (= #{[:db/add "order" :order/id 1000]
                 [:db/add "order" :order/description "Test"]
                 [:db/add "supplier" :db/ident :supplier/A]
                 [:db/add "order" :order/supplier "supplier"]}
               (set (sut/| wdb ptx0))))))
    (testing "expands cardinality-many"
      (let [ptx0 (template [{:db/id "order"
                             :order/id 1000
                             :order/related [{:db/id "relatedA"
                                              :db/ident :related/A}
                                             {:db/id "relatedB"
                                              :db/ident :related/B}]
                             :order/tags [:order/big :order/important]}])]
        (is (= #{[:db/add "order" :order/id 1000]
                 [:db/add "order" :order/related "relatedA"]
                 [:db/add "order" :order/related "relatedB"]
                 [:db/add "relatedA" :db/ident :related/A]
                 [:db/add "relatedB" :db/ident :related/B]
                 [:db/add "order" :order/tags :order/big]
                 [:db/add "order" :order/tags :order/important]}
               (set (sut/| wdb ptx0))))))
    (testing "expands tuples with singleton"
      (let [ptx0 (template [{:db/id "order"
                             :order/id 1000
                             :order/origin ["US" "TX"]}])]
        (is (= #{[:db/add "order" :order/id 1000]
                 [:db/add "order" :order/origin ["US" "TX"]]}
               (set (sut/| wdb ptx0))))))
    (testing "function expansion"
      (let [ptx0 (template [{:db/id "order"
                             :order/id 1000
                             :order/origin ["US" "TX"]}
                            (~`f0 [[:db/add "order" :order/description "Test Description"]])])]
        (is (= #{[:db/add "order" :order/id 1000]
                 [:db/add "order" :order/origin ["US" "TX"]]
                 [:db/add "order" :order/description "Test Description"]}
               (set (sut/| wdb ptx0))))))))

(deftest cloud-expansion
  (let [wdb (as-> *db* %
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
                                    :db/cardinality :db.cardinality/one}]})
              (:db-after %))
        selector [:order/id :order/description :order/origin :order/parent :order/tags :order/related]]
    (testing "function invocation as lazy seq"
      (let [f (lazy-seq (template (~`f0 [[:db/add "order" :order/description "Test Description"]])))
            ptx0 (template [{:db/id "order"
                             :order/id 1000
                             :order/origin ["US" "TX"]}
                            ~f])]
        (is (= #{[:db/add "order" :order/id 1000]
                 [:db/add "order" :order/origin ["US" "TX"]]
                 [:db/add "order" :order/description "Test Description"]}
               (set (sut/| wdb ptx0))))))))

(deftest composition
  (let [wdb (as-> *db* %
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
                                   {:db/ident :supplier/id
                                    :db/valueType :db.type/long
                                    :db/unique :db.unique/identity
                                    :db/cardinality :db.cardinality/one}]})
              (d/with (:db-after %) {:tx-data [{:db/id "supplierA" :supplier/id 1 :db/ident :test/supplierA}
                                               {:db/id "supplierB" :supplier/id 2 :db/ident :test/supplierB}
                                               {:db/id "order"
                                                :order/description "Test Order 0"
                                                :order/id 0
                                                :db/ident :test/orderA
                                                :order/supplier "supplierA"}]})
              (:db-after %))
        selector [:order/id :order/description :order/origin :order/parent]]
    (testing "baseline"
      (let [ptx0 (template [[:db/add "order0" :order/id 1000]])
            ptx1 (template [[:db/add "order1" :order/id 1001]])]
        (is (= #{[:db/add "order0" :order/id 1000]
                 [:db/add "order1" :order/id 1001]}
               (set (sut/| wdb ptx0 ptx1))))))
    (testing "tempids resolve in E position across partials"
      (let [ptx0 (template [{:db/id "order0" :order/id 1000}])
            ptx1 (template [{:db/id "order0" :order/description "Test Order"}])]
        (is (= #{[:db/add "order0" :order/id 1000]
                 [:db/add "order0" :order/description "Test Order"]}
               (set (sut/| wdb ptx0 ptx1))))))
    (testing "lookup refs are not modified by tempid resolution"
      (let [ptx0 (template [{:db/id "order0" :order/id 1000}])
            ptx1 (template [{:db/id "order0" :order/description "Test Order"}
                            [:db/retract [:order/id 0] :order/description "Test Order 0"]])]
        (is (= #{[:db/add "order0" :order/id 1000]
                 [:db/add "order0" :order/description "Test Order"]
                 [:db/retract [:order/id 0] :order/description "Test Order 0"]}
               (set (sut/| wdb ptx0 ptx1))))))
    (testing "tempids resolve in V position across partials"
      (let [ptx0 (template [{:db/id "supplier" :db/ident :test/supplierB}])
            ptx1 (template [{:db/id "order" :order/supplier "supplier"}])]
        (is (= #{[:db/add "order" :order/supplier "supplier"]
                 [:db/add "supplier" :db/ident :test/supplierB]}
               (set (sut/| wdb ptx0 ptx1))))))
    (testing "conflicting datoms are not resolved across partials"
      (let [ptx0 (template [{:db/id "order0" :order/id 1000}])
            ptx1 (template [{:db/id "order0" :order/id 1001}])]
        (is (= #{[:db/add "order0" :order/id 1000]
                 [:db/add "order0" :order/id 1001]}
               (set (sut/| wdb ptx0 ptx1))))))
    (testing "tempids resolve in E position across partials with intervening partials"
      (let [ptx0 (template [{:db/id "order0" :order/id 1000}])
            ptx1 (template [{:db/id "order1" :order/id 1001}])
            ptx2 (template [{:db/id "order0" :order/description "Test Order"}])]
        (is (= #{[:db/add "order0" :order/id 1000]
                 [:db/add "order1" :order/id 1001]
                 [:db/add "order0" :order/description "Test Order"]}
               (set (sut/| wdb ptx0 ptx1 ptx2))))))
    (testing "tempids resolve in E position of :db/retractEntity operations across partials"
      (let [ptx0 (template [{:db/id "order0" :order/id 1000}])
            ptx1 (template [[:db/retractEntity "order0"]])]
        ;; this transaction would fail when consolidated, but it suffices for testing
        (is (= #{[:db/add "order0" :order/id 1000]
                 [:db/retractEntity "order0"]}
               (set (sut/| wdb ptx0 ptx1))))))
    (testing "tempids resolve in E position of :db/cas operations across partials"
      (let [ptx0 (template [{:db/id "order0" :order/id 1000}])
            ptx1 (template [[:db/cas "order0" :order/description nil "Test Order"]])]
        ;; this transaction would fail when consolidated, but it suffices for testing
        (is (= #{[:db/add "order0" :order/id 1000]
                 [:db/cas "order0" :order/description nil "Test Order"]}
               (set (sut/| wdb ptx0 ptx1))))))
    (testing "tempids resolve in V position of :db/cas operations across partials"
      (let [ptx0 (template [{:db/id "order0" :order/id 1000}
                            [:db/add "supplier" :db/ident :test/supplier]])
            ptx1 (template [[:db/cas "order0" :order/supplier nil "supplier"]])]
        ;; this transaction would fail when consolidated, but it suffices for testing; is it even realistic?
        (is (= #{[:db/add "order0" :order/id 1000]
                 [:db/add "supplier" :db/ident :test/supplier]
                 [:db/cas "order0" :order/supplier nil "supplier"]}
               (set (sut/| wdb ptx0 ptx1))))))
    (testing "lookup refs are not conflated with card-many scalars"
      (let [ptx0 (template [{:db/id "order0"
                             :order/id 1000
                             :order/description "Test Order"
                             :order/related [[:supplier/id 1] [:supplier/id 2]]}])]
        (is (= #{[:db/add "order0" :order/id 1000]
                 [:db/add "order0" :order/description "Test Order"]
                 [:db/add "order0" :order/related [:supplier/id 1]]
                 [:db/add "order0" :order/related [:supplier/id 2]]}
               (set (sut/| wdb ptx0))))))
    (testing "supports short-form and long-form retraction"
      (let [ptx0 (template [{:db/id "order0"
                             :order/id 1000
                             :order/description "Test Order"
                             :order/tags [:order/big :order/important]}])
            ptx1 (template [[:db/retract "order0" :order/tags :order/big]
                            [:db/retract "order0" :order/description]])]
        ;; this transaction would fail when consolidated, but it suffices for testing
        (is (= #{[:db/add "order0" :order/id 1000]
                 [:db/add "order0" :order/description "Test Order"]
                 [:db/add "order0" :order/tags :order/important]
                 [:db/add "order0" :order/tags :order/big]
                 [:db/retract "order0" :order/tags :order/big]
                 [:db/retract "order0" :order/description]}
               (set (sut/| wdb ptx0 ptx1))))))))

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

(deftest distinguishable-exceptions
  (let [wdb (as-> *db* %
              ;; install some test attributes
              (d/with % {:tx-data [{:db/ident :order/id
                                    :db/valueType :db.type/long
                                    :db/unique :db.unique/identity
                                    :db/cardinality :db.cardinality/one}
                                   {:db/ident :order/tags
                                    :db/valueType :db.type/keyword
                                    :db/cardinality :db.cardinality/many}]})
              (:db-after %))
        ptx0 (template #{{:db/id "order0"
                          :order/id 1000}})
        ptx1 (template #{[:db/add [:order/id 1001] :order/tags :order/big]})]
    (try
      (d/with wdb {:tx-data [(template (com.recbus.ptx/| ~ptx0 ~ptx1))]})
      (catch Exception e
        (is (match?
             (m/via Throwable->map
                    {:data {:cognitect.anomalies/category :cognitect.anomalies/incorrect
                            :cognitect.anomalies/message #"Unable to resolve entity.*"
                            :db/error :db.error/not-an-entity}
                     :via [{:message "Partial transaction application failed."}
                           {:message "Partial transaction failed."
                            :data {:com.recbus.ptx/tx-data seq?
                                   :com.recbus.ptx/resolved-tx-data seq?}}
                           {:data {:db/error :db.error/not-an-entity}}]})
             e))))))

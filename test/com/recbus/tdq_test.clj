(ns com.recbus.tdq-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [com.recbus.tdq :as sut]
            [datomic.client.api :as d]
            [matcher-combinators.test]))

;;; Fixtures

(def ^:dynamic *db*
  "An empty with-db database value, bound by `with-db-fixture`."
  nil)

(defn- with-db-fixture
  "Create an ephemeral in-memory Datomic Local database and bind a with-db
  value of it to `*db*` for the duration of the tests."
  [f]
  (let [client (d/client {:server-type :datomic-local
                          :system "tdq-test"
                          :storage-dir :mem})
        db-name (str "tdq-test-" (random-uuid))]
    (d/create-database client {:db-name db-name})
    (try
      (binding [*db* (d/with-db (d/connect client {:db-name db-name}))]
        (f))
      (finally
        (d/delete-database client {:db-name db-name})))))

(use-fixtures :once with-db-fixture)

(def ^:private schema
  [{:db/ident :car/vin :db/valueType :db.type/string :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}
   {:db/ident :car/plate :db/valueType :db.type/string :db/unique :db.unique/value :db/cardinality :db.cardinality/one}
   {:db/ident :car/color :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}
   {:db/ident :car/trim :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :car/owner :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :person/email :db/valueType :db.type/string :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}])

(def ^:private trims
  [{:db/ident :trim/base} {:db/ident :trim/sport}])

(def ^:private data
  [{:car/vin "V1" :car/plate "P1" :car/color :blue :car/trim :trim/base}
   {:car/vin "V2" :car/color :black :car/trim :trim/sport}])

(defn- with-tx [db tx-data] (:db-after (d/with db {:tx-data tx-data})))

(defn- test-db [] (-> *db* (with-tx schema) (with-tx trims) (with-tx data)))

(defn- eid [db identifier] (:db/id (d/pull db [:db/id] identifier)))

(defn- tx-result
  "The (speculative) transaction result of `tx-data` against the test database."
  [tx-data]
  (d/with (test-db) {:tx-data tx-data}))

(defn- q
  "Return the result of `sut/q` as a set, for comparisons that ignore the order
  of the relation, which Datomic doesn't guarantee.  Only for queries without
  `:with`, whose results cannot contain duplicates."
  [& args]
  (set (apply sut/q args)))

(defn- failure
  "Call `f` and return a summary of the `ExceptionInfo` it throws, if any."
  [f]
  (try (f)
       nil
       (catch clojure.lang.ExceptionInfo e
         {:message (ex-message e) :data (ex-data e)})))

(defn- rewrite-failure
  [query]
  (failure #(sut/rewrite query {})))

;;; Tests

(deftest rewrite-shapes
  (testing "a sugared clause is padded and rewritten against $delta; the databases prefix :in"
    (is (= '{:find [?v] :in [$0 $delta $] :where [[$delta 1 :car/color ?v _ true]]}
           (sut/rewrite '{:find [?v] :tdq/where [[:db/add 1 :car/color ?v]]} {}))))
  (testing "the op determines the added position"
    (is (= '[[$delta ?e :car/color ?v _ false] [$delta ?e :car/color ?v _ _] [$delta ?e :car/color ?v _ ?added]]
           (:where (sut/rewrite '{:find [?e]
                                  :tdq/where [[:db/retract ?e :car/color ?v] [_ ?e :car/color ?v] [?added ?e :car/color ?v]]}
                                {})))))
  (testing "a lookup ref joins through $ for :db/add and $0 for :db/retract, once per database"
    (is (= '[[$ ??lr0 :car/vin "V1"] [$delta ??lr0 :car/color ?new _ true]
             [$0 ??lr1 :car/vin "V1"] [$delta ??lr1 :car/color ?old _ false]
             [$delta ??lr0 :car/plate ?p _ true]]
           (:where (sut/rewrite '{:find [?old ?new ?p]
                                  :tdq/where [[:db/add [:car/vin "V1"] :car/color ?new]
                                              [:db/retract [:car/vin "V1"] :car/color ?old]
                                              [:db/add [:car/vin "V1"] :car/plate ?p]]}
                                {})))))
  (testing "lrtids are resolved everywhere"
    (is (= '[[$delta 42 :car/owner 43 _ true] [42 :car/vin ?vin]]
           (:where (sut/rewrite '{:find [?vin]
                                  :tdq/where [[:db/add [:db/id "car"] :car/owner [:db/id "ann"]]]
                                  :where [[[:db/id "car"] :car/vin ?vin]]}
                                {"car" 42 "ann" 43})))))
  (testing "... but map entries are not lrtids"
    (is (= '[[(get {:db/id "car"} :db/id) ?x]]
           (:where (sut/rewrite '{:find [?x] :where [[(get {:db/id "car"} :db/id) ?x]]} {})))))
  (testing ":tdq/where precedes :where, which is passed through"
    (is (= '[[$delta ?e :car/color _ _ true] (not [?e :car/vin "V1"]) [?e :car/color ?c]]
           (:where (sut/rewrite '{:find [?e]
                                  :where [(not [?e :car/vin "V1"]) [?e :car/color ?c]]
                                  :tdq/where [[:db/add ?e :car/color]]}
                                {})))))
  (testing "other keys are passed through"
    (let [query (sut/rewrite '{:find [?e ?c] :keys [:e :c] :with [?x] :tdq/where [[:db/add ?e :car/color ?c]]} {})]
      (is (= '{:find [?e ?c] :keys [:e :c] :with [?x]} (select-keys query [:find :keys :with])))
      (is (not (contains? query :tdq/where)))))
  (testing "the caller's inputs follow the databases"
    (is (= '[$0 $delta $ ?c %] (:in (sut/rewrite '{:find [?e] :in [?c %] :tdq/where [[:db/add ?e :car/color ?c]]} {})))))
  (testing ":tdq/where is optional"
    (is (= '{:find [?e] :in [$0 $delta $] :where [[?e :car/vin]]}
           (sut/rewrite '{:find [?e] :where [[?e :car/vin]]} {})))))

(deftest results
  (let [result (tx-result [[:db/add [:car/vin "V1"] :car/color :green] [:db/add [:car/vin "V2"] :car/color :green]])
        db (:db-after result)
        v1 (eid db [:car/vin "V1"])
        v2 (eid db [:car/vin "V2"])]
    (testing "the result is the relation returned by Datomic"
      (let [rows (sut/q result '{:find [?e] :tdq/where [[:db/add ?e :car/color :green]]})]
        (is (= #{[v1] [v2]} (set rows)))
        (is (= 2 (count rows)))))
    (testing "... which is empty when nothing matches"
      (is (empty? (sut/q result '{:find [?e] :tdq/where [[:db/add ?e :car/color :red]]}))))
    (testing "pull expressions (against db-after)"
      (is (= #{[{:car/vin "V1"}] [{:car/vin "V2"}]}
             (q result '{:find [(pull ?e [:car/vin])] :tdq/where [[:db/add ?e :car/color :green]]}))))))

(deftest sugared-clauses
  (let [result (tx-result [[:db/add [:car/vin "V1"] :car/color :green]])
        v1 (eid (:db-after result) [:car/vin "V1"])]
    (testing "assertions"
      (is (= #{[v1 :green]} (q result '{:find [?e ?c] :tdq/where [[:db/add ?e :car/color ?c]]}))))
    (testing "retractions"
      (is (= #{[v1 :blue]} (q result '{:find [?e ?c] :tdq/where [[:db/retract ?e :car/color ?c]]}))))
    (testing "either"
      (is (= #{[:green] [:blue]} (q result '{:find [?c] :tdq/where [[_ _ :car/color ?c]]}))))
    (testing "the added boolean bound to a variable"
      (is (= #{[:green true] [:blue false]} (q result '{:find [?c ?added] :tdq/where [[?added _ :car/color ?c]]}))))
    (testing "changed from/to"
      (is (= #{[:blue :green]} (q result '{:find [?old ?new]
                                           :tdq/where [[:db/retract ?e :car/color ?old] [:db/add ?e :car/color ?new]]}))))))

(deftest lookup-refs
  (testing "for :db/add, lookup refs resolve against db-after: new entities and upserts"
    (let [result (tx-result [{:car/vin "N1" :car/color :red} {:car/vin "V2" :car/color :red}])]
      (is (= #{[:red]} (q result '{:find [?c] :tdq/where [[:db/add [:car/vin "N1"] :car/color ?c]]})))
      (is (= #{[:red]} (q result '{:find [?c] :tdq/where [[:db/add [:car/vin "V2"] :car/color ?c]]})))))
  (testing "for :db/retract, lookup refs resolve against db-before: retracted entities"
    (let [result (tx-result [[:db/retractEntity [:car/vin "V2"]]])
          sport (eid (:db-before result) :trim/sport)]
      (is (= #{[:car/vin "V2"] [:car/color :black] [:car/trim sport]}
             (q result '{:find [?attr ?v]
                         :tdq/where [[:db/retract [:car/vin "V2"] ?a ?v]]
                         :where [[?a :db/ident ?attr]]})))))
  (testing "... and changed identity values"
    (let [result (tx-result [[:db/add [:car/vin "V1"] :car/vin "V1-new"]])]
      (is (= #{["V1" "V1-new"]} (q result '{:find [?old ?new]
                                            :tdq/where [[:db/retract [:car/vin "V1"] :car/vin ?old]
                                                        [:db/add [:car/vin "V1-new"] :car/vin ?new]]})))
      (testing "(the old identity does not resolve in db-after)"
        (is (= #{} (q result '{:find [?v] :tdq/where [[:db/add [:car/vin "V1"] :car/vin ?v]]}))))))
  (testing "reassignment is written with entities bound from the delta"
    (let [result (tx-result [[:db/retract [:car/vin "V1"] :car/plate "P1"] [:db/add [:car/vin "V2"] :car/plate "P1"]])]
      (is (= #{["V1" "V2"]} (q result '{:find [?from ?to]
                                        :tdq/where [[:db/retract ?e1 :car/plate "P1"] [:db/add ?e2 :car/plate "P1"]]
                                        :where [[$0 ?e1 :car/vin ?from] [?e2 :car/vin ?to]]}))))))

(deftest lrtids
  (let [result (tx-result [{:db/id "car" :car/vin "N1" :car/owner "bob"} {:db/id "bob" :person/email "bob@example.com"}])
        car (get (:tempids result) "car")]
    (testing "in the entity position"
      (is (= #{["N1"]} (q result '{:find [?vin] :tdq/where [[:db/add [:db/id "car"] :car/vin ?vin]]}))))
    (testing "in the value position"
      (is (= #{[car]} (q result '{:find [?e] :tdq/where [[:db/add ?e :car/owner [:db/id "bob"]]]}))))
    (testing "in :where"
      (is (= #{["bob@example.com"]} (q result '{:find [?email] :where [[[:db/id "bob"] :person/email ?email]]}))))
    (testing "in args"
      (is (= #{["N1"]} (q result '{:find [?vin] :in [?owner] :tdq/where [[:db/add ?e :car/owner ?owner]] :where [[?e :car/vin ?vin]]}
                          [[:db/id "bob"]]))))
    (testing "an unresolvable lrtid"
      (is (match? {:message "Unresolvable tempid." :data {::sut/tempid "nope"}}
                  (failure #(sut/q result '{:find [?vin] :tdq/where [[:db/add [:db/id "nope"] :car/vin ?vin]]}))))
      (is (match? {:message "Unresolvable tempid." :data {::sut/tempid "nope"}}
                  (failure #(sut/q result '{:find [?e] :in [?owner] :tdq/where [[:db/add ?e :car/owner ?owner]]}
                                   [[:db/id "nope"]])))))))

(deftest three-databases
  (let [result (tx-result [[:db/add [:car/vin "V1"] :car/color :green] [:db/add [:car/vin "V2"] :car/color :green]])]
    (testing "joins across db-before, the delta and db-after"
      (is (= #{["V1" :blue] ["V2" :black]}
             (q result '{:find [?vin ?old]
                         :tdq/where [[:db/add ?e :car/color :green]]
                         :where [[$0 ?e :car/color ?old] [?e :car/vin ?vin]]}))))))

(deftest caller-inputs
  (let [result (tx-result [[:db/add [:car/vin "V1"] :car/color :green] [:db/add [:car/vin "V2"] :car/color :red]])]
    (testing "scalar inputs"
      (is (= #{["V2"]} (q result '{:find [?vin] :in [?c] :tdq/where [[:db/add ?e :car/color ?c]] :where [[?e :car/vin ?vin]]}
                          [:red]))))
    (testing "collection inputs"
      (is (= #{["V1"] ["V2"]} (q result '{:find [?vin] :in [[?c ...]] :tdq/where [[:db/add ?e :car/color ?c]] :where [[?e :car/vin ?vin]]}
                                 [[:red :green]]))))
    (testing "rules run against db-after"
      (is (= #{["V1"]} (q result '{:find [?vin] :in [%] :tdq/where [[:db/add ?e :car/color :green]] :where [(vin ?e ?vin)]}
                          '[[[(vin ?e ?vin) [?e :car/vin ?vin]]]]))))))

(deftest errors
  (testing "a delta query is a map"
    (is (match? {:message "A delta query must be a map."} (rewrite-failure '[:find ?e :where [?e :car/vin]]))))
  (testing ":tdq/where is a vector"
    (is (match? {:message "The :tdq/where clauses must be a vector."}
                (rewrite-failure '{:find [?e] :tdq/where ([:db/add ?e :car/color])}))))
  (testing ":tdq/where contains only sugared clauses"
    (is (match? {:message "A sugared clause must be a vector: [op e a v tx]." :data {::sut/sugared-clause '(not [:db/add ?e :car/color])}}
                (rewrite-failure '{:find [?e] :tdq/where [(not [:db/add ?e :car/color])]})))
    (is (match? {:message #"must start with :db/add" :data {::sut/clause '[1 :car/color ?c]}}
                (rewrite-failure '{:find [?c] :tdq/where [[1 :car/color ?c]]}))))
  (testing "a sugared clause has at most five elements"
    (is (match? {:message #"at most five"} (rewrite-failure '{:find [?c] :tdq/where [[:db/add ?e :car/color ?c _ true]]}))))
  (testing "a lookup ref requires :db/add or :db/retract"
    (is (match? {:message "A lookup ref requires :db/add or :db/retract."}
                (rewrite-failure '{:find [?c] :tdq/where [[_ [:car/vin "V1"] :car/color ?c]]}))))
  (testing "?? variables are reserved"
    (is (match? {:message "Variables starting with ?? are reserved." :data {::sut/variable '??x}}
                (rewrite-failure '{:find [??x] :tdq/where [[:db/add ??x :car/color]]}))))
  (testing "databases cannot be inputs"
    (is (match? {:message "Database inputs are not supported." :data {::sut/input '$}}
                (rewrite-failure '{:find [?e] :in [$] :tdq/where [[:db/add ?e :car/color]]})))))

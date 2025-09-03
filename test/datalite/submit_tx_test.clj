(ns datalite.submit-tx-test
  (:require [clojure.test :refer :all]
            [datalite.core :refer [transact]]
            [datalite.api.xtdb :refer [q submit-tx]]
            [datalite.protocols.duckdb]
            [datalite.keywords.xtdb :as xt]
            #_[clojure.java.jdbc :as jdbc]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as jdbc-sql])
  (:import (java.util UUID)))

(def ^:dynamic *test-conn* nil)

(def schema
  [#:db{:ident :person/xt-id
        :valueType :db.type/uuid
        :cardinality :db.cardinality/one
        :doc "The unique id as defined by XTDB"}

   #:db{:ident :person/name
        :valueType :db.type/string
        :cardinality :db.cardinality/one
        :doc "The name of a person"}

   #:db{:ident :person/age
        :valueType :db.type/long
        :cardinality :db.cardinality/one
        :doc "The age of a person"}

   #:db{:ident :person/likes-films
        :valueType :db.type/ref
        :cardinality :db.cardinality/many
        :references :film/id
        :doc "The films the person likes"}

   #:db{:ident :film/xt-id
        :valueType :db.type/uuid
        :cardinality :db.cardinality/one
        :doc "The unique id as defined by XTDB"}

   #:db{:ident :film/title
        :valueType :db.type/string
        :cardinality :db.cardinality/one
        :doc "The title of the film"
        :full-text-search true}

   #:db{:ident :film/genre
        :valueType :db.type/string
        :cardinality :db.cardinality/one
        :doc "The genre of the film"
        :full-text-search true}

   #:db{:ident :film/release-year
        :valueType :db.type/long
        :cardinality :db.cardinality/one
        :doc "The year the film was released in theaters"}

   #:db{:ident :film/url
        :valueType :db.type/string
        :cardinality :db.cardinality/one
        :doc "The URL where one can find out about the film"}

   #:db{:ident :film/directed-by
        :valueType :db.type/ref
        :cardinality :db.cardinality/one
        :references :person/id
        :doc "The person who directed the film"}])

(defn setup-connection [dbtype db-uri]
  (let [spec {:connection-uri db-uri}
        conn (jdbc/get-connection spec)]
    (-> spec
      (assoc :connection conn)
      (assoc :dbtype dbtype))))

(defn teardown! []
  (let [{:keys [dbtype]} *test-conn*]
    (cond
      (= dbtype :dbtype/postgresql)
      (let [tables (jdbc-sql/query *test-conn*
                     ["SELECT tablename FROM pg_tables WHERE schemaname = 'public'"])]
        (doseq [table tables]
          (println "Dropping table" (:pg_tables/tablename table))
          (jdbc/execute-one! *test-conn* [(str "DROP TABLE IF EXISTS " (:pg_tables/tablename table) " CASCADE")])))

      ;; Skip teardown as we're working with in-memory databases.
      (contains? #{:dbtype/sqlite :dbtype/duckdb} dbtype)
      nil

      :else
      (println "Unknown dbtype, teardown skipped."))))

(defn uuid []
  (UUID/randomUUID))

(defn setup! []
  (teardown!)
  #_(submit-tx *test-conn* [[::xt/put {:datalite/schema schema}]])
  (transact *test-conn* {:tx-data schema})
  (submit-tx *test-conn* [[::xt/put {:xt/id (uuid)
                                     :person/name "Alice"
                                     :person/age 29}]
                          [::xt/put {:xt/id (uuid)
                                     :person/name "Bob"
                                     :person/age 28}]])
  (submit-tx *test-conn* [[::xt/put {:xt/id (uuid)
                                     :film/title "Luca"
                                     :film/genre "Animation"
                                     :film/release-year 2021
                                     :film/url "https://www.themoviedb.org/movie/508943-luca?language=en-US"
                                     :film/directed-by 2    ;; this would be Bob
                                     }]])

  ;; fixme manually insert the relationship for now to test the join queries
  (jdbc-sql/insert! *test-conn* :join_person_likes_films {:person_id 1
                                                      :film_id 1}))

(def dbtypes-to-test [#_{:dbtype :dbtype/sqlite
                       :db-uri "jdbc:sqlite::memory:"}
                      {:dbtype :dbtype/duckdb
                       :db-uri "jdbc:duckdb:duckdb-test.db"}
                      {:dbtype :dbtype/postgresql
                       :db-uri "jdbc:postgresql://localhost:5432/datalite-test?user=datalite&password=datalite"}])

(defn fixture [f]
  (doseq [{:keys [dbtype db-uri]} dbtypes-to-test]
    (binding [*test-conn* (setup-connection dbtype db-uri)]
      (try
        (setup!)
        (f)
        (finally
          #_(teardown!))))))

(use-fixtures :once fixture)

(deftest attribute-assertion-and-retraction
  (let [result (fn []
                 (q *test-conn* '{:find [?id ?name ?age]
                                  :where [[?e :person/name ?name]
                                          [?e :person/age ?age]
                                          [?e :person/id ?id]]}))
        clara-uuid (uuid)]
    (is (= #{[1 "Alice" 29] [2 "Bob" 28]} (result))
      (format "dbtype=%s - baseline" (:dbtype *test-conn*)))

    ;
    ;{:find [(pull ?e [*])]
    ; :where [[?e :person/name "Alice"]]}
    ;

    (submit-tx *test-conn* [[::xt/put {:xt/id (uuid)
                                       :film/title "Elio"
                                       :film/genre "Animation"
                                       :film/release-year 2025}]])

    (submit-tx *test-conn* [[::xt/put {:xt/id clara-uuid
                                       :person/name "Clara"
                                       :person/age 25
                                       :person/likes-films #{1 2}}]])

    (is (= #{[1 "Alice" 29] [2 "Bob" 28] [3 "Clara" 25]} (result))
      (format "dbtype=%s single-assertion" (:dbtype *test-conn*)))

    #_(is (= #{["Alice" "Luca"] ["Clara" "Luca"] ["Clara" "Elio"]}
          (q *test-conn* '{:find [?person-name ?film-name]
                           :where [[?p :person/name ?person-name]
                                   [?p :person/likes-films ?f]
                                   [?f :film/title ?film-name]]})))

    ;; add Elio (film)
    ;; Clara likes Elio too

    ;(transact *test-conn* {:tx-data [[:db/add 1 :person/age 30]]})
    ;(is (= #{[1 "Alice" 30] [2 "Bob" 28]} (result))
    ;  (format "dbtype=%s single-assertion" (:dbtype *test-conn*)))
    ;(transact *test-conn* {:tx-data [{:film/title "Elio"
    ;                                  :film/genre "Animation"
    ;                                  :film/release-year 2025}]})
    ;;; check that we now have 2 films in the db
    ;(is (= #{["Alice" "Luca"]} (q *test-conn* '[:find ?person-name ?film-name
    ;                                            :where
    ;                                            [?p :person/name ?person-name]
    ;                                            [?p :person/likes-films ?f]
    ;                                            [?f :film/title ?film-name]])))
    ;#_(transact *test-conn* {:tx-data [[:db/add 1 :person/likes-films 2]]})
    ;(transact *test-conn* {:tx-data [[:db/add [:person/name "Alice"] :person/likes-films [:film/title "Elio"]]]})
    ;(is (= #{["Alice" "Luca"] ["Alice" "Elio"]} (q *test-conn* '[:find ?person-name ?film-name
    ;                                                             :where
    ;                                                             [?p :person/name ?person-name]
    ;                                                             [?p :person/likes-films ?f]
    ;                                                             [?f :film/title ?film-name]])))
    ;
    ;;; retract that Alice likes Luca
    ;(transact *test-conn* {:tx-data [[:db/retract 1 :person/likes-films 1]]})
    ;(is (= #{["Alice" "Elio"]} (q *test-conn* '[:find ?person-name ?film-name
    ;                                            :where
    ;                                            [?p :person/name ?person-name]
    ;                                            [?p :person/likes-films ?f]
    ;                                            [?f :film/title ?film-name]])))
    ;
    ;(is (= #{[1 "Alice" 30] [2 "Bob" 28]} (result)))
    ;(transact *test-conn* {:tx-data [[:db/retract 1 :person/age 30]]})
    ;(is (= #{[1 "Alice" nil] [2 "Bob" 28]} (result)))
    ;
    ;
    ))


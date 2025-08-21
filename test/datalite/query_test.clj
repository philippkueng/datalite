(ns datalite.query-test
  (:require [clojure.test :refer :all]
            [datalite.core :refer [q transact]]
            [clojure.java.jdbc :as jdbc]
            [babashka.fs :refer [delete-if-exists]]))

(def ^:dynamic *test-conn* nil)

(def schema
  [#:db{:ident :person/name
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
        :doc "The URL where one can find out about the film"}])

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
      (let [tables (jdbc/query *test-conn*
                     ["SELECT tablename FROM pg_tables WHERE schemaname = 'public'"])]
        (doseq [{:keys [tablename]} tables]
          (jdbc/execute! *test-conn* [(str "DROP TABLE IF EXISTS " tablename " CASCADE;")])))

      ;; Skip teardown as we're working with in-memory databases.
      (contains? #{:dbtype/sqlite :dbtype/duckdb} dbtype)
      nil

      :else
      (println "Unknown dbtype, teardown skipped."))))


(defn setup! []
  (teardown!)
  (transact *test-conn* {:tx-data schema})
  (transact *test-conn* {:tx-data [{:person/name "Alice"
                                    :person/age 29}
                                   {:person/name "Bob"
                                    :person/age 28}]})
  (transact *test-conn* {:tx-data [{:film/title "Luca"
                                    :film/genre "Animation"
                                    :film/release-year 2021
                                    :film/url "https://www.themoviedb.org/movie/508943-luca?language=en-US"}]}))

(def dbtypes-to-test [{:dbtype :dbtype/sqlite
                       :db-uri "jdbc:sqlite::memory:"}
                      {:dbtype :dbtype/duckdb
                       :db-uri "jdbc:duckdb:memory:"}
                      {:dbtype :dbtype/postgresql
                       :db-uri "jdbc:postgresql://localhost:5432/datalite-test?user=datalite&password=datalite"}])

(defn fixture [f]
  (doseq [{:keys [dbtype db-uri]} dbtypes-to-test]
    (binding [*test-conn* (setup-connection dbtype db-uri)]
      (try
        (setup!)
        (f)
        (finally
          (teardown!))))))

(use-fixtures :once fixture)

(deftest value-order-of-a-query-response
  (let [result (q *test-conn* '[:find ?id ?name
                                :where
                                [?e :person/name ?name]
                                [?e :person/id ?id]])]
    (is (= #{[1 "Alice"] [2 "Bob"]} result)
      (format "dbtype=%s" (:dbtype *test-conn*)))))

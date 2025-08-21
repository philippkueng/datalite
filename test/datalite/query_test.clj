(ns datalite.query-test
  (:require [clojure.test :refer :all]
            [datalite.core :refer [q create-tables! transact]]
            [mount.core :as mount]
            [clojure.java.jdbc :as jdbc]
            [babashka.fs :refer [delete-if-exists]]))

(def test-database "test.db")
(def db-uri (str "jdbc:sqlite:" test-database))
(declare conn)

(defn on-start []
  (let [spec {:connection-uri db-uri}
        conn (jdbc/get-connection spec)]
    (-> spec
      (assoc :connection conn)
      (assoc :dbtype :dbtype/sqlite))))

(defn on-stop []
  (-> conn :connection .close)
  nil)

(mount/defstate
  ^{:on-reload :noop}
  conn
  :start (on-start)
  :stop (on-stop))

(def schema
  [#:db{:ident :person/name
        :valueType :db.type/string
        :cardinality :db.cardinality/one
        :doc "The name of a person"}

   #:db{:ident :person/age
        :valueType :db.type/long
        :cardinality :db.cardinality/one
        :doc "The age of a person"}

   ;#:db{:ident :person/likes-films
   ;     :valueType :db.type/ref
   ;     :cardinality :db.cardinality/many
   ;     :references #{:film/id}
   ;     :doc "The films the person likes"}

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

(comment
  (mount/start #'conn)
  (mount/stop #'conn))

(defn teardown! []
  (mount/stop #'conn)
  (delete-if-exists test-database))

(defn setup! []
  (teardown!)
  (mount/start #'conn)
  (transact conn {:tx-data schema})
  (transact conn {:tx-data [{:person/name "Alice"
                             :person/age 29}
                            {:person/name "Bob"
                             :person/age 28}]})
  (transact conn {:tx-data [{:film/title "Luca"
                             :film/genre "Animation"
                             :film/release-year 2021
                             :film/url "https://www.themoviedb.org/movie/508943-luca?language=en-US"}]}))

(defn fixture [f]
  (setup!)
  (f)
  (teardown!))

(use-fixtures :once fixture)

(deftest value-order-of-a-query-response
  (let [result (q conn '[:find ?id ?name
                         :where
                         [?e :person/name ?name]
                         [?e :person/id ?id]])]
    (is (= #{[1 "Alice"] [2 "Bob"]} result))))

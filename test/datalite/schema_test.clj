(ns datalite.schema-test
  (:require [clojure.test :refer [deftest is]]
            [datalite.schema :refer [create-internal-table-commands create-table-commands]]))

(deftest datalite-required-table-commands
  (let [expected-queries #{"CREATE TABLE dl_schema (id INTEGER PRIMARY KEY AUTOINCREMENT, schema BLOB, tx_time DATETIME DEFAULT (CURRENT_TIMESTAMP))"
                           "CREATE TABLE dl_transactions (id INTEGER PRIMARY KEY AUTOINCREMENT, data BLOB, tx_time DATETIME DEFAULT (CURRENT_TIMESTAMP))"}
        generated-queries (set (create-internal-table-commands :dbtype/sqlite))]
    (is (= generated-queries expected-queries))))

(deftest schema-to-table-commands-conversion
  (let [schema [#:db{:ident :person/name
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
                     :references :film/id               ;; an addition that isn't needed by Datomic but helps us
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
                     :doc "The URL where one can find out about the film"}]
        expected-queries #{"CREATE TABLE person (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)"
                           "CREATE TABLE film (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, genre TEXT, release_year INTEGER, url TEXT)"
                           "CREATE TABLE join_person_likes_films (film_id INTEGER, person_id INTEGER)"}
        generated-queries (set (create-table-commands :dbtype/sqlite schema))]
    (is (= generated-queries expected-queries))))

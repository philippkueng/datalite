(ns datalite.schema-test
  (:require [clojure.test :refer [deftest is]]
            [datalite.schema :refer [create-internal-table-commands create-table-commands]]))

(deftest datalite-required-table-commands
  (let [expected-queries #{"CREATE TABLE dl_schema (id INTEGER PRIMARY KEY AUTOINCREMENT, schema BLOB, tx_time DATETIME DEFAULT (CURRENT_TIMESTAMP))"
                           "CREATE TABLE dl_transactions (id INTEGER PRIMARY KEY AUTOINCREMENT, data BLOB, tx_time DATETIME DEFAULT (CURRENT_TIMESTAMP))"
                           "CREATE TABLE dl_xt_id_mappings (xt_id TEXT UNIQUE, table_name TEXT, internal_entity_id INTEGER)"}
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
                     :doc "The URL where one can find out about the film"}

                #:db{:ident :film/directed-by
                     :valueType :db.type/ref
                     :cardinality :db.cardinality/one
                     :references :person/id
                     :doc "The person who directed the film"}]
        expected-queries #{"CREATE TABLE join_person_likes_films (film_id INTEGER, person_id INTEGER, valid_from INTEGER NOT NULL, valid_to INTEGER)"
                           "CREATE TABLE film (rowid INTEGER PRIMARY KEY AUTOINCREMENT, id INTEGER, valid_from INTEGER NOT NULL, valid_to INTEGER, title TEXT, genre TEXT, release_year INTEGER, url TEXT, directed_by INTEGER)"
                           "CREATE TABLE person (rowid INTEGER PRIMARY KEY AUTOINCREMENT, id INTEGER, valid_from INTEGER NOT NULL, valid_to INTEGER, name TEXT, age INTEGER)"
                           "CREATE TRIGGER set_id_after_insert_for_person AFTER INSERT ON person FOR EACH ROW WHEN NEW.id IS NULL BEGIN UPDATE person SET id = (SELECT COALESCE(MAX(id), 0) + 1 FROM person WHERE rowid < NEW.rowid) WHERE rowid = NEW.rowid; END"
                           "CREATE TRIGGER set_id_after_insert_for_film AFTER INSERT ON film FOR EACH ROW WHEN NEW.id IS NULL BEGIN UPDATE film SET id = (SELECT COALESCE(MAX(id), 0) + 1 FROM film WHERE rowid < NEW.rowid) WHERE rowid = NEW.rowid; END"}
        generated-queries (set (create-table-commands :dbtype/sqlite schema))]
    (is (= generated-queries expected-queries))))

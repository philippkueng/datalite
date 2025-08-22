(ns datalite.serialisation-test
  (:require [clojure.test :refer :all]
            [datalite.serialisation :refer :all]))

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

(deftest ensure-serialisation-works
  (is (= schema
        (-> schema
          (encode :msgpack)
          (decode :msgpack)))))

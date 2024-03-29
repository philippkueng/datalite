(ns datalite.core
  (:require
    [datalite.utils :refer [replace-dashes-with-underlines]]
    [datalite.schema :refer [create-table-commands
                             drop-table-commands
                             create-full-text-search-table-commands]]
    [datalite.query-conversion :refer [datalog->sql]]
    [mount.core :as mount]
    [clojure.java.jdbc :as jdbc]))

(def db-uri "jdbc:sqlite:sample.db")
(declare db)

(defn on-start []
  (let [spec {:connection-uri db-uri}
        conn (jdbc/get-connection spec)]
    (assoc spec :connection conn)))

(defn on-stop []
  (-> db :connection .close)
  nil)

(mount/defstate
  ^{:on-reload :noop}
  db
  :start (on-start)
  :stop (on-stop))

(defn transact
  "Turn lists of maps into insert calls"
  [connection data]
  (doseq [entry data]
    ;; TODO: 11.06.2022 assuming that the map correlates to a single table insert.
    (let [table-name (->> entry keys first namespace)]
      (jdbc/insert! connection

        ;; table-name
        (keyword table-name)

        ;; remove-namespaces-from-map
        (reduce (fn [non-namespaced-entry namespaced-key]
                  (conj non-namespaced-entry
                    {(-> namespaced-key name replace-dashes-with-underlines keyword) (namespaced-key entry)}))
          {}
          (keys entry))))))

(defn create-tables!
  "Convenience functions to create all the tables required for supporting the schema"
  [db schema]
  (doseq [command (create-table-commands schema)]
    (jdbc/execute! db command))
  (doseq [command (create-full-text-search-table-commands schema)]
    (jdbc/execute! db command)))

(defn- map-record->vec-record [record]
  (->> record
    (into [])
    (sort-by first)
    (map (fn [field] (second field)))
    (into [])))

;(->> {:field_2 "Alice", :field_1 1, :field_3 29}
;  (into [])
;  (sort-by first)
;  (map (fn [field] (second field)))
;  (into [])
;  )
;=> [1 "Alice" 29]

(defn- jdbc-response->datomic-response
  "Converts `({:field_1 1, :field_2 \"Alice\", :field_3 29} {:field_1 2, :field_2 \"Bob\", :field_3 28})`
   into `#{[2 \"Bob\" 28] [1 \"Alice\" 29]}`"
  [response]
  (->> response
    (map map-record->vec-record)
    (into #{})))

(defn q
  [connection datalog-query]
  (let [datalog-sql (datalog->sql datalog-query)]
    (->> (datalog->sql datalog-query)
      (jdbc/query connection)
      jdbc-response->datomic-response)))

(comment
  (q db '[:find ?id ?name
          :where
          [?e :person/name ?name]
          [?e :person/id ?id]])

  ;; todo the order of the response is wrong, ["Alice" 1] while it should be [1 "Alice"]
  ;(q db '[:find ?id ?name
  ;        :where
  ;        [?e :person/name ?name]
  ;        [?e :person/id ?id]])
  ;=> #{["Alice" 1] ["Bob" 2]}

  (datalog->sql '[:find ?id ?name
                  :where
                  [?e :person/name ?name]
                  [?e :person/id ?id]])

  (->> (jdbc/query db "select person.id as field_001, person.name as field_002, person.age as field_003 from person")
    jdbc-response->datomic-response)

  )

(comment
  #_(q db
      '{:find [p1]
        :where [[p1 :name n]
                [p1 :last-name n]
                [p1 :name name]]
        :in [name]}
      "Alice")
  )

(comment
  (mount/start #'db)
  (mount/stop #'db)

  (def schema [#:db{:ident :person/name
                    :valueType :db.type/string
                    :cardinality :db.cardinality/one
                    :doc "The name of a person"}

               {:db/ident :person/age
                :db/valueType :db.type/long
                :db/cardinality :db.cardinality/one
                :db/doc "The age of a person"}

               {:db/ident :person/likes-films
                :db/valueType :db.type/ref
                :db/cardinality :db.cardinality/many
                :db/doc "The films the person likes"}

               {:db/ident :film/title
                :db/valueType :db.type/string
                :db/cardinality :db.cardinality/one
                :db/doc "The title of the film"
                :db/full-text-search true}

               {:db/ident :film/genre
                :db/valueType :db.type/string
                :db/cardinality :db.cardinality/one
                :db/doc "The genre of the film"
                :db/full-text-search true}

               {:db/ident :film/release-year
                :db/valueType :db.type/long
                :db/cardinality :db.cardinality/one
                :db/doc "The year the film was released in theaters"}

               {:db/ident :film/url
                :db/valueType :db.type/string
                :db/cardinality :db.cardinality/one
                :db/doc "The URL where one can find out about the film"}])

  ;; Create the tables.
  (doseq [command (create-table-commands schema)]
    (jdbc/execute! db command))

  (def data [{:person/name "Alice"
              :person/age 29}
             {:person/name "Bob"
              :person/age 28}])

  (def films [{:film/title "Luca"
               :film/genre "Animation"
               :film/release-year 2021
               :film/url "https://www.themoviedb.org/movie/508943-luca?language=en-US"}])

  #_(jdbc/insert! db :person {:id 12 :name "Alice" :age 29})
  (transact db data)
  (transact db films)

  ;; Drop the tables.
  (doseq [command (drop-table-commands schema)]
    (jdbc/execute! db command))

  (jdbc/get-by-id db :person 12)
  (jdbc/find-by-keys db :person {:name "Alice"})

  ;; create the fts virtual table
  (doseq [command (create-full-text-search-table-commands schema)]
    (jdbc/execute! db command))

  ;; searching for films by genre prefix
  (jdbc/query db "select film_fts.title, rank from film_fts where film_fts.genre match 'Anim*' order by rank")

  ;; todo - maybe part for it's separate project
  ;;        extract the schema for a list of maps and create a database schema for it
  ;;        be able to throw some data at a SQLite database so we can query it afterwards.
  ;; todo - let's say we'd like to run a query returning the .id of the original table not the _fts one, how would such a query look like?

  )

(comment
  ;; datomic API for getting a particular entity
  ;; that assumes thought that every entity in the system has a unique `eid`.
  #_(d/entity db eid)

  #_(q db '{:find [?e]
            :where [[?e :person/name "Alice"]]})
  )
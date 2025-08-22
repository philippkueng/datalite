(ns datalite.core
  (:require
    [clojure.set :as set]
    [datalite.config :refer [schema-table-name transactions-table-name]]
    [datalite.utils :refer [replace-dashes-with-underlines]]
    [datalite.schema :refer [create-table-commands
                            drop-table-commands
                            create-full-text-search-table-commands] :as schema]
    [datalite.query-conversion :refer [datalog->sql]]
    [datalite.serialisation :refer [encode decode]]
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

(defn ensure-required-tables!
  "Checks if the required tables exist and if not creates them."
  [{:keys [dbtype] :as connection}]
  (let [table-check-sql
        (case dbtype
          :dbtype/sqlite ["SELECT name FROM sqlite_master WHERE type='table' AND name IN (?, ?)" schema-table-name transactions-table-name]
          :dbtype/postgresql ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN (?, ?)" schema-table-name transactions-table-name]
          :dbtype/duckdb ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_name IN (?, ?)" schema-table-name transactions-table-name]
          (throw (ex-info "Unsupported dbtype" {:dbtype dbtype})))
        ;; The key for table name may differ by DB, so adjust as needed:
        table-key (case dbtype
                    :dbtype/sqlite :name
                    :dbtype/postgresql :table_name
                    :dbtype/duckdb :table_name)
        existing-tables (set (map table-key (jdbc/query connection table-check-sql)))
        required-tables #{schema-table-name transactions-table-name}
        missing-tables (clojure.set/difference required-tables existing-tables)]
    (when (seq missing-tables)
      (doseq [cmd (schema/create-internal-table-commands dbtype)]
        (jdbc/execute! connection cmd)))))

(defn create-tables!
  "Convenience functions to create all the tables required for supporting the schema"
  [connection schema]
  (doseq [command (create-table-commands (:dbtype connection) schema)]
    (jdbc/execute! connection command))
  (when (= :dbtype/sqlite (:dbtype connection))
    (doseq [command (create-full-text-search-table-commands schema)]
      (jdbc/execute! connection command))))


(defn transact
  "Turn lists of maps into insert calls"
  [connection {:keys [tx-data] :as data}]
  (assert (some? tx-data) "The data to be added must be wrapped within a :tx-data map")
  (ensure-required-tables! connection)
  ;; Check if the tx-data is a schema (my assumption is that transact is either called with schema information or data but not mixed)
  (if (every? #(some? (:db/ident %)) tx-data)
    (do
      ;; Persist the schema.
      (jdbc/insert! connection (keyword schema-table-name) {:schema (encode tx-data :msgpack)})

      ;; Apply the schema.
      (create-tables! connection tx-data))

    (doseq [entry tx-data]
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
            (keys entry)))))))

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
  (let [schema (-> (jdbc/query connection (format "select * from %s limit 1" schema-table-name))
                 first
                 :schema
                 (decode :msgpack))]
    (->> (datalog->sql schema datalog-query)
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
       jdbc-response->datomic-response))

(comment
  #_(q db
       '{:find [p1]
         :where [[p1 :name n]
                 [p1 :last-name n]
                 [p1 :name name]]
         :in [name]}
       "Alice"))

(comment
  (mount/start #'db)
  (mount/stop #'db)

  (def schema [{:db/ident :person/name
                :db/valueType :db.type/string
                :db/cardinality :db.cardinality/one
                :db/doc "The name of a person"}

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
  (doseq [command (create-table-commands :dbtype/sqlite schema)]
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
  (transact db {:tx-data data})
  (transact db {:tx-data films})

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
            :where [[?e :person/name "Alice"]]}))

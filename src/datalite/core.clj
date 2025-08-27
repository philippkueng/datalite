(ns datalite.core
  (:require
    [clojure.set :as set]
    [datalite.config :refer [schema-table-name transactions-table-name]]
    [datalite.utils :as utils]
    [datalite.utils :refer [replace-dashes-with-underlines]]
    [datalite.schema :refer [create-table-commands
                             drop-table-commands
                             create-full-text-search-table-commands] :as schema]
    [datalite.query-conversion :refer [datalog->sql]]
    [datalite.serialisation :refer [encode decode]]
    [mount.core :as mount]
    [clojure.java.jdbc :as jdbc]))

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

(defn get-schema
  [connection]
  (-> (jdbc/query connection (format "select * from %s limit 1" schema-table-name))
    first
    :schema
    (decode :msgpack)))

(defn transact
  "Turn lists of maps into insert calls"
  [connection {:keys [tx-data] :as data}]
  (assert (some? tx-data) "The data to be added must be wrapped within a :tx-data map")
  (ensure-required-tables! connection)

  ;; Persist the transactions' tx-data into the transactions table
  (jdbc/insert! connection (keyword transactions-table-name) {:data (encode tx-data :msgpack)})

  ;; Check if the tx-data is a schema (my assumption is that transact is either called with schema information or data but not mixed)
  (if (every? #(some? (:db/ident %)) tx-data)
    (do
      ;; Persist the schema.
      (jdbc/insert! connection (keyword schema-table-name) {:schema (encode tx-data :msgpack)})

      ;; Apply the schema.
      (create-tables! connection tx-data))

    (doseq [entry tx-data]
      ;; An entry can either be a map of asserts or a vector of a single assert.
      (cond
        ;; A map where each key represents an individual assert.
        (map? entry)
        (let [table-name (->> entry keys first namespace)]
          (jdbc/insert! connection

            ;; table-name
            (keyword table-name)

            ;; remove-namespaces-from-map
            (reduce (fn [non-namespaced-entry namespaced-key]
                      (conj non-namespaced-entry
                        {(-> namespaced-key name replace-dashes-with-underlines keyword) (namespaced-key entry)}))
              {}
              (keys entry))))

        ;; A vector which asserts to a single attribute. Can be multiple asserts in case of a one-to-many relationship
        (and (vector? entry)
          (contains? #{:db/add :db/retract} (first entry)))
        (let [schema (get-schema connection)]
          ;; ensure the shape matches my expectations
          ;; todo do more checks with a schema checker (eg. Clojure Spec or Malli)
          (assert (= 4 (count entry)) "There are not enough attributes in the addition or retraction")

          ;; check if the attribute mentioned is part of the schema and to which table it belongs
          (let [attribute (nth entry 2)]
            ;; todo check for :db/add and :db/retract
            (if-let [attribute-schema-entry (->> schema
                                              (filter #(= attribute (:db/ident %)))
                                              first)]
              (if (and (= :db.type/ref (:db/valueType attribute-schema-entry))
                    (= :db.cardinality/many (:db/cardinality attribute-schema-entry)))
                ;; if it's a reference type - we'll have to check its' cardinality
                ;;  - if it's a one -> we can just set it
                ;;  - if it's a many -> we can set it differently
                (condp = (first entry)
                  :db/add
                  (jdbc/insert! connection
                    (utils/join-table-name (:db/ident attribute-schema-entry))
                    {(keyword (format "%s_id" (-> attribute-schema-entry :db/ident (namespace)))) (nth entry 1)
                     (keyword (format "%s_id" (-> attribute-schema-entry :db/references (namespace)))) (nth entry 3)})

                  :db/retract
                  (jdbc/delete! connection
                    (utils/join-table-name (:db/ident attribute-schema-entry))
                    [(format "%s = ? and %s = ?"
                       (format "%s_id" (-> attribute-schema-entry :db/ident (namespace)))
                       (format "%s_id" (-> attribute-schema-entry :db/references (namespace))))
                     (nth entry 1)
                     (nth entry 3)])

                  :else (throw "Invalid transact operation."))

                ;; if it's a normal type -> we can run some additional checks or just attempt to set it
                ;;  and let the database library handle any errors.
                (condp = (first entry)
                  :db/add
                  (jdbc/update! connection
                    (-> attribute-schema-entry :db/ident (namespace) keyword)
                    {(-> attribute name keyword) (nth entry 3)}
                    ["id = ?" (nth entry 1)])

                  :db/retract
                  (jdbc/update! connection
                    (-> attribute-schema-entry :db/ident (namespace) keyword)
                    {(-> attribute name keyword) nil}
                    ["id = ?" (nth entry 1)])))

              (throw "The attribute isn't defined in any prior schema.")))))

        ;; todo how to throw errors properly?
        :else (throw "Not implement transact payload")))))

(comment

  (get-schema films-and-cast/conn)

  )

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
  (let [schema (get-schema connection)
        sql-query (datalog->sql schema datalog-query)
        ;_print-sql-query (println sql-query)
        ]
    (->> sql-query
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

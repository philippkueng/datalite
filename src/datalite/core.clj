(ns datalite.core
  (:require
    [mount.core :as mount]
    [clojure.java.jdbc :as jdbc]
    [clojure.string :as str]))

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

(defn create-table-command
  "Turns a schema definition as required with Datomic into

  ```
  create table person (age INTEGER, id INTEGER, name TEXT)
  ```
  "
  [schema]
  (let [table-name (->> (first schema)
                     :db/ident
                     namespace)

        fields-and-types (->> schema
                           ;; TODO: 11.06.2022 currently only the single cardinality attributes are supported
                           (filter #(= :db.cardinality/one (:db/cardinality %)))
                           (map (fn [attribute]
                                  (str
                                    (name (:db/ident attribute))
                                    " "
                                    (condp = (:db/valueType attribute)
                                      :db.type/string "TEXT"
                                      :db.type/long "INTEGER"))))
                           (str/join ", "))]
    (str "create table " table-name " (" fields-and-types ")")))

(defn drop-table-command
  "Does the inverse of `create-table-command` and creates the command to drop the table."
  [schema]
  (let [table-name (->> (first schema)
                     :db/ident
                     namespace)]
    (str "drop table " table-name)))

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
                    {(keyword (name namespaced-key)) (namespaced-key entry)}))
          {}
          (keys entry))))))

(comment
  (mount/start #'db)
  (mount/stop #'db)

  (def person-schema [{:db/ident :person/id
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one
                       :db/doc "The id of the person"}

                      {:db/ident :person/name
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one
                       :db/doc "The name of a person"}

                      {:db/ident :person/age
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one
                       :db/doc "The age of a person"}])

  (create-table-command person-schema)
  (jdbc/execute! db (create-table-command person-schema))

  (def data [{:person/id 12
              :person/name "Alice"
              :person/age 29}
             {:person/id 13
              :person/name "Bob"
              :person/age 28}])

  #_(jdbc/insert! db :person {:id 12 :name "Alice" :age 29})
  (transact db data)

  (jdbc/get-by-id db :person 12)
  (jdbc/find-by-keys db :person {:name "Alice"})
  #_(jdbc/execute! db (drop-table-command (extract-schema data)))

  (= java.lang.String (type "something"))

  )
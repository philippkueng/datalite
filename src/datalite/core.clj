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

;; ----
;; create the table from the data
;; ----

(defn extract-schema
  "Assumes that a record only has keys belonging to the same namespace and no nested maps."
  [records]
  ;; TODO: 10.06.2022 simplifying things for now and only generating the schema from the first record map.
  (let [single-record (first records)
        table-name (->> single-record
                     keys
                     (map namespace)
                     distinct
                     first)
        fields (->> single-record
                 keys
                 (map name)
                 sort)]
    (map (fn [field]
           {:field-name field
            :field-type (condp = (type ((keyword (str table-name "/" field)) single-record))
                          Long "INTEGER"
                          String "TEXT")
            :table-name table-name}) fields)))

(defn create-table-command
  "Turns
  ```
  ({:field-name \"age\", :field-type \"INTEGER\", :table-name \"person\"}\n {:field-name \"id\", :field-type \"INTEGER\", :table-name \"person\"}\n {:field-name \"name\", :field-type \"TEXT\", :table-name \"person\"})
  ```

  into

  ```
  create table person (age INTEGER, id INTEGER, name TEXT)
  ```
  "
  [schema]
  (let [table-name (:table-name (first schema))
        fields-and-types (->> schema
                           (map #(str (:field-name %) " " (:field-type %)))
                           (str/join ", "))]
    (str "create table " table-name " (" fields-and-types ")")))

(defn drop-table-command
  "Does the inverse of `create-table-command` and creates the command to drop the table."
  [schema]
  (let [table-name (:table-name (first schema))]
    (str "drop table " table-name)))

(comment
  (mount/start #'db)

  (def data [{:person/id 12
              :person/name "Alice"
              :person/age 29}])

  (jdbc/execute! db (create-table-command (extract-schema data)))
  (jdbc/insert! db :person {:id 12 :name "Alice" :age 29})
  (jdbc/get-by-id db :person 12)
  (jdbc/find-by-keys db :person {:name "Alice"})
  (jdbc/execute! db (drop-table-command (extract-schema data)))

  (= java.lang.String (type "something"))

  )
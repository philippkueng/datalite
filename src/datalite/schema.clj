(ns datalite.schema
  (:require [datalite.utils :refer [replace-dashes-with-underlines]]
            [clojure.string :as str]))

(defn- schema->tables
  [schema]
  (->> schema
    (map :db/ident)
    (map namespace)
    distinct))

(defn- schema->fields-and-types
  "Helper function only considers single cardinality attributes."
  [schema table]
  (->> schema
    (filter (fn [attribute] (= table (-> attribute :db/ident namespace))))
    (filter #(= :db.cardinality/one (:db/cardinality %)))
    (map (fn [attribute]
           {:field-name (-> (:db/ident attribute)
                          name
                          replace-dashes-with-underlines)
            :field-type (condp = (:db/valueType attribute)
                          :db.type/string "TEXT"
                          :db.type/long "INTEGER")}))))

(defn create-table-commands
  "Turns a schema definition as required with Datomic into lists of creation commands eg.

  ```
  '(\"create table person (age INTEGER, id INTEGER, name TEXT)\")
  ```
  "
  [schema]
  (->> (schema->tables schema)
    (map (fn [table]
           (let [field-and-types-part (->> (schema->fields-and-types schema table)
                                        (map (fn [{:keys [field-name field-type]}]
                                               (str field-name " " field-type)))
                                        (str/join ", "))]
             (str "create table " table " (" field-and-types-part ")"))))))

(defn drop-table-commands
  "Does the inverse of `create-table-command` and creates the command to drop the table."
  [schema]
  (->> (schema->tables schema)
    (map #(str "drop table " %))))

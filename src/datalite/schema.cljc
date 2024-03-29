(ns datalite.schema
  (:require [datalite.utils :refer [replace-dashes-with-underlines]]
            [clojure.string :as str]))

(defn- schema->tables
  [schema]
  (->> schema
    (map :db/ident)
    (map namespace)
    distinct))

(defn- schema->attributes-of-table
  [table schema]
  (filter (fn [attribute] (= table (-> attribute :db/ident namespace))) schema))

(defn- only-single-cardinality-attributes
  [attributes]
  (filter #(= :db.cardinality/one (:db/cardinality %)) attributes))

(defn- schema->fields-and-types
  "Helper function only considers single cardinality attributes."
  [schema table]
  (->> schema
    (schema->attributes-of-table table)
    (only-single-cardinality-attributes)
    (map (fn [attribute]
           {:field-name (-> (:db/ident attribute)
                          name
                          replace-dashes-with-underlines)
            :field-type (condp = (:db/valueType attribute)
                          :db.type/string "TEXT"
                          :db.type/long "INTEGER"
                          :db.type/boolean "INTEGER")}))))

(defn create-table-commands
  "Turns a schema definition as required with Datomic into lists of creation commands eg.

  ```
  '(\"CREATE TABLE person (age INTEGER, id INTEGER, name TEXT)\")
  ```
  "
  [schema]
  (->> (schema->tables schema)
    (map (fn [table]
           (let [field-and-types-part (->> (cons
                                             {:field-name "id"
                                              :field-type "INTEGER PRIMARY KEY AUTOINCREMENT"}
                                             (schema->fields-and-types schema table))
                                        (map (fn [{:keys [field-name field-type]}]
                                               (str field-name " " field-type)))
                                        (str/join ", "))]
             (str "CREATE TABLE " table " (" field-and-types-part ")"))))))

(defn- schema->full-text-search-fields
  [schema table]
  (->> schema
    (schema->attributes-of-table table)
    (only-single-cardinality-attributes)
    (filter #(some? (:db/full-text-search %)))
    (map #(-> % :db/ident name replace-dashes-with-underlines))))

(defn create-full-text-search-table-commands
  "Creates the equivalent of

  create virtual table film_fts using fts5 (
    title,
    genre,
    content='film',
    content_rowid='id'
  )

  CREATE TRIGGER film_ai AFTER INSERT ON film
    BEGIN
        INSERT INTO film_fts (rowid, title, genre)
        VALUES (new.id, new.title, new.genre);
    END

  CREATE TRIGGER film_ad AFTER DELETE ON film
    BEGIN
      INSERT INTO film_fts (film_fts, rowid, title, genre)
      VALUES ('delete', old.id, old.title, old.genre);
    END

  CREATE TRIGGER film_au AFTER UPDATE ON film
    BEGIN
      INSERT INTO film_fts (film_fts, rowid, title, genre)
      VALUES ('delete', old.id, old.title, old.genre);
      INSERT INTO film_fts (rowid, title, genre)
      VALUES (new.id, new.title, new.genre);
    END;
  "
  [schema]
  (->> (schema->tables schema)
    (map (fn [table]
           (let [fields (schema->full-text-search-fields schema table)]
             (when (not-empty fields)
               (list
                 ;; virtual full-text search table
                 (str "create virtual table " table "_fts using fts5 ("
                   (str/join "," fields)
                   ",content='" table "',content_rowid='id')")

                 ;; after insert trigger
                 (str "create trigger " table "_ai after insert on " table
                   " begin insert into " table "_fts (rowid," (str/join "," fields) ") "
                   "values (new.id,new." (str/join ",new." fields) "); end")

                 ;; after delete trigger
                 (str "create trigger " table "_ad after delete on " table
                   " begin insert into " table "_fts (" table "_fts,rowid," (str/join "," fields) ")"
                   " values ('delete',old.id,old." (str/join ",old." fields) "); end")

                 ;; after update trigger
                 (str "create trigger " table "_au after update on " table
                   " begin insert into " table "_fts (" table "_fts,rowid," (str/join "," fields) ")"
                   " values ('delete',old.id,old." (str/join ",old." fields) ");"
                   " insert into " table "_fts (rowid," (str/join "," fields) ")"
                   " values (new.id,new." (str/join ",new." fields) "); end;"))))))
    flatten
    (remove nil?)))

(defn drop-table-commands
  "Does the inverse of `create-table-command` and creates the command to drop the table."
  [schema]
  (->> (schema->tables schema)
    (map #(str "drop table " %))))

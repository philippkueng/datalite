(ns datalite.schema
  (:require [datalite.utils :as utils :refer [replace-dashes-with-underlines ordered-table-names]]
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
  [dbtype schema table]
  (->> schema
       (schema->attributes-of-table table)
       (only-single-cardinality-attributes)
       (map (fn [attribute]
              {:field-name (-> (:db/ident attribute)
                               name
                               replace-dashes-with-underlines)
               :field-type (condp = (:db/valueType attribute)
                             :db.type/string (condp = dbtype
                                               :dbtype/sqlite "TEXT"
                                               :dbtype/duckdb "VARCHAR"
                                               :dbtype/postgresql "VARCHAR")
                             :db.type/keyword (condp = dbtype
                                                :dbtype/sqlite "TEXT"
                                                :dbtype/duckdb "VARCHAR"
                                                :dbtype/postgresql "VARCHAR")
                             :db.type/long "INTEGER"
                             :db.type/boolean "INTEGER"
                             :db.type/ref "INTEGER")}))))

(defn- join-table-attributes
  "Returns a sequence of maps describing join tables needed for many-to-many/ref attributes."
  [schema]
  (for [attr schema
        :when (and (= :db.cardinality/many (:db/cardinality attr))
                (= :db.type/ref (:db/valueType attr))
                (:db/references attr))]
    (let [tables (ordered-table-names [(namespace (:db/ident attr))
                                       (-> (:db/references attr) namespace)])]
      {:name (utils/join-table-name (:db/ident attr))
       :columns (map #(str % "_id") tables)})))

(defn create-table-commands
  "Turns a schema definition as required with Datomic into lists of creation commands. Example: 'CREATE TABLE person (age INTEGER, id INTEGER, name TEXT)'"
  [dbtype schema]
  (let [sequence-statements (condp = dbtype
                              :dbtype/duckdb (for [table (schema->tables schema)]
                                               (format "CREATE SEQUENCE %s_id_seq" table))
                              '())
        main-tables
        (for [table (schema->tables schema)]
          (let [field-and-types-part (->> (cons
                                           {:field-name "id"
                                            :field-type (condp = dbtype
                                                          :dbtype/sqlite "INTEGER PRIMARY KEY AUTOINCREMENT"
                                                          :dbtype/duckdb (format "INTEGER PRIMARY KEY DEFAULT nextval('%s_id_seq')" table)
                                                          :dbtype/postgresql "INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY")}
                                            (schema->fields-and-types dbtype schema table))
                                       (map (fn [{:keys [field-name field-type]}]
                                              (str field-name " " field-type)))
                                       (str/join ", "))]
            (str "CREATE TABLE " table " (" field-and-types-part ")")))
        join-tables
        (for [{:keys [name columns]} (join-table-attributes schema)]
          (str "CREATE TABLE " name
               " (" (str/join ", " (map #(str % " INTEGER") columns)) ")"))
        supporting-tables
        (list "CREATE TABLE schema (schema TEXT)")]
    (concat sequence-statements main-tables join-tables supporting-tables)))

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


(comment
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
                     :references #{:film/id}                ;; an addition that isn't needed by Datomic but helps us
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
                     :doc "The URL where one can find out about the film"}]]
    (= schema (read-string (pr-str schema))))


  )

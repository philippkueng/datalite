(ns datalite.schema
  (:require [datalite.config :refer [schema-table-name
                                     transactions-table-name
                                     xt-id-mapping-table-name]]
            [datalite.utils :as utils :refer [replace-dashes-with-underlines
                                              ordered-table-names
                                              remove-line-breaks-and-trim
                                              keyword->sql-name]]
            [clojure.string :as str]))

;; as of https://www.perplexity.ai/search/when-wanting-to-execute-this-q-HH0shjFvRBWUBpx_loCjVQ
(def reserved-postgresql-keywords #{"all"
                                    "analyse"
                                    "analyze"
                                    "and"
                                    "any"
                                    "array"
                                    "as"
                                    "asc"
                                    "asymmetric"
                                    "authorization"
                                    "between"
                                    "binary"
                                    "both"
                                    "case"
                                    "cast"
                                    "check"
                                    "collate"
                                    "collation"
                                    "column"
                                    "concurrently"
                                    "constraint"
                                    "create"
                                    "cross"
                                    "current_catalog"
                                    "current_date"
                                    "current_role"
                                    "current_schema"
                                    "current_time"
                                    "current_timestamp"
                                    "current_user"
                                    "default"
                                    "deferrable"
                                    "desc"
                                    "distinct"
                                    "do"
                                    "else"
                                    "end"
                                    "except"
                                    "false"
                                    "fetch"
                                    "for"
                                    "foreign"
                                    "freeze"
                                    "from"
                                    "full"
                                    "grant"
                                    "group"
                                    "having"
                                    "ilike"
                                    "in"
                                    "initial"
                                    "inner"
                                    "intersect"
                                    "into"
                                    "is"
                                    "isnull"
                                    "join"
                                    "lateral"
                                    "leading"
                                    "left"
                                    "like"
                                    "limit"
                                    "localtime"
                                    "localtimestamp"
                                    "natural"
                                    "not"
                                    "notnull"
                                    "null"
                                    "offset"
                                    "on"
                                    "only"
                                    "or"
                                    "order"
                                    "outer"
                                    "overlaps"
                                    "placing"
                                    "primary"
                                    "references"
                                    "returning"
                                    "right"
                                    "select"
                                    "session_user"
                                    "similar"
                                    "some"
                                    "symmetric"
                                    "table"
                                    "then"
                                    "to"
                                    "trailing"
                                    "true"
                                    "union"
                                    "unique"
                                    "user"
                                    "using"
                                    "variadic"
                                    "verbose"
                                    "when"
                                    "where"
                                    "window"
                                    "with"})

(defn reserved-keyword? [column-name]
  (contains? reserved-postgresql-keywords (if (keyword? column-name)
                                            (keyword->sql-name column-name)
                                            column-name)))

(defn escape-reserved-keyword [column-name]
  (format "\"%s\"" (if (keyword? column-name)
                     (keyword->sql-name column-name)
                     column-name)))

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
              {:field-name (let [field-name (-> (:db/ident attribute)
                                                name
                                                replace-dashes-with-underlines)]
                             (if (reserved-keyword? field-name)
                               (escape-reserved-keyword field-name)
                               field-name))
               :field-type (condp = (:db/valueType attribute)
                             :db.type/string (condp = dbtype
                                               :dbtype/sqlite "TEXT"
                                               :dbtype/duckdb "VARCHAR"
                                               :dbtype/postgresql "VARCHAR")
                             :db.type/keyword (condp = dbtype
                                                :dbtype/sqlite "TEXT"
                                                :dbtype/duckdb "TEXT"
                                                :dbtype/postgresql "TEXT")
                             :db.type/uuid (condp = dbtype
                                             :dbtype/sqlite "TEXT"
                                             :dbtype/duckdb "UUID"
                                             :dbtype/postgresql "UUID")
                             :db.type/long "INTEGER"
                             :db.type/bigint (condp = dbtype
                                               :dbtype/sqlite "INTEGER"
                                               :dbtype/duckdb "BIGINT"
                                               :dbtype/postgresql "BIGINT")
                             :db.type/bigdec (condp = dbtype
                                               :dbtype/sqlite "NUMERIC"
                                               :dbtype/duckdb "NUMERIC"
                                               :dbtype/postgresql "NUMERIC")
                             :db.type/boolean "INTEGER"
                             :db.type/ref "INTEGER"

                             :db.type/json "JSONB"
                             :db.type/interval "INTERVAL"
                             :db.type/date "DATE"
                             :db.type/instant (condp = dbtype
                                                :dbtype/sqlite "TEXT"
                                                :dbtype/duckdb "TIMESTAMP"
                                                :dbtype/postgresql "TIMESTAMP WITH TIME ZONE"))}))))

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
       :columns (->> tables
                  (map (fn [involved-table] {:field-name (str involved-table "_id")
                                             :field-type "INTEGER"})))})))

(defn create-internal-table-commands
  "Generates the SQL commands for the required database to track schemas"
  [dbtype]
  (let [id-type (fn [sequence-prefix]
                  (condp = dbtype
                    :dbtype/sqlite "INTEGER PRIMARY KEY AUTOINCREMENT"
                    :dbtype/duckdb (format "INTEGER PRIMARY KEY DEFAULT nextval('%s_id_seq')" sequence-prefix)
                    :dbtype/postgresql "INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY"))
        blob-type (condp = dbtype
                    :dbtype/sqlite "BLOB"
                    :dbtype/duckdb "BLOB"
                    :dbtype/postgresql "BYTEA")
        timestamp-type (condp = dbtype
                         :dbtype/sqlite "DATETIME DEFAULT (CURRENT_TIMESTAMP)"
                         :dbtype/duckdb "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"
                         :dbtype/postgresql "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP")]
    (->> (list
           (when (= :dbtype/duckdb dbtype)
             (format "CREATE SEQUENCE %s_id_seq" schema-table-name))
           (format "CREATE TABLE %s (id %s, schema %s, tx_time %s)"
             schema-table-name
             (id-type schema-table-name)
             blob-type
             timestamp-type)
           (when (= :dbtype/duckdb dbtype)
             (format "CREATE SEQUENCE %s_id_seq" transactions-table-name))
           (format "CREATE TABLE %s (id %s, data %s, tx_time %s)"
             transactions-table-name
             (id-type schema-table-name)
             blob-type
             timestamp-type)
           (format "CREATE TABLE %s (xt_id %s UNIQUE, table_name TEXT, internal_entity_id INTEGER)"
                   xt-id-mapping-table-name
                   (condp = dbtype
                     :dbtype/sqlite "TEXT"
                     :dbtype/duckdb "UUID"
                     :dbtype/postgresql "UUID")))
      (remove nil?))))

(defn create-table-commands
  "Turns a schema definition as required with Datomic into lists of creation commands. Example: 'CREATE TABLE person (age INTEGER, id INTEGER, name TEXT)'"
  [dbtype schema]
  (let [sequence-statements (condp = dbtype
                              :dbtype/duckdb (for [table (schema->tables schema)]
                                               (format "CREATE SEQUENCE %s_id_seq" table))
                              '())
        timestamp-columns (list
                            {:field-name "valid_from"
                             :field-type (condp = dbtype
                                           :dbtype/sqlite "INTEGER NOT NULL"
                                           :dbtype/duckdb "BIGINT NOT NULL"
                                           :dbtype/postgresql "BIGINT NOT NULL")}
                            ;; if the valid_to is open-ended 'infinity' - we'll keep the value nil.
                            {:field-name "valid_to"
                             :field-type (condp = dbtype
                                           :dbtype/sqlite "INTEGER"
                                           :dbtype/duckdb "BIGINT"
                                           :dbtype/postgresql "BIGINT")})
        main-tables
        (for [table (schema->tables schema)]
          (let [field-and-types-part (->> (concat
                                            (list
                                              ;; SQLite doesn't have the capability to increase a non primary key integer
                                              ;;  so we'll have to add another columns rowid so that an after trigger can fill
                                              ;;  in an increasing id using a trigger.
                                              (when (= :dbtype/sqlite dbtype)
                                                {:field-name "rowid"
                                                 :field-type "INTEGER PRIMARY KEY AUTOINCREMENT"})
                                              {:field-name "id"
                                               :field-type (condp = dbtype
                                                             :dbtype/sqlite "INTEGER"
                                                             :dbtype/duckdb (format "INTEGER DEFAULT nextval('%s_id_seq')" table)
                                                             :dbtype/postgresql "INTEGER GENERATED BY DEFAULT AS IDENTITY")})
                                            timestamp-columns
                                            (schema->fields-and-types dbtype schema table))
                                       (remove nil?)
                                       (map (fn [{:keys [field-name field-type]}]
                                              (str field-name " " field-type)))
                                       (str/join ", "))]
            (str "CREATE TABLE " table " (" field-and-types-part ")")))
        join-tables
        (for [{:keys [name columns]} (join-table-attributes schema)]
          (str "CREATE TABLE " name
               " (" (str/join ", " (map #(str/join " " (list (:field-name %) (:field-type %)))
                                     (concat columns timestamp-columns))) ")"))
        triggers (condp = dbtype
                   :dbtype/sqlite (for [table (schema->tables schema)]
                                    (format
                                      (remove-line-breaks-and-trim
                                        "CREATE TRIGGER set_id_after_insert_for_%s
                                         AFTER INSERT ON %s
                                         FOR EACH ROW
                                         WHEN NEW.id IS NULL
                                         BEGIN
                                           UPDATE %s
                                           SET id = (SELECT COALESCE(MAX(id), 0) + 1 FROM %s WHERE rowid < NEW.rowid)
                                           WHERE rowid = NEW.rowid;
                                         END")
                                      table
                                      table
                                      table
                                      table))
                   '())]
    (concat sequence-statements main-tables join-tables triggers)))

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

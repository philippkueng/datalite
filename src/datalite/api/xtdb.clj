(ns datalite.api.xtdb
  (:require [datalite.core :as core]
            [datalite.keywords.xtdb :as xt]
            [datalite.utils :refer [replace-dashes-with-underlines] :as utils]
            [clojure.set :as set]
            [clojure.java.jdbc :as jdbc]
            #_[next.jdbc :as jdbc]
            #_[next.jdbc.sql :as jdbc-sql]
            [taoensso.tufte :as tufte])
  (:import [java.time Instant]
           [java.sql Timestamp]))

(defn xt-id->internal-id
  "A utility function to map an XTDB :xt/id value with the database internals' id for that entity."
  [connection table-name xt-id]
  (->> (jdbc/query connection
         [(format "select id from %s where xt_id = ? limit 1" table-name)
          xt-id])
    first
    :id))

(defn submit-tx
  "Function to be used for inserting data in XTDB v1 style."
  [connection tx-data]
  (tufte/p :ensure-required-tables
    (core/ensure-required-tables! connection))
  (tufte/p :persist-transaction-data
    (core/persist-transaction-data connection tx-data))

  (let [schema (tufte/p :get-schema (core/get-schema connection))
        refs-of-cardinality-many (->> schema
                                   (filter #(and
                                              (= :db.cardinality/many (-> % :db/cardinality))
                                              (= :db.type/ref (-> % :db/valueType)))))]

    (if
      ;; A crude check if the data that's being submitted is a schema structure.
      (and
          (-> tx-data first second :schema some?)
          (->> tx-data first second :schema (every? #(-> % :db/ident some?))))

      (core/apply-schema connection (-> tx-data first second :schema))

      (tufte/p :submit-tx-process-command
        (doseq [[operation entry] tx-data]
          (let [valid-time (Instant/now)
                valid-time-in-milliseconds (.toEpochMilli valid-time)]
            (condp = operation

              ;; for any given map (entry)
              ;; -> rename the :xt/id to the one matching this entity's table.
              ;; -> consult the schema and separate the :db.cardinality/many ref attributes from the rest
              ;; -> upsert the map with against the table
              ;; -> upsert all the :db.cardinality/many refs one by one
              ::xt/put (let [table-name (->> (keys entry)
                                          (map namespace)
                                          (remove #(= "xt" %))
                                          ;; todo ensure we only have a single table name left in the list.
                                          first)
                             refs-of-cardinality-many-keys (->> refs-of-cardinality-many
                                                             (map :db/ident)
                                                             set)
                             renamed-xt-id (if (nil? (:xt/id entry))
                                             (throw (ex-info ":xt/id is missing" entry))
                                             (-> entry
                                               (assoc (keyword table-name "xt_id") (:xt/id entry))
                                               (dissoc :xt/id)))
                             cardinality-many-ref-entries (select-keys renamed-xt-id refs-of-cardinality-many-keys)
                             upsertable-entries (apply dissoc renamed-xt-id refs-of-cardinality-many-keys)

                             ;; Reference attributes which the entity previously might have had and for which we have to
                             ;;  ensure that all the currently valid relationships have their valid_to set to valid-time.
                             omitted-cardinality-many-ref-entries (set/difference refs-of-cardinality-many-keys (-> cardinality-many-ref-entries keys set))

                             ;_print-refs-of-cardinality-many-keys (println (pr-str refs-of-cardinality-many-keys))
                             ;_print-cardinality-many-ref-entries (println (pr-str cardinality-many-ref-entries))
                             ;_print-omitted (println (pr-str omitted-cardinality-many-ref-entries))
                             ]
                         (jdbc/with-db-transaction [t-conn connection]

                           ;; Check if there's an entry in the database already for the provided :xt/id & valid_at time
                           (jdbc/execute! t-conn
                             [(format "update %s set valid_to = ? where xt_id = ? and valid_from < ? and (valid_to > ? or valid_to is null)"
                                table-name)
                              valid-time-in-milliseconds
                              (:xt/id entry)
                              valid-time-in-milliseconds
                              valid-time-in-milliseconds])

                           ;; If there are segments which are in the database and have their :valid_from after the :valid-at-time, then we'll need to fetch the closest one to use
                           ;;  it's :valid_from as the :valid_to of the current segment to be inserted.

                           (let [
                                 ;; Check if there's already an entry with this :xt/id in the table.
                                 id (xt-id->internal-id t-conn table-name (:xt/id entry))

                                 ;; Remove the namespaces from the keys prior to insertion.
                                 insertion-map (merge (reduce (fn [non-namespaced-entry namespaced-key]
                                                                (conj non-namespaced-entry
                                                                  {(-> namespaced-key name replace-dashes-with-underlines keyword) (namespaced-key upsertable-entries)}))
                                                        {}
                                                        (keys upsertable-entries))
                                                 {:valid_from valid-time-in-milliseconds}
                                                 (when (number? id)
                                                   {:id id}))]
                             (if-let [existing-valid-to (->> (jdbc/query t-conn
                                                               [(format "select valid_to from %s where xt_id = ? and valid_from > ? order by valid_from asc limit 1"
                                                                  table-name)
                                                                (:xt/id entry)
                                                                valid-time-in-milliseconds])
                                                          first
                                                          :valid_to)]
                               ;; If there's an existing segment after the current valid-time
                               (jdbc/insert! t-conn (keyword table-name) (assoc insertion-map :valid_to existing-valid-to))

                               ;; There's no segment after the current valid-time and this segment marks the one being valid until the end of time.
                               (jdbc/insert! t-conn (keyword table-name) insertion-map)))

                           ;; If the tx-data contained attributes which are tied to cardinality/many references
                           ;;  go through them all one by one and insert them into their respective join table.
                           (let [
                                 ;; Fetch the integer `id` of the entity just inserted (doesn't need to be the same row, we just need to match the :xt/id to the `id`.
                                 id (xt-id->internal-id t-conn table-name (:xt/id entry))]

                             (doseq [[attribute value] cardinality-many-ref-entries]
                               ;; Get the schema entry for this attribute to know with which table to join and how
                               (let [attribute-schema-entry (->> refs-of-cardinality-many
                                                              (filter #(= attribute (-> % :db/ident)))
                                                              first)
                                     values (if-not (or (set? value) (seq? value) (vector? value) (list? value))
                                              (list value)
                                              value)
                                     join-table-name (utils/join-table-name (:db/ident attribute-schema-entry))
                                     source-table-column-name (format "%s_id" (-> attribute-schema-entry :db/ident (namespace)))
                                     target-table-column-name (format "%s_id" (-> attribute-schema-entry :db/references (namespace)))]

                                 (doseq [value values]

                                   ;; If there's already an entry that matches both ids AND the valid-time is in between its valid_from and valid_to,
                                   ;; then set it's valid_to to the valid-time.
                                   (jdbc/execute! t-conn
                                     [(format "update %s set valid_to = ? where %s = ? and %s = ? and valid_from < ? and (valid_to > ? or valid_to is null)"
                                        join-table-name
                                        source-table-column-name
                                        target-table-column-name)
                                      valid-time-in-milliseconds
                                      id
                                      value
                                      valid-time-in-milliseconds
                                      valid-time-in-milliseconds])

                                   (let [insertion-map {(keyword source-table-column-name) id
                                                        (keyword target-table-column-name) value
                                                        :valid_from valid-time-in-milliseconds}]
                                     (if-let [existing-valid-to (->> (jdbc/query t-conn
                                                                       [(format "select valid_to from %s where %s = ? and %s = ? and valid_from > ? order by valid_from asc limit 1"
                                                                          join-table-name
                                                                          source-table-column-name
                                                                          target-table-column-name)
                                                                        id
                                                                        value
                                                                        valid-time-in-milliseconds])
                                                                  first
                                                                  :valid_to)]
                                       ;; After this check if there are entries that match both ids and have their valid_from time after the valid-time,
                                       ;;  if so, take the earliest valid_from time and use that for the current entry's valid_to value.
                                       (jdbc/insert! t-conn join-table-name (assoc insertion-map :valid_to existing-valid-to))

                                       ;; If there's no entry matching the ids after the valid time, then just insert the entry and leave the
                                       ;;  valid_to nil.
                                       (jdbc/insert! t-conn join-table-name insertion-map))))

                                 ;; Given the values (compare them with what the values would have looked like 1 millisecond prior,
                                 ;;  If there are join table entries which are no longer mentioned, change their valid_to value to the
                                 ;;  current valid-time
                                 (let [previously-valid-references (->> (jdbc/query t-conn [(format "select %s from %s where %s = ? and valid_from < ? and (valid_to > ? or valid_to is null)"
                                                                                              target-table-column-name
                                                                                              join-table-name
                                                                                              source-table-column-name)
                                                                                            id
                                                                                            valid-time-in-milliseconds
                                                                                            valid-time-in-milliseconds])
                                                                     (map (keyword target-table-column-name))
                                                                     set)]
                                   (when-let [invalid-references (set/difference previously-valid-references values)]
                                     (doseq [invalid-reference invalid-references]
                                       (jdbc/execute! t-conn [(format "update %s set valid_to = ? where %s = ? and %s = ? and valid_from < ? and (valid_to > ? or valid_to is null)"
                                                                join-table-name
                                                                source-table-column-name
                                                                target-table-column-name)
                                                              valid-time-in-milliseconds
                                                              id
                                                              invalid-reference
                                                              valid-time-in-milliseconds
                                                              valid-time-in-milliseconds]))))
                                 )
                               )

                             ;; Check if there are cardinality/many ref attributes which weren't mentioned in this entry.
                             ;;  For those attributes, check if there are join table entries, and if so, change their valid_to
                             ;;  to the current valid-time.

                             (doseq [omitted-relationship-attribute omitted-cardinality-many-ref-entries]
                               (let [attribute-schema-entry (->> refs-of-cardinality-many
                                                              (filter #(= omitted-relationship-attribute (-> % :db/ident)))
                                                              first)
                                     join-table-name (utils/join-table-name (:db/ident attribute-schema-entry))
                                     source-table-column-name (format "%s_id" (-> attribute-schema-entry :db/ident (namespace)))]
                                 (jdbc/execute! t-conn [(format "update %s set valid_to = ? where %s = ? and valid_from < ? and (valid_to > ? or valid_to is null)"
                                                          join-table-name
                                                          source-table-column-name)
                                                        valid-time-in-milliseconds
                                                        id
                                                        valid-time-in-milliseconds
                                                        valid-time-in-milliseconds])))
                             )))

              ::xt/delete (throw (ex-message "Not implemented."))
              ::xt/evict (throw (ex-message "Not implemented."))
              (throw (ex-message "Operation not implemented."))))))
      ))


  ;; look through all the keys and dissoc the ones being of db.cardinality/many before running a dedicated
  ;;  transact invocation for those (once for :db/add and once for :db/retract).
  ;;  -> for this we'll need to fetch the schema

  )

(defn map->datomic-query
  "A helper function to convert the XT EQL datalog into datomic datalog
   I've used https://www.perplexity.ai/search/help-me-with-a-clojure-macro-t-9dFVKrH0QpCEi4vQOqiVvQ for Perplexity to suggest me a function."
  [m]
  (vec (mapcat (fn [[k v]]
                 (if (vector? v)
                   (cons k v)
                   [k v]))
         m)))

(comment

  (map->datomic-query '{:find [?title ?year ?genre]
                        :where [[?e :movie/title ?title]
                                [?e :movie/release-year ?year]
                                [?e :movie/genre ?genre]
                                [(= ?genre "something")]
                                [?e :movie/release-year 1985]]})

  #_(let [xt-query '{:find [?title ?year ?genre]
                   :where [[?e :movie/title ?title]
                           [?e :movie/release-year ?year]
                           [?e :movie/genre ?genre]
                           [(= ?genre "something")]
                           [?e :movie/release-year 1985]]}

        parsed-query (parser/parse (map->datomic-query xt-query))]
    parsed-query)

  )

(defn q [connection query]
  (core/q connection (map->datomic-query query)))




(ns datalite.api.xtdb
  (:require [datalite.core :as core]
            [datalite.keywords.xtdb :as xt]
            [datalite.utils :refer [replace-dashes-with-underlines] :as utils]
            [clojure.java.jdbc :as jdbc]
            #_[next.jdbc :as jdbc]
            #_[next.jdbc.sql :as jdbc-sql]))

(comment
  ;(xt/submit-tx node [[::xt/put {:xt/id (java.util.UUID/randomUUID)
  ;                               :customer/name "Max Muster"
  ;                               :customer/address "Kreuzstrasse 214"
  ;                               :customer/telephone "+41 440 32 52"}]])

  )

(defn submit-tx
  "Function to be used for inserting data in XTDB v1 style."
  [connection tx-data]
  (core/ensure-required-tables! connection)
  (core/persist-transaction-data connection tx-data)

  (let [schema (core/get-schema connection)
        refs-of-cardinality-many (->> schema
                                   (filter #(and
                                              (= :db.cardinality/many (-> % :db/cardinality))
                                              (= :db.type/ref (-> % :db/valueType)))))]

    (if (every? #(some? (-> % second :db/ident)) tx-data)
      #_(every? #(some? (-> % second :datalite/schema)) tx-data)
      (core/apply-schema connection tx-data)

      #_(->> tx-data
          (map (fn [[operation entry]]
                 ;; if the entry contains an :xt/id entry - look at the other keys and use the table
                 ;;  information from there and re-format :xt/id to eg. person/xt_id
                 (condp = operation
                   ::xt/put (let [refined-entry (if (nil? (:xt/id entry))
                                                  (throw (ex-info ":xt/id is missing" entry))
                                                  (let [entry-table (->> (keys entry)
                                                                      (map namespace)
                                                                      (remove #(= "xt" %))
                                                                      ;; todo ensure we only have a single table name left in the list.
                                                                      first)]
                                                    (-> entry
                                                      (assoc (keyword entry-table "xt_id") (:xt/id entry))
                                                      (dissoc :xt/id))))]
                              refined-entry)

                   ::xt/delete (throw (ex-message "Not implemented."))
                   ::xt/evict (throw (ex-message "Not implemented.")))))
          vec
          (#(core/transact connection {:tx-data %})))

      (doseq [[operation entry] tx-data]
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
                                                         (map :db/ident))
                         renamed-xt-id (if (nil? (:xt/id entry))
                                         (throw (ex-info ":xt/id is missing" entry))
                                         (-> entry
                                           (assoc (keyword table-name "xt_id") (:xt/id entry))
                                           (dissoc :xt/id)))
                         cardinality-many-ref-entries (select-keys renamed-xt-id refs-of-cardinality-many-keys)
                         upsertable-entries (apply dissoc renamed-xt-id refs-of-cardinality-many-keys)]
                     (jdbc/with-db-transaction [t-conn connection]
                       ;; todo check if there's already an entry with the same :xt/id in the database
                       ;;  if so, change the :valid-to to now and insert a new entry.

                       ;; Insert the entries which can just be upserted into an entities row
                       (let [_insertion (jdbc/insert! t-conn

                                          ;; table-name
                                          (keyword table-name)

                                          ;; remove-namespaces-from-map
                                          (reduce (fn [non-namespaced-entry namespaced-key]
                                                    (conj non-namespaced-entry
                                                      {(-> namespaced-key name replace-dashes-with-underlines keyword) (namespaced-key upsertable-entries)}))
                                            {}
                                            (keys upsertable-entries)))

                             ;; fetch the integer `id` of the entity just inserted.
                             {:keys [id]} (jdbc/query t-conn
                                                     [(format "select id from %s where xt_id = ? limit 1" table-name)
                                                      (:xt/id entry)])]

                         (doseq [[attribute value] cardinality-many-ref-entries]
                           ;; get the schema entry for this attribute
                           (let [attribute-schema-entry (->> refs-of-cardinality-many
                                                          (filter #(= attribute (-> % :db/ident)))
                                                          first)
                                 values (if-not (or (set? value) (seq? value) (vector? value) (list? value))
                                          (list value)
                                          value)
                                 _print-attribute-schema-entry (println (pr-str attribute-schema-entry))
                                 _print-values (println (pr-str values))]
                             ;; todo deal with relations which have previously been mentioned but are not part of the current
                             ;;  tx-data and hence need to be marked as no longer valid.

                             (doseq [value values]
                               (jdbc/insert! t-conn
                                 (utils/join-table-name (:db/ident attribute-schema-entry))
                                 (let [payload
                                       {(keyword (format "%s_id" (-> attribute-schema-entry :db/ident (namespace))))
                                        ;; todo find the id of the row which was inserted as part of the same jdbc transaction
                                        id
                                        #_(resolve-lookup-ref connection (nth entry 1))
                                        (keyword (format "%s_id" (-> attribute-schema-entry :db/references (namespace))))
                                        ;; todo think through the case when "value" is a set (I think it can only be a set)
                                        value
                                        #_(resolve-lookup-ref connection (nth entry 3))}
                                       ;_print-payload (println (pr-str payload))
                                       ]
                                   payload)))
                             )
                           )))
                     )

          ::xt/delete (throw (ex-message "Not implemented."))
          ::xt/evict (throw (ex-message "Not implemented."))
          (throw (ex-message "Operation not implemented."))))
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




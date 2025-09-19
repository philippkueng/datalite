(ns load-testing
  (:require [datalite.schema]
            [datalite.core :as d]
            [datalite.api.xtdb :refer [q submit-tx]]
            [datalite.keywords.xtdb :as xt]
            [mount.core :as mount]
            [clojure.java.jdbc :as jdbc]
            [taoensso.tufte :as tufte]))

;; Send `profile` signals to console
(tufte/add-handler! :my-console-handler (tufte/handler:console))

(def schema
  [#:db{:ident :person/xt-id
        :valueType :db.type/uuid
        :cardinality :db.cardinality/one
        :doc "The unique id as defined by XTDB"}

   #:db{:ident :person/name
        :valueType :db.type/string
        :cardinality :db.cardinality/one
        :doc "The name of a person"}

   #:db{:ident :person/date-of-birth
        :valueType :db.type/long
        :cardinality :db.cardinality/one
        :doc "The birthday of a person"}

   #:db{:ident :person/likes-films
        :valueType :db.type/ref
        :cardinality :db.cardinality/many
        :references :film/id                            ;; an addition that isn't needed by Datomic but helps us
        :doc "The films the person likes"}

   #:db{:ident :film/xt-id
        :valueType :db.type/uuid
        :cardinality :db.cardinality/one
        :doc "The unique id as defined by XTDB"}

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
        :doc "The URL where one can find out about the film"}

   #:db{:ident :film/directed-by
        :valueType :db.type/ref
        :cardinality :db.cardinality/one
        :references :person/wikidata-id
        :doc "The person directing the film"}])

(defn uuid [] (java.util.UUID/randomUUID))

(defn setup-connection [dbtype db-uri]
  (let [spec {:connection-uri db-uri}
        conn (jdbc/get-connection spec)]
    (-> spec
      (assoc :connection conn)
      (assoc :dbtype dbtype))))

;; - create a database connection
#_(defonce conn {:connection (jdbc/get-connection {:connection-uri "jdbc:duckdb:memory:"})
                 :dbtype :dbtype/duckdb})

#_(defonce conn {:connection (jdbc/get-connection {:connection-uri "jdbc:postgresql://localhost:5432/datalite-test?user=datalite&password=datalite"})
                 :dbtype :dbtype/postgresql})

#_(defonce conn {:connection (jdbc/get-connection {:connection-uri "jdbc:postgresql://localhost:5433/datalite-dev?user=datalite&password=datalite"})
               :dbtype :dbtype/postgresql})

(defonce conn (setup-connection :dbtype/postgresql "jdbc:postgresql://localhost:5433/datalite-dev?user=datalite&password=datalite"))


(comment
  ;; Delete all tables in the database!
  (let [tables (jdbc/query conn
                 ["SELECT tablename FROM pg_tables WHERE schemaname = 'public'"])]
    (doseq [{:keys [tablename]} tables]
      (jdbc/execute! conn [(str "DROP TABLE IF EXISTS " tablename " CASCADE;")])))


  ;; Apply the schema
  (submit-tx conn [[::xt/put {:schema schema}]])


  (submit-tx conn [[::xt/put {:xt/id (uuid)
                              :person/name "Test"
                              :person/date-of-birth 12}]])

  ;; updating the same entity over and over again
  (tufte/profile
    {:level :info}
    (let [base-entry {:xt/id (uuid)
                      :person/name "Test"
                      :person/date-of-birth 0}]
      (time
        (doall
          (doseq [i (range (* 100 1000))]
            (tufte/p :submit-tx
              (submit-tx conn [[::xt/put (assoc base-entry :person/date-of-birth i)]])))))))

  ;; 1800ms for 1000 entries -> 1.8ms per entry
  ;; 22s for 10k entries -> 2.2ms per entry
  ;; 573s for 100k entries -> 5.7ms per entry (40-70% CPU usage of the postgres container)
  ;; - without any indexes we can see quite a slowdown the more entries one has
  ;; 425s for 100k entries -> 4.2ms per entry (40-70% CPU usage of the postgres container)

  ;; ---
  ;; Profiling using tufte (first run to insert 100k updates to the same entity)
  ;; ---

  ;2025-09-16T20:03:31.454018Z INFO yoda-10.local load-testing[107,3] Tufte pstats
  ;pId                            nCalls        Min      50% ≤      90% ≤      95% ≤      99% ≤        Max       Mean   MAD      Clock  Total
  ;
  ;:submit-tx                    100,000        2ms        5ms        6ms        6ms        7ms       20ms        5ms  ±15%      8.03m   100%
  ;:submit-tx-process-command    100,000        2ms        4ms        5ms        5ms        5ms       19ms        4ms  ±19%      6.44m    80%
  ;:persist-transaction-data     100,000      361μs      586μs      680μs      720μs      812μs       10ms      592μs  ±11%     59.16s    12%
  ;:get-schema                   100,000      108μs      193μs      226μs      247μs      292μs        9ms      199μs  ±12%     19.94s     4%
  ;:ensure-required-tables       100,000      101μs      158μs      186μs      199μs      231μs        4ms      159μs  ±11%     15.95s     3%
  ;
  ;Accounted                                                                                                                    16.06m   200%
  ;Clock                                                                                                                         8.03m   100%


  ;; inserting new entities all the time
  (let [base-entry {:xt/id (uuid)
                    :person/name "Test"
                    :person/date-of-birth 0}]
    (time
      (doall
        (doseq [i (range (* 100 1000))]
          (submit-tx conn [[::xt/put (-> base-entry
                                       (assoc :person/date-of-birth i)
                                       (assoc :xt/id (uuid)))]])))))
  ;; 182s for 100k entries -> 1.8ms per entry (with about 20% CPU load on the postgres container)
  ;; 173s for another 100k entries thereafter (also with the same 20ish% CPU load on the postgres container)
  )


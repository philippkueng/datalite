(ns datalite.core
  (:require
    [datalite.utils :refer [replace-dashes-with-underlines]]
    [datalite.schema :refer [create-table-commands
                             drop-table-commands]]
    [mount.core :as mount]
    [clojure.java.jdbc :as jdbc]))

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
                    {(-> namespaced-key name replace-dashes-with-underlines keyword) (namespaced-key entry)}))
          {}
          (keys entry))))))

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
                :db/fulltext true}

               {:db/ident :film/genre
                :db/valueType :db.type/string
                :db/cardinality :db.cardinality/one
                :db/doc "The genre of the film"
                :db/fulltext true}

               {:db/ident :film/release-year
                :db/valueType :db.type/long
                :db/cardinality :db.cardinality/one
                :db/doc "The year the film was released in theaters"}

               {:db/ident :film/url
                :db/valueType :db.type/string
                :db/cardinality :db.cardinality/one
                :db/doc "The URL where one can find out about the film"}])

  ;; Create the tables.
  (doseq [command (create-table-commands schema)]
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
  (transact db data)
  (transact db films)

  ;; Drop the tables.
  (doseq [command (drop-table-commands schema)]
    (jdbc/execute! db command))

  (jdbc/get-by-id db :person 12)
  (jdbc/find-by-keys db :person {:name "Alice"})
  )
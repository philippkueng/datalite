(ns datalite.query-conversion
  (:require [clojure.set :as set]
            [datalog.parser :as parser]
            [datalite.utils :as utils]
            [clojure.string :as str]))

(defn- enrich-vec-of-maps-with-index [vector]
  (->> vector
       (map-indexed
        (fn [index map]
          (assoc map :index index)))
       (into [])))

(comment
  (enrich-vec-of-maps-with-index [{:foo "bar"} {:bar "foo"}]))

(defn- unconsumed-queries [where-clauses consumed-where-clauses]
  (->> where-clauses
       (remove (fn [clause] (contains? consumed-where-clauses (:index clause))))))

(defn- zero-prefix [number]
  (cond
    (< number 10) (str "00" number)
    (< number 100) (str "0" number)
    :else (str number)))

(defn datalog->sql
  ([schema query]
   (let [parsed-query (parser/parse query)
         consumed-where-clauses (atom #{})
         index-enriched-where-clauses (enrich-vec-of-maps-with-index (:qwhere parsed-query))
         select-fields (let [find-symbols (->> (:qfind parsed-query)
                                            :elements
                                            (map :symbol)
                                            vec)
                             correlating-where-clauses (let [where-clauses index-enriched-where-clauses]
                                                         (->> find-symbols
                                                           (map (fn [find-symbol]
                                                                  (->> where-clauses
                                                                    (filter (fn [where-clause]
                                                                              (= find-symbol (-> where-clause
                                                                                               :pattern
                                                                                               (nth 2)
                                                                                               :symbol))))
                                                                    first)))))
                             _update-consumed-where-clauses! (doseq [clause correlating-where-clauses]
                                                              (swap! consumed-where-clauses conj (:index clause)))]
                         (let [where-clauses index-enriched-where-clauses]
                           (map-indexed
                             (fn [index find-symbol]
                               (let [entity-clause (->> where-clauses
                                                     (filter #(= find-symbol (-> % :pattern (nth 0) :symbol)))
                                                     first)]
                                 (if entity-clause
                                   (let [table (-> entity-clause :pattern (nth 1) :value namespace)]
                                     (str table ".id as field_" (zero-prefix index)))
                                   (let [attribute (-> correlating-where-clauses (nth index) :pattern (nth 1) :value)]
                                     (str
                                       (namespace attribute)
                                       "."
                                       (utils/replace-dashes-with-underlines (name attribute))
                                       " as "
                                       "field_"
                                       (zero-prefix index))))))
                             find-symbols)))
         select-part (str "SELECT " (str/join ", " select-fields))
         involved-tables (->> parsed-query
                           :qwhere
                           (filter :pattern)
                           (map (fn [pattern]
                                  (-> (:pattern pattern)
                                    (nth 1)
                                    :value
                                    namespace)))
                           distinct
                           utils/ordered-table-names)
         ;_pr-involved-tables (clojure.pprint/pprint involved-tables)
         ;; based on what do we decide which table is the base? - take the first one alphabetically for now.
         from-part (str "FROM " (-> involved-tables first))
         ;_pr-unconsumed-where-clauses (clojure.pprint/pprint (unconsumed-queries
         ;                                                      index-enriched-where-clauses
         ;                                                      @consumed-where-clauses))
         join-part (if (> (count involved-tables) 1)
                     ;; we got more than 1 table, hence we'll need to join them

                     ;; in my example queries so far, we're only joining the :person entity with the :film entity
                     ;;  however if we join multiple entities together, we'll need to keep track of how we'd need to join
                     ;;  them. Meaning there'd have to be a pair relationship between eg. person <-> film and another
                     ;;  between film <-> location, and yet another between location <-> country
                     ;;
                     ;; Assuming I have the latest version of the schema at hand we can:
                     ;; -> use the involved-tables and find all the schema entries of :valueType :db.type/ref
                     ;; person <-> film
                     ;; film <-> location
                     ;; location <-> country
                     ;;
                     ;; how can I find a connection eg. from person to country? I must have all the tables involved in
                     ;;  the connection, but I'll need to find a path.

                     ;; return an empty string as only a single table is involved which doesn't require a SQL join

                     ;; Extract the where clauses which match :ref values in the schema as those will have to be used
                     ;;  for the JOINs

                     ;; an example schema entry representing of the joins
                     ;#:db{:ident :person/likes-films
                     ;     :valueType :db.type/ref
                     ;     :cardinality :db.cardinality/many
                     ;     :references #{:film/id}                ;; an addition that isn't needed by Datomic but helps us
                     ;     :doc "The films the person likes"}
                     (let [where-clauses (unconsumed-queries
                                           index-enriched-where-clauses
                                           @consumed-where-clauses)
                           reference-attributes-from-schema (->> schema
                                                              (filter #(= :db.type/ref (:db/valueType %))))
                           reference-attributes-from-schema-idents-only (->> reference-attributes-from-schema
                                                                          (map :db/ident)
                                                                          set)
                           ;_pr-reference-attributes-from-schema (clojure.pprint/pprint reference-attributes-from-schema)
                           join-relevant-where-clauses (->> where-clauses
                                                         (filter #(contains? reference-attributes-from-schema-idents-only (-> % :pattern (nth 1) :value))))
                           ;_pr-join-relevant-where-clauses (clojure.pprint/pprint join-relevant-where-clauses)

                           ;; add the `from` and `to` tables to it so we have all the information about a clause in the same place
                           enriched-where-clauses (->> join-relevant-where-clauses
                                                    (map (fn [clause]
                                                           (let [attribute (-> clause :pattern (nth 1) :value)
                                                                 matching-schema-clause (->> reference-attributes-from-schema
                                                                                          (filter #(= attribute (-> % :db/ident)))
                                                                                          first)]
                                                             (-> clause
                                                               (assoc :attribute attribute)
                                                               (assoc :from-table (-> attribute namespace))
                                                               (assoc :from-column (-> attribute name))
                                                               (assoc :to-table (-> matching-schema-clause :db/references (namespace)))
                                                               (assoc :to-column (-> matching-schema-clause :db/references (name))))))))
                           ;_enriched-where-clauses (clojure.pprint/pprint enriched-where-clauses)
                           from-table "film"
                           join-expressions (loop [join-expressions []
                                                   existing-tables #{from-table}
                                                   where-clauses enriched-where-clauses]
                                              (if (> (count where-clauses) 0)
                                                ;; We need to find the first clause which involved tables overlap with our :existing-tables
                                                (let [clause-to-add (->> where-clauses
                                                                      (filter #(not-empty (set/intersection existing-tables #{(-> % :from-table) (-> % :to-table)})))
                                                                      first)
                                                      ;_start (println "ITERATION START ----")
                                                      ;_ordered-clauses (clojure.pprint/pprint join-expressions)
                                                      ;_existing-tables (clojure.pprint/pprint existing-tables)
                                                      ;_where-clauses (clojure.pprint/pprint where-clauses)
                                                      ;_end (println "ITERATION END ----")
                                                      ]
                                                  ;(Thread/sleep 3000)
                                                  (swap! consumed-where-clauses conj (:index clause-to-add))
                                                  (recur
                                                    (conj join-expressions
                                                      (->> (list
                                                             (format "JOIN %s ON %s.%s = %s.%s"
                                                               (utils/join-table-name (:attribute clause-to-add))
                                                               (:from-table clause-to-add)
                                                               (:from-column clause-to-add)
                                                               (:to-table clause-to-add)
                                                               (:to-column clause-to-add))
                                                             ;; figure out which of the tables this joins is already present
                                                             (when (not (contains? existing-tables (:from-table clause-to-add)))
                                                               (format "JOIN %s on %s.%s = %s.%s"
                                                                 (:from-table clause-to-add)
                                                                 (:from-table clause-to-add)
                                                                 (:from-column clause-to-add)
                                                                 (:to-table clause-to-add)
                                                                 (:to-column clause-to-add)))
                                                             (when (not (contains? existing-tables (:to-table clause-to-add)))
                                                               (format "JOIN %s on %s.%s = %s.%s"
                                                                 (:to-table clause-to-add)
                                                                 (:from-table clause-to-add)
                                                                 (:from-column clause-to-add)
                                                                 (:to-table clause-to-add)
                                                                 (:to-column clause-to-add))))
                                                        (remove nil?)))

                                                    (set/union existing-tables #{(-> clause-to-add :from-table) (-> clause-to-add :to-table)})

                                                    (->> where-clauses
                                                      (remove #(= (:index %) (:index clause-to-add))))))

                                                (flatten join-expressions)))
                           ;_print-join-expressions (clojure.pprint/pprint join-expressions)
                           ]

                       ;; todo with this information we can construct the join of the tables, however how do we ensure the source table is also included?
                       ;;  should we do the `join` part prior to the `from` part, and then put the table into the `from` part which hasn't been mentioned elsewhere?

                       (str/join " " join-expressions))

                     ;; As we only got a single table to work with, leave the join part empty
                     "")
         where-clauses (->> (unconsumed-queries
                              index-enriched-where-clauses
                              @consumed-where-clauses)
                         (map (fn [pattern]
                                (let [field (let [namespaced-keyword (-> pattern :pattern (nth 1) :value)]
                                              (str
                                                (namespace namespaced-keyword)
                                                "."
                                                (utils/replace-dashes-with-underlines (name namespaced-keyword))))
                                      raw-value (-> pattern :pattern (nth 2) :value)
                                      value (if (= java.lang.String (type raw-value))
                                              (str "'" raw-value "'")
                                              raw-value)]
                                  (str/join " " [field "=" value])))))
         where-part (when (not-empty where-clauses)
                      (str "WHERE " (str/join " AND " where-clauses)))]
     (->> [select-part from-part join-part where-part]
       (remove nil?)
       (str/join " "))))

  ;; This is discouraged to be used as it'll fail for queries needing more information for JOINs
  ([query] (datalog->sql [] query)))


(comment

  (datalog->sql [#:db{:ident :person/likes-films
                      :valueType :db.type/ref
                      :cardinality :db.cardinality/many
                      :references :film/id                  ;; an addition that isn't needed by Datomic but helps us
                      :doc "The films the person likes"}
                 #:db{:ident :person/lives-at
                      :valueType :db.type/ref
                      :cardinality :db.cardinality/many
                      :references :location/id
                      :doc "The locations the person lives at"}
                 #:db{:ident :location/country
                      :valueType :db.type/ref
                      :cardinality :db.cardinality/one
                      :references :country/id
                      :doc "The country a particular location is in"}]
    '[:find ?person-name ?country-name ?title ?year ?genre
      :where
      [?e :film/title ?title]
      [?e :film/release-year ?year]
      [?e :film/genre ?genre]

      [?p :person/name ?person-name]
      [?p :person/likes-films ?e]

      [?p :person/lives-at ?l]
      [?l :location/country ?c]
      [?c :country/name ?country-name]

      [?e :film/release-year 1985]]))

(comment

  (some? #{:foo})
  (not-empty #{:foo})
  (not-empty #{})

  (conj [1 2 3] 4)

  (parser/parse '[:find ?person-name ?title ?year ?genre
                  :where
                  [?e :film/title ?title]
                  [?e :film/release-year ?year]
                  [?e :film/genre ?genre]

                  [?p :person/name ?person-name]
                  [?p :person/likes-films ?e]

                  [?e :film/release-year 1985]]))



(comment
  (map-indexed (fn [idx item] (str idx "_" item))
               ["one" "two" "three"])

  (type "animation")
  (type 1985)

  (contains? #{"one" "two" "three"} "one2")

  (namespace :movie/genre)                                  ;; -> movie
  (name :movie/genre)                                       ;; -> genre
  )
(comment
  (let [parsed-query (parser/parse '[:find ?title ?year ?genre
                                     :where
                                     [?e :movie/title ?title]
                                     [?e :movie/release-year ?year]
                                     [?e :movie/genre ?genre]
                                     [(= ?genre "something")]
                                     [?e :movie/release-year 1985]])
        find-variable (->> (:qfind parsed-query)
                           :elements
                           (map :symbol))]
    parsed-query)

  (datalog->sql '[:find ?title ?genre ?year
                  :where
                  [?e :movie/title ?title]
                  [?e :movie/genre ?genre]
                  [?e :movie/release-year ?year]
                  [?e :movie/release-year 1985]]))

(comment
  #_(q db
       '{:find [p1]
         :where [[p1 :person/name n]
                 [p1 :person/last-name n]
                 [p1 :person/name name]]
         :in [name]}
       "Alice")

  ;; Analysis of the query

  ;; first we'd have to resolve the last clause [p1 :name name] to get `p1`
  ;;

  ;; What would a SQL query look like?
  ;; SELECT id FROM ?? WHERE ??.name = ??.last_name AND ??.name = <the-name-given>;

  ;; Can we identify the table name from the attributes? - definitely, NO (if multiple tables have those attributes)
  ;; Hence if we have a datalog query like the one above we have to figure out which tables are involved and query them all.
  ;; We also need a unique id per entry across the database.
  ;;  -> SQLite might offer something in that respect, otherwise we might be able to use string like
  ;;     `<table_name>_<ìd column value>`

  ;; from https://docs.datomic.com/on-prem/getting-started/query-the-data.html
  '[:find ?title ?year ?genre
    :where
    [?e :movie/title ?title]
    [?e :movie/release-year ?year]
    [?e :movie/genre ?genre]
    [?e :movie/release-year 1985]]

  ;; SELECT title, year, genre FROM movie WHERE movie.release_year = 1985;

;; ----
  ;; Solving it using a Graph Database??

  '{:find [p1]
    :where [[p1 :person/name n]
            [p1 :person/last-name n]
            [p1 :person/name name]]
    :in [name]}

  ;; is a puzzle with the contents of the `where-clause` being the puzzle to solve.
  '[[p1 :person/name n]
    [p1 :person/last-name n]
    [p1 :person/name name]]

  ;; As a graph
  ;; p1 - :person/name -> n
  ;; p1 - :person/last-name -> n
  ;; p1 - :person/name -> name

  ;; so if we provide `name` -> should be able to backtrack to `p1`

  ;; If I'd have to write the query for it manually it'd look like
  ;; SELECT person.id FROM person WHERE person.name = person.last_name AND person.name = 'name';

  ;; -----

  ;; a JOIN example
  '[[p1 :person/name n]
    [p1 :person/likes-films f1]
    [f1 :film/title film-title]
    [f1 :film/genre (contains? "Animation")]]

  ;; how do I decide which table to use as the base for the join? - can I query the database to know which table is bigger?
  ;;
  )
(comment
  (parser/parse '[:find ?title ?year ?genre
                  :where
                  [?e :movie/title ?title]
                  [?e :movie/release-year ?year]
                  [?e :movie/genre ?genre]
                  [?e :movie/release-year 1985]])

  ;#datalog.parser.type.Query{:qfind #datalog.parser.type.FindRel{:elements [#datalog.parser.type.Variable{:symbol ?title}
  ;                                                                          #datalog.parser.type.Variable{:symbol ?year}
  ;                                                                          #datalog.parser.type.Variable{:symbol ?genre}]},
  ;                           :qwith nil,
  ;                           :qin [#datalog.parser.type.BindScalar{:variable #datalog.parser.type.SrcVar{:symbol $}}],
  ;                           :qwhere [#datalog.parser.type.Pattern{:source #datalog.parser.type.DefaultSrc{},
  ;                                                                 :pattern [#datalog.parser.type.Variable{:symbol ?e}
  ;                                                                           #datalog.parser.type.Constant{:value :movie/title}
  ;                                                                           #blueydatalog.parser.type.Variable{:symbol ?title}]}
  ;                                    #datalog.parser.type.Pattern{:source #datalog.parser.type.DefaultSrc{},
  ;                                                                 :pattern [#datalog.parser.type.Variable{:symbol ?e}
  ;                                                                           #datalog.parser.type.Constant{:value :movie/release-year}
  ;                                                                           #datalog.parser.type.Variable{:symbol ?year}]}
  ;                                    #datalog.parser.type.Pattern{:source #datalog.parser.type.DefaultSrc{},
  ;                                                                 :pattern [#datalog.parser.type.Variable{:symbol ?e}
  ;                                                                           #datalog.parser.type.Constant{:value :movie/genre}
  ;                                                                           #datalog.parser.type.Variable{:symbol ?genre}]}
  ;                                    #datalog.parser.type.Pattern{:source #datalog.parser.type.DefaultSrc{},
  ;                                                                 :pattern [#datalog.parser.type.Variable{:symbol ?e}
  ;                                                                           #datalog.parser.type.Constant{:value :movie/release-year}
  ;                                                                           #datalog.parser.type.Constant{:value 1985}]}],
  ;                           :qlimit nil,
  ;                           :qoffset nil,
  ;                           :qreturnmaps nil}

  (let [parsed-query (parser/parse '[:find ?title ?year ?genre
                                     :where
                                     [?e :movie/title ?title]
                                     [?e :movie/release-year ?year]
                                     [?e :movie/genre ?genre]
                                     [?e :movie/release-year 1985]])]
    (->> (:qfind parsed-query)
         :elements
         (map :symbol)))

  ;; find the clauses in the :where block that are referring to the first variable in the :find block.
  (let [parsed-query (parser/parse '[:find ?title ?year ?genre
                                     :where
                                     [?e :movie/title ?title]
                                     [?e :movie/release-year ?year]
                                     [?e :movie/genre ?genre]
                                     [?e :movie/release-year 1985]])
        first-find-variable (->> (:qfind parsed-query)
                                 :elements
                                 (map :symbol)
                                 first)]
    (->> (:qwhere parsed-query)
         (filter (fn [pattern]
                   (= first-find-variable
                      (-> pattern :pattern (nth 2) :symbol)))))))


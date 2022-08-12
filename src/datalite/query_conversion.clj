(ns datalite.query-conversion
  (:require [datalog.parser :as parser]
            [datalite.utils :refer [replace-dashes-with-underlines]]
            [clojure.string :as str]))

(defn- enrich-vec-of-maps-with-index [vector]
  (->> vector
    (map-indexed
      (fn [index map]
        (assoc map :index index)))
    (into [])))

(comment
  (enrich-vec-of-maps-with-index [{:foo "bar"} {:bar "foo"}])

  )

(defn- unconsumed-queries [where-clauses consumed-where-clauses]
  (->> where-clauses
    (remove (fn [clause] (contains? consumed-where-clauses (:index clause))))))

(defn- zero-prefix [number]
  (cond
    (< number 10) (str "00" number)
    (< number 100) (str "0" number)
    :else (str number)))

(defn datalog->sql [query]
  (let [parsed-query (parser/parse query)
        consumed-where-clauses (atom #{})
        select-fields (let [find-symbols (->> (:qfind parsed-query)
                                           :elements
                                           (map :symbol)
                                           vec)
                            ;; todo how to decide which query is the nonarithmetic positive literal in the body if there are multiple matches?
                            correlating-where-clauses (let [where-clauses (enrich-vec-of-maps-with-index (:qwhere parsed-query))]
                                                        (->> find-symbols
                                                          (map (fn [find-symbol]
                                                                 (->> where-clauses
                                                                   (filter (fn [where-clause]
                                                                             (= find-symbol (-> where-clause
                                                                                              :pattern
                                                                                              (nth 2)
                                                                                              :symbol))))
                                                                   first)))))
                            update-consumed-where-clauses! (doseq [clause correlating-where-clauses]
                                                             (swap! consumed-where-clauses conj (:index clause)))]
                        (->> correlating-where-clauses
                          (map (fn [where-clause]
                                 (-> where-clause
                                   :pattern
                                   (nth 1)
                                   :value)))
                          (map-indexed (fn [index attribute]
                                         (str
                                           (namespace attribute)
                                           "."
                                           (replace-dashes-with-underlines (name attribute))
                                           " as "
                                           "field_"
                                           (zero-prefix index))))))
        select-part (str "SELECT " (str/join ", " select-fields))
        from-part (let [tables (->> parsed-query
                                 :qwhere
                                 (filter :pattern)
                                 (map (fn [pattern]
                                        (-> (:pattern pattern)
                                          (nth 1)
                                          :value
                                          namespace)))
                                 distinct)]
                    (str "FROM " (first tables)))
        ;; todo ignoring the 1st level ?e for now
        where-clauses (->> (unconsumed-queries
                             (enrich-vec-of-maps-with-index (:qwhere parsed-query))
                             @consumed-where-clauses)
                        (map (fn [pattern]
                               (let [field (let [namespaced-keyword (-> pattern :pattern (nth 1) :value)]
                                             (str
                                               (namespace namespaced-keyword)
                                               "."
                                               (replace-dashes-with-underlines (name namespaced-keyword))))
                                     raw-value (-> pattern :pattern (nth 2) :value)
                                     value (if (= java.lang.String (type raw-value))
                                             (str "'" raw-value "'")
                                             raw-value)]
                                 (str/join " " [field "=" value])))))
        where-part (when (not-empty where-clauses)
                     (str "WHERE " (str/join " AND " where-clauses)))]
    (->> [select-part from-part where-part]
      (remove nil?)
      (str/join " "))))

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
                  [?e :movie/release-year 1985]])

  )

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
  ;                                                                           #datalog.parser.type.Variable{:symbol ?title}]}
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
                  (-> pattern :pattern (nth 2) :symbol))))))

  )


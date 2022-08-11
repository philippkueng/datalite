(ns datalite.query-conversion-test
  (:require [clojure.test :refer :all]
            [datalite.query-conversion :refer [datalog->sql]]
            [clojure.string :as str]))

(defn- remove-line-breaks-and-trim
  [query]
  (->> (str/split-lines query)
    (map str/trim)
    (str/join " ")))

(comment
  (remove-line-breaks-and-trim "SELECT
          movie.title as movie__title,
          movie.year as movie__year,
          movie.genre as movie__genre
         FROM movie
         WHERE movie.release_year = 1985")

  (let [datalog-query '[:find ?title ?year ?genre
                        :where
                        [?e :movie/title ?title]
                        [?e :movie/release-year ?year]
                        [?e :movie/genre ?genre]
                        [?e :movie/release-year 1985]]
        sql-query (remove-line-breaks-and-trim
                    "SELECT
                      movie.title as movie__title,
                      movie.year as movie__year,
                      movie.genre as movie__genre
                     FROM movie
                     WHERE movie.release_year = 1985")]
    (datalog->sql datalog->sql))
  )

(deftest simple-filter-query-against-a-single-entity-type
  (let [datalog-query '[:find ?title ?year ?genre
                        :where
                        [?e :movie/title ?title]
                        [?e :movie/release-year ?year]
                        [?e :movie/genre ?genre]
                        [?e :movie/release-year 1985]]
        sql-query (remove-line-breaks-and-trim
                    "SELECT
                      movie.title as field_000,
                      movie.release_year as field_001,
                      movie.genre as field_002
                     FROM movie
                     WHERE movie.release_year = 1985")]
    (is (= sql-query (datalog->sql datalog-query)))))

(deftest multi-filter-query-against-a-single-entity-type
  (let [datalog-query '[:find ?title ?year ?genre
                        :where
                        [?e :movie/title ?title]
                        [?e :movie/release-year ?year]
                        [?e :movie/genre ?genre]
                        [?e :movie/release-year 1985]
                        [?e :movie/genre "animation"]]
        sql-query (remove-line-breaks-and-trim
                    "SELECT
                      movie.title as field_000,
                      movie.release_year as field_001,
                      movie.genre as field_002
                     FROM movie
                     WHERE
                      movie.release_year = 1985
                      AND movie.genre = 'animation'")]
    (is (= sql-query (datalog->sql datalog-query)))))

(comment
  (run-tests)
  )

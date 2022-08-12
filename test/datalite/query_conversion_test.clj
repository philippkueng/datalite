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

(deftest simple-query-against-a-single-entity-type-without-a-filter-condition
  (let [datalog-query '[:find ?id ?title ?year ?genre
                        :where
                        [?e :film/title ?title]
                        [?e :film/release-year ?year]
                        [?e :film/genre ?genre]
                        [?e :film/id ?id]]
        sql-query (remove-line-breaks-and-trim
                    "SELECT
                      film.id as field_000,
                      film.title as field_001,
                      film.release_year as field_002,
                      film.genre as field_003
                     FROM film")]
    (is (= (datalog->sql datalog-query) sql-query))))

(deftest simple-filter-query-against-a-single-entity-type
  (let [datalog-query '[:find ?title ?year ?genre
                        :where
                        [?e :film/title ?title]
                        [?e :film/release-year ?year]
                        [?e :film/genre ?genre]
                        [?e :film/release-year 1985]]
        sql-query (remove-line-breaks-and-trim
                    "SELECT
                      film.title as field_000,
                      film.release_year as field_001,
                      film.genre as field_002
                     FROM film
                     WHERE film.release_year = 1985")]
    (is (= sql-query (datalog->sql datalog-query)))))

(deftest multi-filter-query-against-a-single-entity-type
  (let [datalog-query '[:find ?title ?year ?genre
                        :where
                        [?e :film/title ?title]
                        [?e :film/release-year ?year]
                        [?e :film/genre ?genre]
                        [?e :film/release-year 1985]
                        [?e :film/genre "animation"]]
        sql-query (remove-line-breaks-and-trim
                    "SELECT
                      film.title as field_000,
                      film.release_year as field_001,
                      film.genre as field_002
                     FROM film
                     WHERE
                      film.release_year = 1985
                      AND film.genre = 'animation'")]
    (is (= sql-query (datalog->sql datalog-query)))))

(comment
  (run-tests)
  )

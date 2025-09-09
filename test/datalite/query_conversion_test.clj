(ns datalite.query-conversion-test
  (:require [clojure.test :refer :all]
            [datalite.query-conversion :refer [datalog->sql]]
            [datalite.utils :refer [remove-line-breaks-and-trim]])
  (:import [java.time Instant]))

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
                     WHERE movie.release_year = 1985
                     AND movie.valid_from < 1757065259958 AND (movie.valid_to > 1757065259958 OR movie.valid_to IS NULL)")]
    (datalog->sql datalog->sql (Instant/ofEpochMilli 1757065259958))))

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
                     FROM film
                     WHERE film.valid_from < 1757065259958 AND (film.valid_to > 1757065259958 OR film.valid_to IS NULL)")]
    (is (= (datalog->sql datalog-query (Instant/ofEpochMilli 1757065259958)) sql-query))))

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
                     WHERE film.release_year = 1985
                     AND film.valid_from < 1757065259958 AND (film.valid_to > 1757065259958 OR film.valid_to IS NULL)")]
    (is (= sql-query (datalog->sql datalog-query (Instant/ofEpochMilli 1757065259958))))))

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
                      AND film.genre = 'animation'
                      AND film.valid_from < 1757065259958 AND (film.valid_to > 1757065259958 OR film.valid_to IS NULL)")]
    (is (= sql-query (datalog->sql datalog-query (Instant/ofEpochMilli 1757065259958))))))

(deftest returning-values-and-the-entity-id
  (let [datalog-query '[:find ?e ?title ?year ?genre
                        :where
                        [?e :film/title ?title]
                        [?e :film/release-year ?year]
                        [?e :film/genre ?genre]
                        [?e :film/release-year 1985]]
        sql-query (remove-line-breaks-and-trim
                   "SELECT
                        film.id as field_000,
                        film.title as field_001,
                        film.release_year as field_002,
                        film.genre as field_003
                       FROM film
                       WHERE film.release_year = 1985
                       AND film.valid_from < 1757065259958 AND (film.valid_to > 1757065259958 OR film.valid_to IS NULL)")]
    (is (= sql-query (datalog->sql datalog-query (Instant/ofEpochMilli 1757065259958))))))

(deftest filter-query-against-multiple-joined-entities
    (let [schema [#:db{:ident :person/likes-films
                       :valueType :db.type/ref
                       :cardinality :db.cardinality/many
                       :references :film/id                 ;; an addition that isn't needed by Datomic but helps us
                       :doc "The films the person likes"}]
          datalog-query '[:find ?person-name ?title ?year ?genre
                          :where
                          [?e :film/title ?title]
                          [?e :film/release-year ?year]
                          [?e :film/genre ?genre]

                          [?p :person/name ?person-name]
                          [?p :person/likes-films ?e]

                          [?e :film/release-year 1985]]
          sql-query (remove-line-breaks-and-trim
                      "SELECT
                        person.name as field_000,
                        film.title as field_001,
                        film.release_year as field_002,
                        film.genre as field_003
                       FROM film
                       JOIN join_person_likes_films ON join_person_likes_films.film_id = film.id
                       JOIN person ON join_person_likes_films.person_id = person.id
                       WHERE film.release_year = 1985
                       AND film.valid_from < 1757065259958 AND (film.valid_to > 1757065259958 OR film.valid_to IS NULL)
                       AND person.valid_from < 1757065259958 AND (person.valid_to > 1757065259958 OR person.valid_to IS NULL)
                       AND join_person_likes_films.valid_from < 1757065259958 AND (join_person_likes_films.valid_to > 1757065259958 OR join_person_likes_films.valid_to IS NULL)")]
      (is (= sql-query (datalog->sql schema datalog-query (Instant/ofEpochMilli 1757065259958))))))

(deftest filter-query-against-cardinality-one-joined-entities
  (let [schema [#:db{:ident :film/directed-by
                     :valueType :db.type/ref
                     :cardinality :db.cardinality/one
                     :references :person/id                 ;; an addition that isn't needed by Datomic but helps us
                     :doc "The person who directed this film"}]
        datalog-query '[:find ?person-name ?film-title
                        :where
                        [?f :film/title ?film-title]
                        [?f :film/directed-by ?p]
                        [?p :person/name ?person-name]]
        sql-query (remove-line-breaks-and-trim
                    "SELECT
                      person.name as field_000,
                      film.title as field_001
                     FROM film
                     JOIN person ON film.directed_by = person.id
                     WHERE film.valid_from < 1757065259958 AND (film.valid_to > 1757065259958 OR film.valid_to IS NULL)
                     AND person.valid_from < 1757065259958 AND (person.valid_to > 1757065259958 OR person.valid_to IS NULL)")]
    (is (= sql-query (datalog->sql schema datalog-query (Instant/ofEpochMilli 1757065259958))))))

(comment
  (run-tests)
  )

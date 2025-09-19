(ns datalite.protocols.cheshire
  (:require [cheshire.generate :as chegen])
  (:import [java.time Instant LocalDate LocalTime ZoneId Duration]
           [java.time.format DateTimeFormatter]))

(def java-time-instant-sql-str-formatter (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"))
(defn java-time-instant->sql-str [instant]
  (.format java-time-instant-sql-str-formatter
           (.atZone instant (ZoneId/of "UTC"))))

(defn java-time-instant->json-str [instant]
  (.format DateTimeFormatter/ISO_INSTANT instant))

(chegen/add-encoder Instant
                    (fn [c json-generator]
                      (.writeString json-generator (java-time-instant->json-str c))))

;; Encoder for LocalDate
(chegen/add-encoder
  LocalDate
  (fn [ld jg]
    (.writeString jg (.toString ld)))) ; ISO format e.g. "2025-08-06"

;; Encoder for LocalTime
(chegen/add-encoder
  LocalTime
  (fn [lt jg]
    (.writeString jg (.toString lt)))) ; ISO format e.g. "12:00:00"

(chegen/add-encoder
  Duration
  (fn [d jg]
    (.writeString jg (.toString d)))) ; e.g. "PT1H30M"

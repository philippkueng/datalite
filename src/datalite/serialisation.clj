(ns datalite.serialisation
  (:require [cognitect.transit :as transit])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream)
           [java.time LocalDate Instant]))

(def local-date-write-handler
  (transit/write-handler
    "local-date"
    (fn [^LocalDate ld] (.toString ld))
    (fn [^LocalDate ld] (.toString ld))))

(def instant-write-handler
  (transit/write-handler
    "instant"
    (fn [^Instant inst] (.toString inst))
    (fn [^Instant inst] (.toString inst))))

(defn encode
  "Encode data to Transit using the given format (:json or :msgpack). Returns a byte array."
  [data format]
  (let [out (ByteArrayOutputStream. 4096)
        writer (transit/writer out format {:handlers {LocalDate local-date-write-handler
                                                      Instant instant-write-handler}})]
    (transit/write writer data)
    (.toByteArray out)))

(def local-date-read-handler
  (transit/read-handler
    (fn [s] (LocalDate/parse s))))

(def instant-read-handler
  (transit/read-handler
    (fn [s] (Instant/parse s))))

(defn decode
  "Decode Transit data from a byte array using the given format (:json or :msgpack)."
  [bytes format]
  (let [in (ByteArrayInputStream. bytes)
        reader (transit/reader in format {:handlers {"local-date" local-date-read-handler
                                                     "instant" instant-read-handler}})]
    (transit/read reader)))

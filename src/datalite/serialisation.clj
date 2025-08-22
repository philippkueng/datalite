(ns datalite.serialisation
  (:require [cognitect.transit :as transit])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream)))

(defn encode
  "Encode data to Transit using the given format (:json or :msgpack). Returns a byte array."
  [data format]
  (let [out (ByteArrayOutputStream. 4096)
        writer (transit/writer out format)]
    (transit/write writer data)
    (.toByteArray out)))

(defn decode
  "Decode Transit data from a byte array using the given format (:json or :msgpack)."
  [bytes format]
  (let [in (ByteArrayInputStream. bytes)
        reader (transit/reader in format)]
    (transit/read reader)))

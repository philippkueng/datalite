(ns datalite.protocols.postgresql
  (:require [clojure.java.jdbc :as jdbc]
            [cheshire.core :as json]
            [datalite.protocols.cheshire])
  (:import [org.postgresql.util PGobject]
           [clojure.lang IPersistentMap IPersistentVector ISeq Keyword]
           [java.time Instant OffsetDateTime ZoneOffset]))


(extend-protocol jdbc/ISQLValue
  IPersistentMap
  (sql-value [value] (doto (PGobject.) (.setType "jsonb") (.setValue (json/generate-string value))))

  IPersistentVector
  (sql-value [value] (doto (PGobject.) (.setType "jsonb") (.setValue (json/generate-string value))))

  ISeq
  (sql-value [value] (doto (PGobject.) (.setType "jsonb") (.setValue (json/generate-string value)))))

(extend-protocol jdbc/IResultSetReadColumn
  PGobject
  (result-set-read-column [pgobj _ _]
    (let [value (.getValue pgobj)]
      (case (.getType pgobj)
        "jsonb" (json/parse-string value true)
        "json" (json/parse-string value true)
        value))))


(extend-protocol jdbc/ISQLValue
  Keyword
  (sql-value [k] (if (nil? (namespace k))
                   (name k)
                   (format "%s/%s" (namespace k) (name k))))

  Instant
  (sql-value [inst]
    (OffsetDateTime/ofInstant inst ZoneOffset/UTC)))
(ns datalite.protocols.duckdb
  (:require #_[clojure.java.jdbc :as jdbc]
    [next.jdbc :as jdbc]))

;; Needed as DuckDB doesn't return a ByteArray for the BLOB datatype so we'll need to convert it manually.
#_(extend-protocol jdbc/IResultSetReadColumn
  org.duckdb.DuckDBResultSet$DuckDBBlobResult
  (result-set-read-column [val _ _]
    (let [buffer-field (.getDeclaredField (class val) "buffer")]
      (.setAccessible buffer-field true)
      (let [byte-buffer (.get buffer-field val)
            bytes (byte-array (.remaining byte-buffer))]
        (.get byte-buffer bytes)
        bytes))))

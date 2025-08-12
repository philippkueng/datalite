(ns datalite.utils
  (:require [clojure.string :as str]))

(defn replace-dashes-with-underlines
  "As SQLite can't have field names like `release-year` we'll need to convert it."
  [field-name]
  (-> field-name
      (str/replace #"-" "_")))

(defn ordered-table-names
  "Given a collection of table names, returns them in a canonical order (alphabetical)."
  [tables]
  (sort tables))

(defn join-table-name [schema-attribute]
  "eg. :person/likes-films"
  (replace-dashes-with-underlines (format "join_%s_%s"
                                    (namespace schema-attribute)
                                    (name schema-attribute))))

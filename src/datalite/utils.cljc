(ns datalite.utils
  (:require [clojure.string :as str]))

(defn replace-dashes-with-underlines
  "As SQLite can't have field names like `release-year` we'll need to convert it."
  [field-name]
  (-> field-name
      (str/replace #"\-" "_")))
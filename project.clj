(defproject cascalog-workshop "1.0.0"
  :source-path "src/clj"
  :aot :all
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
		             [org.danlarkin/clojure-json "1.1-SNAPSHOT"]
		             [cascalog "1.8.1-SNAPSHOT"]
                 [elephantdb/elephantdb-cascalog "0.0.7"]
                 ]
  :dev-dependencies [
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.2.1"]])

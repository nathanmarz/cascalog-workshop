(defproject cascalog-workshop "1.0.0"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :aot :all
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
		 [org.danlarkin/clojure-json "1.1-SNAPSHOT"]
		 [cascalog "1.7.0-SNAPSHOT"]
                 ]
  :dev-dependencies [
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.2.1"]])

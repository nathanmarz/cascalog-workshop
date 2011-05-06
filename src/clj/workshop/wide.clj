(ns workshop.wide
  (:use [workshop bootstrap])
  (:use [cascalog playground]))

(bootstrap-workshop)

(def wide-source-fields ["!id" "!first" "!last" "!age" "!gender" "!location"])

(def wide-source
     (name-vars
      [[1 "bob" "smith" 27 "m" "philadelphia"]
       [2 "alice" "wonder" 48 "f" nil]
       [3 "bob" "johnson" nil nil nil]
       ]
      wide-source-fields))


(defn first-name-counts []
  (<- [?first ?count]
      ((select-fields wide-source ["!first"]) ?first)
      (c/count ?count)
      ))

(defn first-name-counts2 []
  (<- [?first ?count]
      (wide-source :#> 6 {1 ?first})
      (c/count ?count)
      ))

(defn first-name-counts3 []
  (<- [!first ?count]
      (wide-source :>> wide-source-fields)
      (c/count ?count)
      ))

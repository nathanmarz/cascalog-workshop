(ns workshop.playground-examples
  (:use [workshop bootstrap datastores])
  (:use [cascalog playground]))

(bootstrap-workshop)


;; Cascading-dbmigrate example

;; (def users-db-tap
;;   (DBMigrateTap.
;;     1
;;     "com.mysql.jdbc.Driver"
;;     (. Config BACKTYPE_DB)
;;     (. Config MIGRATOR_USERNAME)
;;     (. Config MIGRATOR_PASSWORD)
;;     "users"
;;     "user_id"
;;     (into-array ["user_id" "account_type"])
;;     ))

;; Basics problems

(defn half-decade-pairs []
  (<- [?person1 ?person2]
      (age ?person1 ?age1)
      (age ?person2 ?age2)
      (- ?age1 5 :> ?age2)
      (:distinct false)
      ))

(defn safe-bucket-age [age]
  (when age
    (int (/ age 10))))

(defn demographics-bucketing []
  (<- [!age-bucket !!gender !!city ?count]
      (age ?person !!age)
      (safe-bucket-age !!age :> !age-bucket)
      (gender ?person !!gender)
      (location ?person _ _ !!city)
      (c/count ?count)
      ))


(defmapcatop split [^String s]
  (seq (.split s " ")))

(defn to-lower [^String s]
  (.toLowerCase s))

(defn word-count []
  (<- [?word ?count]
      (sentence ?sentence)
      ((c/comp #'to-lower split) ?sentence :> ?word)
      (c/count ?count)
      (>= ?count 3)))

(defn followed-and-follows []
  (let [follows-count (<- [?person ?count]
                          (follows ?person _)
                          (c/count ?count))
        followers-count (<- [?person ?count]
                            (follows _ ?person)
                            (c/count ?count))]
    (<- [?person]
        (follows-count ?person ?fcount)
        (>= ?fcount 2)
        (followers-count ?person ?fcount2)
        (>= ?fcount2 2)
        (:distinct false))
    ))

(defn follow-3-years-younger1 []
  (<- [?person1 ?person2]
      (follows ?person1 ?person2)
      (age ?person1 ?age1)
      (age ?person2 ?age2)
      (- ?age1 3 :> ?age2)
      (:distinct false)))

(defn follow-3-years-younger2 []
  (<- [?person1 ?person2]
      (follows ?person1 ?person2)
      (age ?person1 ?age1)
      (age ?person2 ?age2)
      (- ?age1 3 :> ?age1-younger)
      (= ?age1-younger ?age2)
      (:distinct false)))




(defn query-planner-example []
  (with-debug
    (compile-flow
     (stdout)
     (<- [?delta ?count]
         (age ?person1 ?age1)
         (follows ?person1 ?person2)
         (age ?person2 ?age2)
         (* 2 ?age2 :> ?double-age2)
         (< ?double-age2 ?age1)
         (- ?age1 ?age2 :> ?delta)
         (c/count ?count)
         (> ?count ?delta)))))




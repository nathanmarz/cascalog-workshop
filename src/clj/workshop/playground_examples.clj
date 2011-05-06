(ns workshop.playground-examples
  (:use [workshop bootstrap datastores])
  (:use [cascalog playground]))

(bootstrap-workshop)

(defn people-who-follow []
  (<- [?person]
      (follows ?person _)
      ;; (:distinct false)
      ))


;; Cascading-dbmigrate example

;; (def users-db-tap
;;   (DBMigrateTap.
;;     64
;;     "com.mysql.jdbc.Driver"
;;     (. Config BACKTYPE_DB)
;;     (. Config MIGRATOR_USERNAME)
;;     (. Config MIGRATOR_PASSWORD)
;;     "users"
;;     "user_id"
;;     (into-array ["user_id" "account_type"])
;;     ))

;; Basics problems

(defn male-or-old []
  (let [male (<- [?person] (gender ?person "m") (:distinct false))
        old (<- [?person] (age ?person ?age) (> ?age 30) (:distinct false))]
    (union male old)))

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


(defn follow-no-one []
  (<- [?person]
      (person ?person)
      (follows ?person _ :> false)
      (:distinct false)))

(defn no-follow-emily []
  (<- [?person]
      (person ?person)
      (follows ?person "emily" :> false)
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




;; multigroup example

;; 2 reduce steps
(defn person-follow-count []
  (<- [?person ?count]
      (person ?person)
      (follows ?person !!person2)
      (c/!count !!person2 :> ?count)))

(defmultibufferop counter-multi [person follows]
  [[(count follows)]])

(defn person-follow-count-optimized []
  (multigroup [?person] [?count]
              counter-multi
              (name-vars person ["?person"])
              (name-vars follows ["?person" "?follows"])
              ))

;; Problems

(defmultibufferop age-and-follower-count [ages followers]
  (let [age (if (empty? ages)
              nil
              (nth (apply max-key first ages) 2))]
    [[age
      (count followers)]])
  )

;; person's most recent age from dirty-ages and number of followers

(defn age+follower-count []
  (multigroup [?person] [?age ?count]
              age-and-follower-count
              (name-vars dirty-ages ["?ts" "?person" "?age"])
              (name-vars follows ["?person" "?follows"])
              ))


(defn young-sociability []
  (<- [?person ?follows]
      (age ?person ?age)
      (< ?age 30)
      (follows ?person _ :> ?follows)
      (:distinct false)))

(defn male-dirty-age-25 []
  (<- [?person]
      (gender ?person "m")
      (dirty-ages _ ?person 25 :> true)
      (:distinct false)))

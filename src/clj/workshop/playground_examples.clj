(ns workshop.playground-examples
  (:use [workshop bootstrap datastores])
  (:use [cascalog playground]))

(bootstrap-workshop)

(defmapop double-val [val]
  (* 2 val))

(defmapop double-and-triple [val]
  [(* 2 val) (* 3 val)])

(deffilterop big-age1 [val]
  (> val 30))

(defn big-age2 [val]
  (> val 30))

(defn age-filter [afilter]
  (<- [?person ?age]
      (age ?person ?age)
      (afilter ?age)
      (:distinct false)))

(defn big-age1-query []
  (age-filter big-age1))

(defn big-age2-query []
  ;; only works with var
  (age-filter #'big-age2))

;; extract or filter pattern
(defmapcatop extract-full-name [str]
  (let [tokens (seq (.split str " "))]
    (when (= 2 (count tokens))
      tokens
      )))

(defaggregateop [first-n-agg [n]]
  ([] []) ;init function
  ([res val] (if (< (count res) n) (conj res val) res)) ;aggregate function
  ([res] res) ;extract function
  )


(defbufferop [first-n-buf [n]] [tuples]
  (take n tuples)
  )

(defparallelagg xor
  :init-var #'identity
  :combine-var #'bit-xor)

(defn split-xor-init [val]
  (if (odd? val)
    [val 0]
    [0 val]))

(defn split-xor-combine [odd1 even1 odd2 even2]
  [(bit-xor odd1 odd2) (bit-xor even1 even2)])

(defparallelagg split-xor
  :init-var #'split-xor-init
  :combine-var #'split-xor-combine
  )


;; this only works when running in local mode (just talking to in memory "database"
(deffilterop [insert-ages-to-db [url]] {:stateful true}
  ([] (open-db-connection url))
  ([conn person age]
     (.db-put conn person age)
     false)
  ([conn] (.db-close conn))
  )




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




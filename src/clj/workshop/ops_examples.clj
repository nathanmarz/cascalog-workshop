(ns workshop.ops-examples
  (:use [workshop bootstrap datastores])
  (:use [cascalog playground])
  (:require [cascalog [ops-impl :as ops-impl]])
  (:use [clojure.contrib.str-utils :only [str-join]]))

(bootstrap-workshop)

;; examples


(defmapop double-val [val]
  (* 2 val))

(defmapop double-and-triple [val]
  [(* 2 val) (* 3 val)])

(deffilterop multiple-of-7 [val]
  (= 0 (mod val 7)))

(defn double-function [val]
  (* 2 val))

(deffilterop big-age1 [val]
  (> val 30))

(defn big-age2 [val]
  (> val 30))

;; to show difference between defn and *ops
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



(defaggregateop [first-n-agg [n]]
  ([] []) ;init function
  ([res val] (if (< (count res) n) (conj res val) res)) ;aggregate function
  ([res] res) ;extract function
  )


(defbufferop [first-n-buf [n]] [tuples]
  (take n tuples)
  )




;; (?<- (stdout) [?num] (integer ?i) (duplicate ?i :> ?num) (:distinct false))

;; Aggregator examples

(defbufferop str-append-buffer [tuples]
  [(str-join "," (map (comp str first) tuples))]
  )

(defaggregateop str-append-agg
  ([] nil)
  ([curr val] (if curr (str curr "," val) val))
  ([curr] [curr])
  )

(defn str-append-combine [val1 val2]
  (str val1 "," val2))

(defparallelagg str-append-pagg
  :init-var #'str
  :combine-var #'str-append-combine)

;; str-append example
(defn all-followers [op]
  (<- [?person ?list]
      (follows ?follower ?person)
      (op ?follower :> ?list)))

;; Parameterized example


(deffilterop [multiple-of [n]] [val]
  (= 0 (mod val n)))

 
;; controlling memory usage

(defbufferiterop [chunked-average [chunk-size]] [tuples-iter]
  (for [chunk (partition chunk-size
                         (map first (iterator-seq tuples-iter)))]
    (/  (double (reduce + chunk)) (count chunk))
    ))

(defn location-fc-averages [root chunk-size]
  (let [location (location-data root)
        fc (followers-count-data root)]
    (<- [?location ?point]
        (location ?person ?location)
        (fc ?person ?fc)
        (:sort ?fc)
        (chunked-average [chunk-size] ?fc :> ?point)
        )))

;; stateful example:

;; this only works when running in local mode (just talking to in memory "database"
(deffilterop [insert-ages-to-db [url]] {:stateful true}
  ([] (open-db-connection url))
  ([conn person age]
     (.db-put conn person age)
     false)
  ([conn] (.db-close conn))
  )

;; (?<- (stdout) [?person ?age] (age ?person ?age) (insert-ages-to-db ["a"] ?person ?age))

;; (?<- (stdout) [?person ?age] (age ?person ?age) (inc ?age :> ?age2) (insert-ages-to-db ["a"] ?person ?age2))





;; Problems:

;; extract or filter pattern
(defmapcatop extract-full-name [str]
  (let [tokens (seq (.split str " "))]
    (when (= 2 (count tokens))
      [tokens]
      )))



(defmapcatop duplicate [val]
  (repeat val val))

;; (?<- (stdout) [?val] (integer ?i) (duplicate ?i :> ?val) (:distinct false))

(defmapcatop [duplicate-n [n]] [val]
  (repeat n val))

;; (?<- (stdout) [?val] (integer ?i) (duplicate-n [5] ?i :> ?val) (:distinct false))

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


(defn keep-max-val [amap]
  (if (empty? amap)
    amap
    (into {}
          [(first (sort-by #(* -1 (second %)) amap))])))

(defaggregateop most-frequent-val
  ([] {})
  ([state val] 
     (let [curr-val (state val)]
       (if curr-val
         (update-in state [val] inc)
         (assoc
             (keep-max-val state)
           val 1
           ))))
  ([state] [((comp first first) (keep-max-val state))]))


;; (?<- (stdout) [?person ?gender] (gender-fuzzy ?person ?g _) (most-frequent-val ?g :> ?gender) (:sort ?g))


(defn choose-recent-val [val1 time1 val2 time2]
  (if (> time2 time1)
    [val2 time2]
    [val1 time1])
  )

(defparallelagg most-recent-val
  :init-var #'ops-impl/identity-tuple
  :combine-var  #'choose-recent-val)

;; (?<- (stdout) [?person ?gender] (gender-fuzzy ?person ?g ?t) (most-recent-val ?g ?t :> ?gender _))


;;alternate implementation of most-recent-val

;; (?<- (stdout) [?person ?gender] (gender-fuzzy ?person ?g ?t) (:sort ?t) (:reverse true) (c/limit [1] ?g :> ?gender))

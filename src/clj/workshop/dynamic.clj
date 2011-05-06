(ns workshop.dynamic
  (:use [cascalog playground])
  (:use [workshop bootstrap datastores]))

(bootstrap-workshop)

(defn emily-male-follows []
  (let [person-var "?person"]
    (<- [person-var]
        (follows "emily" person-var)
        (gender person-var "m"))))

(defn emily-male-follows2 []
  (let [out-vars (vec (v/gen-nullable-vars 1))]
    (<- out-vars
        (follows :>> (cons "emily" out-vars))
        (gender :>> (conj out-vars "m")))
    ))


;; problems

(defn global-sort [sq fields]
  (let [out-fields (get-out-fields sq)
        new-out-fields (v/gen-nullable-vars (count out-fields))]
    (<- new-out-fields
        (sq :>> out-fields)
        (:sort :<< fields)
        ((IdentityBuffer.) :<< out-fields :>> new-out-fields))))

(defn chained-pairs-simple [pairs chain-length]
  {:pre [(>= chain-length 2)]}
  (let [out-vars (v/gen-nullable-vars chain-length)
        var-pairs (partition 2 1 out-vars)]
    (construct out-vars
               (concat
                (for [var-pair var-pairs]
                  [pairs :>> var-pair])
                [[:distinct false]]))
    ))


(defn attach-chains [chain1 chain2]
  (let [out1 (get-out-fields chain1)
        out2-chained (v/gen-nullable-vars (dec (count (get-out-fields chain2))))
        out-vars (concat out1 out2-chained)
        out2 (cons (last out1) out2-chained)]
    (<- out-vars
        (chain1 :>> out1)
        (chain2 :>> out2)
        (:distinct false))
    ))

(defn chained-pairs-smart [pairs chain-length]
  {:pre [(>= chain-length 2)]}
  (let [iterations (int (log2 (dec chain-length)))
        chains (reductions
                (fn [chain _]
                  (attach-chains chain chain))
                pairs
                (range iterations))
        binary (reverse (binary-rep (dec chain-length)))
        chains-to-use (compact
                       (map (fn [chain bit]
                              (if (= bit 1) chain))
                            chains
                            binary))]
    (reduce attach-chains chains-to-use)
    ))



(def read-repair
     (<- [!val !timestamp :> !out-val]
         (:sort !timestamp) (:reverse true)
         (c/limit [1] !val :> !out-val)))

(defn select* [gen]
  (let [outfields (get-out-fields gen)]
    (predmacro [invars outvars]
      {:pre [(= 0 (count (concat invars outvars)))]}
      [[gen :>> outfields]]
      )))

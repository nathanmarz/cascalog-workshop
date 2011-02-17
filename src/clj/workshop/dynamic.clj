(ns workshop.dynamic
  (:use [workshop bootstrap datastores]))

(bootstrap-workshop)

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

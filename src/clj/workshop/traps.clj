(ns workshop.traps
  (:use [workshop bootstrap datastores]))

(bootstrap-workshop)

(defn bad-filter [val]
  (if (= 14675 val )
    (throw (RuntimeException.)))
  (>= val 5000))

(defn trapped-followers-count [root trap-dir]
  (<- [?id ?fc]
      ((followers-count-data root) ?id ?fc)
      (bad-filter ?fc)
      (:distinct false)
      (:trap (hfs-textline trap-dir))
      ))

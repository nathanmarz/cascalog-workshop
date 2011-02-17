(ns workshop.ops-examples
  (:use [workshop bootstrap datastores]))

(bootstrap-workshop)


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

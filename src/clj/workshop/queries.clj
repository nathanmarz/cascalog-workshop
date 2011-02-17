(ns workshop.queries
  (:use [workshop bootstrap datastores])
  (:use [clojure.contrib.seq-utils :only [find-first]]))

(bootstrap-workshop)


(def dist-pairs (partition 2 1 [0 10 100 1000 10000 nil]))

(defn- bucketize [count]
  (first
   (find-first (fn [[low up]]
                 (or (not up)
                     (and
                      (<= low count)
                      (< count up))))
               dist-pairs)))

(defn follower-count-distribution [root]
  (let [fc (followers-count-data root)]
    (<- [?bucket ?count]
        (fc _ ?fc)
        (bucketize ?fc :> ?bucket)
        (c/count ?count))
    ))

(defn dist-init [count]
  (doall
    (map
      (fn [[l u]]
        (if (and (>= count l)
                 (or (not u) (< count u)))
            1
            0))
      dist-pairs)))

(defn dist-combine [& vals]
  (let [[v1 v2] (partition (count dist-pairs) vals)]
    (doall (map + v1 v2))))

(defparallelagg mk-distribution :init-var #'dist-init
                                :combine-var #'dist-combine)


(defn follower-count-distribution2 [root]
  (let [fc (followers-count-data root)
        out-vars (v/gen-nullable-vars (count dist-pairs))]
    (<- out-vars
        (fc _ ?fc)
        (mk-distribution ?fc :>> out-vars))
    ))

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

(defmapcatop split [^String s]
  (seq (.split s " ")))

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

(defn total-retweets [root]
  (let [reaction (reaction-data root)
        reactor (reactor-data root)]
    (<- [?reactor ?total]
        (reaction _ ?to)
        (reactor ?reactor ?to)
        (c/count ?total))
    ))

(defn comparitively-influential [root]
  (let [total-retweets (total-retweets root)
        fc (followers-count-data root)]
    (<- [?bucket ?influencer ?rank]
        (fc ?person ?fc)
        (total-retweets ?person ?count)
        (bucketize ?fc :> ?bucket)
        (:sort ?count) (:reverse true)
        (c/limit-rank [10] ?person :> ?influencer ?rank))))

(defn many-tweets-people [root n]
  (let [reactor (reactor-data root)]
    (<- [?person ?amt]
        (reactor ?person _)
        (c/count ?amt)
        (>= ?amt n)
        )))

(defn location-tweets [root]
  (let [location (location-data root)
        reactor (reactor-data root)]
    (<- [?location ?amt]
        (location ?person ?location)
        (reactor ?person _)
        (c/count ?amt))
    ))

(defn top-locations [root]
  (c/first-n (location-tweets root) 100 :sort "?amt" :reverse true))

(defn to-lower [^String s]
  (.toLowerCase s))

(def stop-words #{"and" "the" "a" "of" "in" "i" "to" "for" "&" "my" "is" "on" "with" "at" "i'm" "de"
"-" "you" "from" "am" "all" "me" "about" "an" "it" "be" "that" "by" "who"
"not" "are" "your" "en" "la" "as" "we" "/" "but" "do" "this" "y" "one" "," "what" "et"
"e" "or" "also" "so" "our" "don't" "." "@" "!" "..."})

(defn valid-word? [word]
  (and (> (count word) 2)
       (not (contains? stop-words word))))

(defn description-word-count [root]
  (let [description (description-data root)]
    (<- [?word ?count]
        (description _ ?description)
        ((c/comp split #'to-lower) ?description :> ?word)
        (valid-word? ?word)
        (c/count ?count)
        )))

(defn description-tag-cloud [root]
  (c/first-n (description-word-count root) 100 :sort "?count" :reverse true))



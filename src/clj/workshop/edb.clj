(ns workshop.edb
  (:use [workshop bootstrap datastores queries])
  (:use [cascalog.playground])
  )

(bootstrap-workshop)

(defn ser-long [num]
  (BytesWritable.
   (.getBytes
    (json/encode-to-str {:val num})
    )))

(defn total-retweets-edb! [root out-dir]
  (?- (elephant-tap out-dir
                    {:num-shards 4
                     :persistence-factory (JavaBerkDB.)}
                    {:updater nil})
      (<- [?key ?val]
          ((total-retweets root) ?key ?num)
          (ser-long ?num :> ?val)
          (:distinct false)
          )))

(defn print-retweets [out-dir person-id]
  (with-single-service-handler [handler {"test" out-dir}]
    (json/decode-from-str
     (String. (-> handler
                  (.getLong "test" person-id)
                  .get_data)))
    ))

(defn append-combine [^bytes ser1 ^bytes ser2]
  (let [l1 (json/decode-from-str (String. ser1))
        l2 (json/decode-from-str (String. ser2))]
    (.getBytes (json/encode-to-str (concat l1 l2)))
    ))

(defn append-updater [^LocalPersistence lp ^bytes k ^bytes v]
  (let [old (.get lp k)
        newv (if-not old
               v
               (append-combine old v)
               )]
    (.add lp k newv)
    ))

(defbufferop to-json-list [tuples]
  [(BytesWritable. (.getBytes (json/encode-to-str (map first tuples))))])

(defn followers-list []
  (<- [?person ?list]
      (follows ?follower ?person)
      (to-json-list ?follower :> ?list)))

(defn followers-tap [out-dir]
  (elephant-tap out-dir
                {:num-shards 4
                 :persistence-factory (JavaBerkDB.)}
                {:updater (mk-clj-updater #'append-updater)
                 :deserializer (string-deserializer)}))

(defn followers-list-edb! [out-dir]
  (?- (followers-tap out-dir)
      (followers-list)
      ))

(defn followers-list-gary-edb! [out-dir]
  (?- (followers-tap out-dir)
      (<- [?person ?list]
          ((followers-list) ?person ?list)
          (= ?person "gary")
          (:distinct false))
      ))

(defn print-followers [out-dir person]
  (with-single-service-handler [handler {"test" out-dir}]
    (json/decode-from-str
     (String. (-> handler
                  (.getString "test" person)
                  .get_data)))
    ))



;; Print out contents of an edb store on DFS:
;; (?- (stdout) (name-vars (followers-tap "/tmp/edb1") ["?k" "?v"]))

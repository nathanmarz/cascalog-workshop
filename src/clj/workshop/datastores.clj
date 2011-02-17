(ns workshop.datastores
  (:use [workshop bootstrap]))

(bootstrap-workshop)

(defn- parse-str [^String s]
  (seq (.split s "\t")))


(defn longify-all [& strs]
  (map #(Long/parseLong %) strs)
  )

(defn- tab-parsed-longs [root name num-fields]
  (let [tap (hfs-textline (str root "/" name))
        int-vars (v/gen-nullable-vars num-fields)
        out-vars (v/gen-nullable-vars num-fields)]
    (<- out-vars
        (tap !line)
        (parse-str !line :>> int-vars)
        (longify-all :<< int-vars :>> out-vars)
        (:distinct false))
    ))

(defn- decode-property [^String json]
  (let [m (json/decode-from-str json)]
    [(:id m) (:val m)]))

(defn- property-data [root name]
  (let [tap (hfs-textline (str root "/" name))]
    (<- [!id !val]
        (tap !line)
        (decode-property !line :> !id-num !val)
        (long !id-num :> !id)
        (:distinct false))
    ))

(defn reaction-data [root]
  (tab-parsed-longs root "reaction" 2))

(defn reactor-data [root]
  (tab-parsed-longs root "reactor" 2))

(defn description-data [root]
  (property-data root "description"))

(defn name-data [root]
  (property-data root "name"))

(defn following-count-data [root]
  (property-data root "following"))

(defn followers-count-data [root]
  (property-data root "followers"))

(defn statuses-count-data [root]
  (property-data root "statuses"))

(defn location-data [root]
  (property-data root "location"))








(ns workshop.util
  )

(defn log2 [val]
  (/ (double (Math/log val)) (Math/log 2)))

(defn compact [aseq]
  (filter (complement nil?) aseq))

(defn binary-rep
  "
2 -> [1 0]
3 -> [1 1]
4 -> [1 0 0]
5 -> [1 0 1]
"
  [anum]
  (for [char (Integer/toBinaryString anum)]
    (if (= char \1) 1 0)
    ))

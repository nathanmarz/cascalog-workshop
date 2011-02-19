(ns workshop.bootstrap)

(defmacro bootstrap-workshop []
  '(do
     (require (quote [cascalog [ops :as c] [vars :as v]]))
     (use (quote [cascalog api]))
     (import (quote org.apache.hadoop.io.BytesWritable))
     (require (quote [org.danlarkin [json :as json]]))
     (import (quote cascalog.ops.IdentityBuffer))
     (use (quote [workshop util]))
     (use (quote [elephantdb.cascalog core]))
     (use (quote [elephantdb.testing :only [with-single-service-handler]]))
     (import (quote [elephantdb.persistence JavaBerkDB LocalPersistence]))
     (import (quote org.apache.hadoop.io.BytesWritable))
     ))

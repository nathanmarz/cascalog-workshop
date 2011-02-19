(import (quote [java.io PrintStream]))
(import (quote [cascalog WriterOutputStream]))
(import (quote [org.apache.log4j Logger WriterAppender SimpleLayout]))
(.addAppender (Logger/getRootLogger) (WriterAppender. (SimpleLayout.) *out*))
(System/setOut (PrintStream. (WriterOutputStream. *out*)))
(alter-var-root
 #'cascalog.rules/*JOB-CONF*
 (fn [_] {"io.sort.mb" 1 }))

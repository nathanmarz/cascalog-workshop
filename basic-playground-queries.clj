(?<- (stdout) [?person]
         (age ?person ?age) (< ?age 30))

(?<- (stdout) [?person ?age]
         (age ?person ?age) (< ?age 30))

(?<- (stdout) [?n]
         (integer ?n) (* ?n ?n :> ?n))

(?<- (stdout) [?n]
         (integer ?n) (* ?n ?n ?n :> ?n))

(?<- (stdout) [?person1 ?person2]
         (age ?person1 ?age1)
         (follows ?person1 ?person2)
         (age ?person2 ?age2)
         (< ?age2 ?age1))

(?<- (stdout) [?count]
         (age _ ?age) (< ?age 30) (c/count ?count))

(?<- (stdout) [?person ?count]
         (follows ?person _) (c/count ?count))

(defmapcatop split [sentence]
         (seq (.split sentence "\\s+")))

(?<- (stdout) [?word ?count]
         (sentence ?sentence)
         (split ?sentence :> ?word)
         (c/count ?count))

(let [many-follows (<- [?person]
                           (follows ?person _)
                           (c/count ?count)
                           (> ?count 2))]
                     (?<- (stdout) [?person1 ?person2]
                        (many-follows ?person1)
                        (many-follows ?person2)
                        (follows ?person1 ?person2)))

(?<- (stdout) [?person ?age ?gender]
         (age ?person ?age) (gender ?person ?gender))

;;   vs.

(?<- (stdout) [?person !!age !!gender]
         (age ?person !!age) (gender ?person !!gender))

                

                

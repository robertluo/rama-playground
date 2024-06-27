(ns robertluo.analytics.core
  "A simple analytics library for gaming data"
  (:require
   [cheshire.core :as json]
   [schema.core :as s]
   [schema.coerce :as cc]
   [schema.utils :as su]
   [clojure.java.io :as io]
   [com.rpl.rama.path :as p])
  (:import
   [clojure.lang Keyword]))

(s/defschema GameScore
  {:timestamp            Long           ;;Time for the recording
   :username             String         ;;username of the player
   :game                 Keyword        ;;game name
   :server               Keyword        ;;server name
   :score-at             Long           ;;score at the time
   :game-id              String         ;;game id
   :display              Keyword        ;;Display mode
   :platform             Keyword        ;;Platform where game played
   :money                Long           ;;Money change delta
   :tax                  Long           ;;Tax charged
   :event                Keyword        ;;Event name
   :score                Long           ;;Score change delta
   })

(def json->GameScore
  (cc/coercer (assoc GameScore Keyword s/Any) cc/json-coercion-matcher))

(defn samples 
  [filename]
  (let [data (-> (io/reader filename)
                 (json/parse-stream true))]
    (->> data
         (p/transform [p/ALL] json->GameScore))))

^:rct/test
(comment
  (json->GameScore {:timestamp 100 :score-at 100 :server "foo" :game "bar" :username "foo" :game-id "bar" :display "2D" :platform "PC" :money 100 :event "login" :score 100})
  (count (p/select [p/ALL su/error?] (samples "sample-data/game-score.json"))) ;=> 0
  )

;;## Agragating data

(defn score-stat
  [game-scores]
  (->
   (->> [:score :money :tax]
        (map #(->> (p/select [p/ALL %] game-scores)
                   (transduce identity + 0)))
        (zipmap [:score :money :tax]))
   (assoc :cnt (count game-scores))))

^:rct/test
(comment
  (score-stat [{:score-at 100 :server "foo" :game "bar" :username "user1" :money 100 :score 1 :tax 10}
               {:score-at 100 :server "foo" :game "bar" :username "user2" :money 1000 :score 2 :tax 50}])
  ;=>> {:cnt 2 :score 3 :money 1100 :tax 60}
  )

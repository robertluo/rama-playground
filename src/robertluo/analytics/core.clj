(ns robertluo.analytics.core
  "A simple analytics library for gaming data"
  (:require
   [cheshire.core :as json]
   [schema.core :as s]
   [schema.coerce :as cc]
   [schema.utils :as su]
   [clojure.java.io :as io]
   [com.rpl.rama.path :as p]
   [robertluo.analytics.util :as util])
  (:import
   [clojure.lang Keyword]))

;;## Input data format

(s/defschema GameScore
  "A schema for game score data"
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
  "A json->GameScore coercion function"
  (cc/coercer (assoc GameScore Keyword s/Any) cc/json-coercion-matcher))

(defn- samples
  "Reads the game score data from the given `filename`"
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

;;## Aggregating data

(defrecord
 ScoreStat
 [score-at-to ;To when the score-at ends
  score       ;total score
  money       ;total money
  tax         ;total tax
  cnt         ;count of records
  user-sketch ;sketch of distinct users
  ])

;TODO t-digest for score distribution
;TODO set of distinct games

(defn score-stat
  "returns a stat map for the given `game-scores`"
  [game-scores]
  (->
   (->> [:score :money :tax]
        (map #(->> (p/select [p/ALL %] game-scores)
                   (transduce identity + 0)))
        (zipmap [:score :money :tax]))
   (assoc :cnt (count game-scores)
          :user-sketch (->> (p/select [p/ALL :username] game-scores)
                            (util/sketch))
          :score-at-to (->> game-scores
                            (p/select [p/ALL :score-at])
                            (apply max)))))

(defn game-stat-merge
  "Merges two score stats into one"
  [stat1 stat2]
  (->ScoreStat
   (max (:score-at-to stat1) (:score-at-to stat2))
   (+ (:score stat1) (:score stat2))
   (+ (:money stat1) (:money stat2))
   (+ (:tax stat1) (:tax stat2))
   (+ (:cnt stat1) (:cnt stat2))
   (util/union (:user-sketch stat1) (:user-sketch stat2))))

^:rct/test
(comment
  ;testing score-stat
  (score-stat [{:score-at 10  :server "foo" :game "bar" :username "user1" :money 100 :score 1 :tax 10}
               {:score-at 100 :server "foo" :game "bar" :username "user2" :money 1000 :score 2 :tax 50}])
  ;=>>
  {:score-at-to 100 :cnt 2 :score 3 :money 1100 :tax 60 :user-sketch #(= 2 (count %))}

  ;testing merge
  (game-stat-merge (score-stat [{:score-at 100 :server "foo" :game "bar" :username "user1" :money 100 :score 1 :tax 10}])
                   (score-stat [{:score-at 100 :server "foo" :game "bar" :username "user2" :money 1000 :score 2 :tax 50}]))
  ;=>>
  {:score 3, :money 1100, :tax 60, :cnt 2, :user-sketch #(= 2 (count %))}
  )

;;## Merge/segment by time
;;For a batch of game scores, we make it a score stat, however, when we merge them, we need to segment them by time (score-at)

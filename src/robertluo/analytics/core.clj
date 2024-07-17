(ns robertluo.analytics.core
  "A simple analytics library for gaming data"
  (:require
   [cheshire.core :as json]
   [schema.core :as s]
   [schema.coerce :as cc]
   [schema.utils :as su]
   [clojure.java.io :as io]
   [com.rpl.rama.path :as p]
   [robertluo.ull :as ull])
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

(defn json-str->GameScore
  "Converts a json string to a GameScore record"
  [json-str]
  (-> json-str
      (json/parse-string keyword)
      json->GameScore))

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
;; core data structure for score statistics
(defrecord
 ScoreStat
 [score-at     ;;latest score-at
  score        ;;total score
  money        ;;total money
  tax          ;;total tax
  cnt          ;;count of records
  user-counter ;;sketch of distinct users
  ])
;TODO t-digest for score distribution
;TODO set of distinct games

(defn nil-stat []
  (->ScoreStat 0 0 0 0 0 (ull/create-ull)))

(defn single-stat [game-score]
  (->ScoreStat
   (:score-at game-score)
   (:score game-score)
   (:money game-score)
   (:tax game-score)
   1
   (-> (ull/create-ull) (ull/add-string (:username game-score)))))

(defn game-stat-merge
  "Merges two score stats into one"
  [stat1 stat2]
  (->ScoreStat
   (max (:score-at stat1) (:score-at stat2))
   (+ (:score stat1) (:score stat2))
   (+ (:money stat1) (:money stat2))
   (+ (:tax stat1) (:tax stat2))
   (+ (:cnt stat1) (:cnt stat2))
   (ull/union (:user-counter stat1) (:user-counter stat2))))

^:rct/test
(comment
  (nil-stat)
  ;=>>
  {:user-counter #(zero? (ull/estimate-count %))}
  
  ;testing merge
  (game-stat-merge 
   (single-stat {:score-at 100 :score 100 :money 100 :tax 10 :username "foo"})
   (single-stat {:score-at 200 :score 200 :money 200 :tax 20 :username "bar"}))
  ;=>>
  {:score-at 200, :score 300 :money 300, :tax 30, :cnt 2, :user-counter #(= 2 (ull/estimate-count %))}
  )

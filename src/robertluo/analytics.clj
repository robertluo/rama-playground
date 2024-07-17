(ns robertluo.analytics
  "Game score basic analytics"
  (:require
   [robertluo.analytics.core :as core]
   [robertluo.analytics.util :as util]
   [com.rpl.rama :as r]
   [com.rpl.rama.path :as p])
  (:import 
   [clojure.lang Keyword]
   [robertluo.analytics.core ScoreStat]))

(r/deframaop 
  ^{:doc "Segment time into different granularity"}
  granularity
  [*timemillis]
  (util/segment-time *timemillis :> {:keys [*minute *hour *day *month]})
  (:> :minute *minute)
  (:> :hour *hour)
  (:> :day *day)
  (:> :month *month))

(def +stat-combiner
  "A combiner to merge score stats"
  (r/combiner
   core/game-stat-merge
   :init-fn core/nil-stat))

(def GameStatsSchema
  "Game Stats Schema"
  {Keyword        ;;game
   {Keyword       ;;server
    {Keyword      ;;platform
     {Keyword     ;;granurity
      (r/map-schema
       Long       ;;score-at
       ScoreStat
       {:subindex? true})}}}})

(r/defmodule 
  ^{:doc "Game score module"}
  GameScoreModule
  [setup topologies]
  ;because the original data is just a json string, we just use random partitioner
  (r/declare-depot setup *game-scores :random)
  
  ;Microbatch topology seems fit for analytics
  (let [topo (r/microbatch-topology topologies "game-score")]
    (r/declare-pstate topo $$game-stats GameStatsSchema)
    
    ;ETL stats from json string
    (r/<<sources 
     topo
     (r/source> *game-scores :> %microbatch)
     (%microbatch :> *gsj)
     (core/json-str->GameScore *gsj :> {:keys [*game *server *platform *score-at] :as *gs})
     (r/|hash *game) ;;partition by game
     (core/single-stat *gs :> *stat)
     (granularity *score-at :> *gradunarity *bucket)
     (r/+compound $$game-stats
                  {*game
                   {*server
                    {*platform
                     {*gradunarity
                      {*bucket (+stat-combiner *stat)}}}}}))
    (r/<<query-topology 
     topologies 
     "total-score"
     [:> *res]
     (r/|hash)
     (r/local-select> [(p/keypath :PDK) p/MAP-VALS p/MAP-VALS p/MAP-VALS :month p/MAP-VALS]
                      $$game-stats
                      :> *stat)
     (r/|origin)
     (+stat-combiner *stat :> *res))))

(comment
  (require '[com.rpl.rama.test :as rtest]
           '[clojure.java.io :as io])
  (def lines (->> "sample-data/game-score.txt"
                  io/reader
                  line-seq
                  (take 100)))
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc GameScoreModule {:tasks 4 :threads 2})
    (let [mn (r/get-module-name GameScoreModule)
          depot (r/foreign-depot ipc mn "*game-scores")
          ps (r/foreign-pstate ipc mn "$$game-stats")
          q (r/foreign-query ipc mn "total-score")]
      (doseq [line lines]
        (r/foreign-append! depot line))
      (r/foreign-invoke-query q)
      (r/foreign-select [p/MAP-VALS p/MAP-VALS p/MAP-VALS :month] ps {:pkey :PDK})))
  (def sample {:PDK
               {:超级高手
                {:windows
                 {:minute
                  {100000 (core/nil-stat)
                   200000 (core/nil-stat)}}}}})
  (p/select [p/MAP-VALS p/MAP-VALS p/MAP-VALS :minute] sample)
  )

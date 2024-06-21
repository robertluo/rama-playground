(ns robertluo.timeseries
  "Time series data structures and operations"
  (:use [com.rpl.rama]))

(defrecord RenderLatency [url render timestamp])

(defrecord WindowStat [cardinality total last min max])

(defn single-window
  [render]
  (->WindowStat 1 render render render render))

(defn combine-window
  "Combine two window stats into one."
  [stat1 stat2]
  (letfn [(max [x y] (if (or (nil? x) (nil? y)) (or x y) (Math/max x y)))
          (min [x y] (if (or (nil? x) (nil? y)) (or x y) (Math/min x y)))]
    (->WindowStat
     (+ (:cardinality stat1) (:cardinality stat2))
     (+ (:total stat1) (:total stat2))
     (or (:last stat2) (:last stat1))
     (min (:min stat1) (:min stat2))
     (max (:max stat1) (:max stat2)))))

^:rct/test
(comment
  (combine-window (->WindowStat 1 1 1 1 nil) (->WindowStat 2 2 nil 2 2)) ;=>
  #robertluo.timeseries.WindowStat{:cardinality 3, :total 3, :last 1, :min 1, :max 2}
  )

(def +combine-measurements
  "Combine measurements into a window stat."
  (combiner combine-window :init-fn (fn [] (->WindowStat 0 0 nil nil nil))))

(deframaop indexes 
  [*timestamp]
  (long (/ *timestamp (* 1000 60)) :> *minute)
  (long (/ *minute 60) :> *hour)
  (long (/ *hour 24) :> *day)
  (long (/ *day 30) :> *month))

(comment
  (indexes {:timestamp 1614556800000}) ;=>
  {:minute 26909280, :hour 448488, :day 18687, :month 622}
  )
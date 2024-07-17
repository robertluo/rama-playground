(ns robertluo.analytics.util
  "Utilities for statistics and analytics"
  (:require
   [tick.core :as t]))

;;## Time segmenting

(defn segment-time
  "Segment a given instant into minute, hour, day and month.
    - `timemillis`: The timemillis to be segmented.
    - option `:zone`: The time zone can be specified with the `zone` option, default to +08."
  [timemillis & {:keys [zone-hour-offset]
                 :or   {zone-hour-offset 8}}]
  (let [zoned   (-> timemillis (t/instant) (t/offset-by (t/zone-offset zone-hour-offset)))
        minute  (-> zoned (t/with :second-of-minute 0))
        hour    (-> minute (t/with :minute-of-hour 0))
        day     (-> hour (t/with :hour-of-day 0))
        month   (-> day (t/with :day-of-month 1))
        to-long (fn [zdt] (-> zdt (t/instant) (t/long)))]
    {:minute (to-long minute)
     :hour   (to-long hour)
     :day    (to-long day)
     :month  (to-long month)}))

^:rct/test
(comment
  (t/long (t/now))
  (segment-time 1720147251) ;=>
  {:minute 1720140, :hour 1717200, :day 1699200, :month -28800})
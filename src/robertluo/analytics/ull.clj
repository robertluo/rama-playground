(ns robertluo.analytics.ull
  "UltraLogLog sketching implementation in Clojure.
   This implementation is based on the Java library hash4j.

   Basic usage:
   (-> (sketch [\"foo\" \"bar\" \"baz\" \"bar\"])
       count) ;=> 3
   (-> (sketch [\"foo\" \"bar\"])
       (union (sketch [\"baz\" \"bar\"]))
       count) ;=> 3
  "
  (:require
   [taoensso.nippy :as nippy])
  (:import
   [com.dynatrace.hash4j.hashing Hashing]
   [com.dynatrace.hash4j.distinctcount UltraLogLog]))

(defprotocol Sketchable
  "A protocol for sketching data"
  (-sketch [this words])
  (-merge [this other]))

;;A SketchULL is a wrapper over a mutable UltraLogLog Object, with its hasher built in.
;;Thisstructure that can be used to estimate the number of distinct elements in a set
;;using the UltraLogLog algorithm. 
;;(https://javadoc.io/doc/com.dynatrace.hash4j/hash4j/latest/com/dynatrace/hash4j/distinctcount/UltraLogLog.html)
(deftype SketchULL [^Hashing hasher ^UltraLogLog ull]
  Sketchable
  (-sketch
    [this words]
    (doseq [word words] 
      (.add ull (.hashCharsToLong hasher word)))
    this)
  (-merge
    [_ other]
    (SketchULL. hasher (UltraLogLog/merge ull (.ull other))))
  clojure.lang.Counted
  (count
    [_]
    (Math/round (.getDistinctCountEstimate ull))))

(defn sketch
  "Create a new Sketch with the given words, you can then use `count` function
   to get the estimated number of distinct elements in the set." 
  ([words]
   (-> (SketchULL. (Hashing/komihash5_0) (UltraLogLog/create 12))
       (-sketch words))))

(defn union
  "Merge two sketches into one."
  [sketch1 sketch2]
  (-merge sketch1 sketch2))

^:rct/test
(comment
  ;basic usage
  (-> (sketch ["foo" "bar" "baz" "bar"])
      count) ;=> 3
  ;union
  (-> (sketch ["foo" "bar" "baz" "secret"])
      (union (sketch ["baz" "bar"]))
      (count)) ;=> 4
  )
  

;;Nippy serialization
;;
#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-freeze
 SketchULL ::sketch-ull
 [^SketchULL x ^java.io.DataOutput data-output]
 (let [state (.getState (.ull x))]
   (.writeInt data-output (alength state))
   (.write data-output state)))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-thaw 
 ::sketch-ull
 [^java.io.DataInput data-input]
 (let [len (.readInt data-input)]
   (if (pos? len)
     (let [state (byte-array len)]
       (.readFully data-input state)
       (SketchULL. (Hashing/komihash5_0)
                   (UltraLogLog/wrap state)))
     (throw (ex-info "Invalid state length" {:len len})))))

^:rct/test
(comment
  ;roundtrip freeze/thaw
  (-> (nippy/freeze (sketch ["foo" "bar" "baz" "bar"]))
      (nippy/thaw)
      count) ;=> 3
  )
  
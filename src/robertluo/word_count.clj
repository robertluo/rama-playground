(ns robertluo.word-count
  (:require
   [com.rpl.rama :as r :refer [deframaop ?<- defmodule]]
   [com.rpl.rama.aggs :as aggs]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.path :as p]
   [clojure.string :as str]))

;;# Word Count Example
;;
;;## Introduction
;;This example demonstrates how to count the frequency of each word in a sentence Using Rama.
;;
;;## Let's go!
;;
;;In order to use common clojure code in Rama ETL, we need to define a Rama 
;;operator.
;;
;;What it actually does is to using input variable *s, then emit(:>) to a stream.

(deframaop
  ^{:doc "A rama operator to normalize a sentence *s to a sequence of words."}
  normalize 
  [*s]
  (:> (->> (str/split *s #" ") (map str/lower-case) (ops/explode))))
  
;; Introduce rama test library
(require '[com.rpl.rama.test :as rtest])
  
;; I am surprised that rama seems does not provide a stream to sequence function
;; to make tests easily. So I have to write one.  
(defn- collect-all [s]
  (let [cs (atom [])]
    (?<- (normalize s :> *word)
         (swap! cs conj *word))
    @cs))
  
(collect-all "Hello world from Robert")
  
;;## Rama Module
;;A Rama Module is a collection of Rama topologies, depots, pstates, and queries.
;;By convention, the name is in PascalCase, and ends with `Module`.
(defmodule 
  ^{:doc "A module to count the frequency of each word in a sentence."}
  WordCountModule [setup topologies]
  
  ;;declare a depot named *sentences
  (r/declare-depot setup *sentences (r/hash-by identity))
  
  ;;make topologies for this depot
  (let [topo (r/stream-topology topologies "word-count")]

    ;A pstate is a persistent state that can be shared among topologies.
    ;Declare a pstate named $$word-count has a schema of String(word)->Long(count) 
    (r/declare-pstate topo $$word-count {String Long})
    
    ;A ETL topology, it defines a series of operations to transform data.
    (r/<<sources
     topo
     ;We defines that depot *sentences goes into variable *data
     (r/source> *sentences :> *data)
     ;Then we use predifined operator normalize to split the sentence into words
     (normalize *data :> *word)
     ;rama assign tasks by hash of word
     (r/|hash *word)
     ;Aggregation on $$word-count, simply use aggs/+count to count the word
     (r/+compound $$word-count {*word (aggs/+count)}))

    ;;declare a simple query named count-of
    (r/<<query-topology
     topologies "count-of"
     [*word :> *res]
     (r/|hash *word)
     (r/local-select> (p/keypath *word) $$word-count :> *res)
     (r/|origin))))

;;## Use the module
;;In real world, the module is used in another program, hence it has
;;`foreign` in the name, meaning it is outside the defining module.
(defn- send-select [sentences word]
  ;create a InProcessCluster(a.k.a IPC) to run the module
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc WordCountModule {:tasks 4 :threads 2})
    
    ;refer to the depot for appending sentences
    ;refer to the query for counting the frequency of a word 
    (let [mn (r/get-module-name WordCountModule)
          depot (r/foreign-depot ipc mn "*sentences")
          count-of (r/foreign-query ipc mn "count-of")]
      (doseq [sentence sentences]
        (r/foreign-append! depot sentence))
      (r/foreign-invoke-query count-of word))))

;;Now we can use the module to count the frequency of each word in a sentence.
(send-select ["Hello world" "hello from Robert" "heLLo from Nam"] "hello")

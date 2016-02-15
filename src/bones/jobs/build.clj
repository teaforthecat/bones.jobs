(ns bones.jobs.build )



(defn sym-to-topic [^clojure.lang.Keyword job-sym]
  (-> (str job-sym)
      (clojure.string/replace "/" "..") ;; / is illegal in kafka topic name
      (subs 1))) ;; remove leading colon

(defn topic-to-sym [^String topic-name]
  (-> topic-name
      (clojure.string/replace ".." "/") ;; puts the / back
      (keyword))) ;; put the colon back

(defn topic-name-input [^clojure.lang.Keyword job-sym]
  (str (sym-to-topic job-sym) "-input"))

(defn topic-name-output [^clojure.lang.Keyword job-sym]
  (str (sym-to-topic job-sym) "-output"))

(defn general-output [^clojure.lang.Keyword job-sym]
  (str (namespace job-sym ) "-output"))

(defn input-lifecycle [task-name]
  {:lifecycle/task task-name
   :lifecycle/calls :onyx.plugin.kafka/read-messages-calls})

(defn output-lifecycle [task-name]
  {:lifecycle/task task-name
   :lifecycle/calls :onyx.plugin.kafka/write-messages-calls})

(defn topic-reader [fn-keyword]
  "builds a catalog entry that reads from a kafka topic for a particular user function"
  (let [onyx-name (topic-name-input fn-keyword)]
    {:onyx/name (keyword onyx-name)
     :onyx/plugin :onyx.plugin.kafka/read-messages
     :onyx/batch-size 1
     :onyx/min-peers 1
     :onyx/max-peers 1 ;; one reader is all that is required
     :onyx/type :input
     :onyx/medium :kafka
     :kafka/group-id "onyx" ;;?
     :kafka/topic onyx-name
     :kafka/partition "0" ;; wtf n-partitions is 0 TODO: file bug
     :kafka/offset-reset :largest ;; ?
     :kafka/zookeeper "127.0.0.1:2181" ;; can be updated in conf
     :kafka/deserializer-fn :bones.jobs.serializer/deserialize  ;; required
     })) ;; can be updated in conf


(defn topic-writer [^String fn-keyword]
  "builds a catalog entry that writes to a kafka topic"
  (let [onyx-name (topic-name-output fn-keyword)]
    {:onyx/name (keyword onyx-name)
     :onyx/plugin :onyx.plugin.kafka/write-messages
     :onyx/batch-size 1
     :onyx/min-peers 1 ;;?
     :onyx/max-peers 1 ;;?
     :onyx/type :output
     :onyx/medium :kafka
     :kafka/group-id "onyx" ;;?
     :kafka/topic onyx-name
     :kafka/partition "0" ;; wtf n-partitions is 0 TODO: file bug
     :kafka/offset-reset :largest
     :kafka/zookeeper "127.0.0.1:2181" ;; can be updated in conf
     :kafka/serializer-fn :bones.jobs.serializer/serialize ;; required
     })) ;; can be updated in conf

(defn fn-task [^clojure.lang.Keyword ns-fn]
  "builds a catalog entry that performs some user function"
  {:onyx/name ns-fn
   :onyx/fn ns-fn
   :onyx/batch-size 1
   :onyx/max-peers 1
   :onyx/type :function})

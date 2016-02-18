(ns bones.jobs.system
  (:require [com.stuartsierra.component :as component]
            [schema.core :as s]
            [taoensso.timbre :as log]
            [onyx.api]
            [onyx.plugin.kafka] ;;must be in classpath
            ;; for boot-local-sytem (testing) only, no extra dependencies added
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.kafka.embedded-server :as ke]
            [clj-kafka.admin :as ka]
            ;; end boot-local-sytem
            ;; producer
            [clj-kafka.zk :as zk :refer [broker-list brokers]]
            [clj-kafka.new.producer :as nkp]
            ;; end producer
            [bones.jobs.serializer :as serializer]
            [bones.conf :as conf]))

(defprotocol Produce
  (produce [cmp topic key data]))

(defrecord Producer [conf]
  component/Lifecycle
  (start [cmp]
    (if (:producer cmp)
      (do
        (log/info "producer already started")
        cmp)
      (let [conf (:conf cmp)
            kafka-brokers (brokers {"zookeeper.connect" (:zookeeper-addr conf)})
            bootstrap-servers (broker-list kafka-brokers)
            data-format (or (:bones.kafka/data-format conf) serializer/default-data-format)
            serializer (serializer/transit-encoder data-format)]
        (-> cmp
            (assoc :serializer serializer)
            (assoc :producer (nkp/producer {"bootstrap.servers" bootstrap-servers}
                                           (nkp/byte-array-serializer)
                                           (nkp/byte-array-serializer)))))))
  (stop [cmp]
    (if-let [producer (:producer cmp)]
      (.close producer))
    (dissoc cmp :producer))
  Produce
  (produce [cmp topic key data]
    (if (:producer cmp)
      (let [bytes ((:serializer cmp) data)
            key-bytes (.getBytes (str key)) ;; this is all necessary. I know.
            record (nkp/record topic key-bytes bytes)]
        (nkp/send (:producer cmp) record))
      (do
        (log/info "no producer")
        cmp))))

(defrecord OnyxPeers [n-peers onyx-peer-group conf]
  component/Lifecycle
  (start [cmp]
    (if (:peers cmp)
      (do
        (log/info "Onyx Peers already started")
        cmp)
      (do
        (log/info "Starting Onyx Peers")
        (let [npeers (or (:onyx.peer/n-peers conf) n-peers 3)]
          (assoc cmp
                 :peers
                 (onyx.api/start-peers npeers (:peer-group onyx-peer-group)))))))
  (stop [cmp]
    (if-let [pg (:peers cmp)]
      (do
        (log/info "Stopping Onyx Peers")
        (doseq [v-peer (:peers cmp)]
          (try
            ;; jobs should have already stopped so this should be quick
            (onyx.api/shutdown-peer v-peer)
            (catch InterruptedException e
              (log/warn "Peer not shutting down: " (.getMessage e)))))
        (dissoc cmp :peers))
      (do
        (log/info "Onyx peers is not running")
        cmp))))

(defrecord OnyxPeerGroup [conf]
  component/Lifecycle
  (start [cmp]
    (if (:peer-group cmp)
      (do
        (log/info "Onyx Peer Group already started")
        cmp)
      (do
        (log/info "Starting Onyx Peer Group")
        ;; assuming zookeeper is already started, or running elsewhere
        (let [pconf (assoc conf :zookeeper/server? false)]
          (assoc cmp
                 :peer-group
                 ;; note: validates with onyx.schema/PeerConfig
                 (onyx.api/start-peer-group pconf))))))
  (stop [cmp]
    (if-let [pg (:peer-group cmp)]
      (try
        (log/info "Stopping Onyx Peer Group")
        (onyx.api/shutdown-peer-group pg)
        (dissoc cmp :peer-group)
        (catch InterruptedException e
          (log/warn (str "Peer Group not shutting down:" (.getMessage e)))))
      (do
        (log/info "Onyx Peer Group is not running")
        cmp))))

(s/defschema ZkConf
  {(s/optional-key :zookeeper/server?) s/Bool
   :zookeeper/address s/Str
   :onyx/id s/Str
   s/Any s/Any} )

(defrecord ZK [conf]
  component/Lifecycle
  (start [cmp]
    (s/validate ZkConf conf)
    (if (:zookeeper cmp)
      (do
        (log/info "ZooKeeper is already running")
        cmp)
      (assoc cmp :zookeeper (.start (zookeeper conf)))))
  (stop [cmp]
    (if (:zookeeper cmp)
      (do
        ;; todo: somehow wait for zookeeper to actually stop
        ;; we might have to go to the process level here to be sure
        (.stop (:zookeeper cmp))
        (dissoc cmp :zookeeper))
      (do
        (log/info "ZooKeeper is not running")
        cmp))))

(s/defschema KafkaConf
  {:kafka/hostname s/Str
   :kafka/port (s/cond-pre s/Str s/Int)
   :kafka/broker-id (s/cond-pre s/Str s/Int)
   :zookeeper-addr s/Str
   (s/optional-key :kafka/num-partitions) (s/cond-pre s/Str s/Int) ;; can't be zero
   (s/optional-key :kafka/log-dir ) s/Str
   s/Any s/Any})

(defrecord Kafka [conf]
  component/Lifecycle
  (start [cmp]
    (s/validate KafkaConf conf)
    (if (:kafka cmp)
      (do
        (log/info "Kafka is already running")
        cmp)
      (let [{:keys [:kafka/hostname
                    :kafka/port
                    :kafka/broker-id
                    :kafka/log-dir
                    :kafka/num-partitions
                    :zookeeper-addr]} conf
            kconf {:hostname hostname
                   :port port
                   :broker-id broker-id
                   :zookeeper-addr zookeeper-addr
                   :log-dir log-dir
                   :num-partitions num-partitions}]
        (assoc cmp :kafka (component/start (ke/map->EmbeddedKafka kconf))))))
  (stop [cmp]
    (if (:kafka cmp)
      (do
        (log/info "Stopping Kafka")
        (component/stop (:kafka cmp))
        (dissoc cmp :kafka))
      (do
        (log/info "Kafka is not running")
        cmp))))

(defrecord Job [job conf job-id task-ids]
  component/Lifecycle
  (start [cmp]
    (if (:job-id cmp)
      (do
        (log/info "Job is already running")
        cmp)
      (do
        ;; create topics if they don't exist
        (let [topics (remove nil? (map :kafka/topic (:catalog job)))
              zk (ka/zk-client (:zookeeper-addr conf))]
          (doseq [topic topics]
            (if-not (ka/topic-exists? zk topic)
                    (do
                      (ka/create-topic zk topic)
                      (log/info "created topic: " topic)))))
        ;; assume zookeeper is already started
        (let [peer-conf (assoc conf :zookeeper/server? false)
              ;; do it finally
              result (onyx.api/submit-job peer-conf job)]
          (if (:job-id result)
            (log/info "submitted job-id: " (:job-id result)))
          (-> cmp
              (assoc :job-id (:job-id result))
              (assoc :task-ids (:task-ids result)))))))
  (stop [cmp]
    (if (:job-id cmp)
      (do
        (log/info "stopping Job")
        (let [{:keys [job-id conf]} cmp]
          (onyx.api/kill-job conf job-id)
          (if (onyx.api/await-job-completion conf job-id)
            (log/info "job completed successfully")
            (log/info "job killed"))
          (dissoc cmp :job)))
      (do
        (log/info "job not running")
        cmp))))

(defprotocol JobsProtocol
  (start [cmp job])
  (stop  [cmp & some-jobs]))

(defrecord Jobs [onyx-peers conf]
  JobsProtocol
  (start [cmp job]
    (update cmp :jobs conj (component/start (map->Job {:job job :conf (:conf cmp)}))))
  (stop [cmp & some-jobs]
    (if (some map? some-jobs)
      ;; some jobs
      (map component/stop some-jobs)
      ;; or all jobs
      (map component/stop (:jobs cmp)))))

(defn system [config]
  (atom (component/system-map
         :conf (conf/map->Conf (assoc config
                                      :sticky-keys (keys config)
                                      :mappy-keys [[:zookeeper-addr :zookeeper/address]]))
         ;; for testing only
         :zookeeper (component/using
                     (map->ZK {})
                     [:conf])

         ;; for testing only
         :kafka (component/using
                 (map->Kafka {})
                 [:zookeeper :conf])

         :onyx-peer-group (component/using
                           (map->OnyxPeerGroup {})
                           [:kafka :conf])

         :onyx-peers (component/using
                      (map->OnyxPeers {})
                      [:onyx-peer-group :conf])

         :jobs (component/using
                (map->Jobs {})
                [:onyx-peers :conf])

         ;; this should be started separately
         :producer (component/using
                    (map->Producer {})
                    [:conf]))))

(defn submit-job
  "adds job to jobs list under the :jobs component"
  [sys job]
  (if (= {} @sys) (throw (ex-info "system not yet booted" {})))
  (swap! sys update :jobs start job))

(defn stop-jobs [sys]
  (swap! sys update :jobs stop))

(defn start-system [system & components]
  (swap! system component/update-system components component/start))

(defn stop-system [system & components]
  (swap! system component/update-system-reverse components component/stop))

(defn new-component [system component & dependencies]
  (get (component/update-system system (conj dependencies component) component/start) component))

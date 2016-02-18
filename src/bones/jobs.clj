(ns bones.jobs
  (:require [onyx.api :as onyx]
            [onyx.plugin.kafka] ;;must be in classpath
            [bones.jobs.serializer] ;;must be in classpath
            [schema.core :as s]
            [bones.jobs.build :as build]
            [bones.jobs.system :as system]))

;; the alpha and omega
;; the keeper of the system
(def sys (atom {}))

(def simple-peer-conf
  "to be overriden in conf files, used to start boot-local-system"
  {:onyx.messaging/impl :aeron
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging.aeron/allow-short-circuit? false ;; to test the whole system
   :onyx/id "onyx" ;;override
   :zookeeper/address "localhost:2181"
   :zookeeper.server/port 2181
   :zookeeper/server? true ;; for development
   :kafka/hostname "localhost"
   :kafka/port 9092
   :kafka/broker-id 0
   :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
   :onyx.messaging/bind-addr "0.0.0.0"
   :onyx.messaging/peer-port 40200

   ;; unfortunately,
   ;; this number is tied to the number of commands * 3 (for reader+function+writer)
   :onyx.peer/n-peers 12 ;; 4 commands max here
   })


(defn command-job [fn-keyword]
  (let [reader (build/topic-reader fn-keyword)
        fn-task (build/fn-task fn-keyword)
        writer (build/topic-writer fn-keyword)
        input-lc (build/input-lifecycle (:onyx/name reader))
        ;; output-lc (build/input-lifecycle (:onyx/name writer))]
        ;; the above error took 2 hours of my life. can you find it?
        output-lc (build/output-lifecycle (:onyx/name writer))]
    ;; this job is never done, balanced allows multiple things to happen at once
    {:task-scheduler :onyx.task-scheduler/balanced
     :workflow [[(:onyx/name reader) fn-keyword]
                [fn-keyword (:onyx/name writer)]]
     :catalog [reader fn-task writer]
     :lifecycles [input-lc output-lc]}))

(defn submit-job [job]
  (system/submit-job sys job))

(defn stop-jobs []
  (system/stop-jobs sys))

(defn list-jobs []
  (let [jobs (get-in @sys [:jobs :jobs])]
    (map :job-id jobs)))

(s/defschema ConfFiles
  "A list of config files"
  [(s/one s/Str "conf file") s/Str])

(defn create-system
  "sets the stage with all the components
   bones.jobs/sys is further acted upon by the *boot functions below"
  [conf-files]
  ;; flatten incase the user wants to build a list of files
  (let [files (flatten conf-files)]
    (s/validate ConfFiles files)
    (if (= {} @sys)
      (reset! sys @(system/system (assoc simple-peer-conf :conf-files files)))
      (throw (ex-info "system already exists, use the *boot functions below" {:system-map (keys @sys)})))))

(defn boot-local-system
  "for use in the repl. starts all components
  access the system at bones.jobs/sys"
  [conf-file & conf-files]
  (create-system (conj conf-files conf-file))
  (system/start-system sys :conf :zookeeper :kafka :onyx-peer-group :onyx-peers :jobs))

(defn boot-onyx-system
  "for use in a main function. starts only onyx components, assumes kafka is running elsewhere
   access the system at bones.jobs/sys"
  [conf-file & conf-files]
  (create-system (conj conf-files conf-file))
  (system/start-system sys :conf :onyx-peer-group :onyx-peers :jobs))

(defn unboot-system
  "stops the system, all components"
  []
  (if (= {} @sys) (throw (ex-info "system not yet created, use boot-local-system or boot-onyx-system" {})))
  (system/stop-system sys :conf :zookeeper :kafka :onyx-peer-group :onyx-peers :jobs))

(defn reboot-system
  "reboots the system"
  []
  (if (= {} @sys) (throw (ex-info "system not yet created, use boot-local-system or boot-onyx-system" {})))
  (system/stop-system sys :conf :onyx-peer-group :onyx-peers)
  (system/start-system sys :conf :onyx-peer-group :onyx-peers))

(defn new-producer
  "creates new kafka producer
  (let [p (new-producer)]
    (.produce p topic key data))"
  []
  (system/new-component @sys :producer :conf))

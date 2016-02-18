(ns bones.jobs-test
  (:require [clojure.test :refer :all]
            [bones.jobs :as jobs]
            [onyx.static.validation :as validator]
            [onyx.static.planning :as planning]
            [matcha :as m]))

(defn dummy-x [segment]
  {:another "segment"})

(defn validate-job [job]
  ;; taken from the onyx/api.clj
  ;; (validator/validate-peer-config peer-config)
  (validator/validate-job (assoc job :workflow (:workflow job)))
  (validator/validate-flow-conditions (:flow-conditions job) (:workflow job))
  (validator/validate-lifecycles (:lifecycles job) (:catalog job))
  (validator/validate-windows (:windows job) (:catalog job))
  (validator/validate-triggers (:triggers job) (:windows job)))


(defn discover-tasks [job]
  (planning/discover-tasks (:catalog job) (:workflow job)))

(deftest submit-job
  (testing "submits one job that"
    (let [fake-job (jobs/command-job ::who)
          result (validate-job fake-job)
          tasks (discover-tasks fake-job)]
      (testing "passes validation"
        (m/is (m/= nil) result))
      (testing "each task gets an id"
        (m/is (m/every? (m/has-entry-that :id (m/instance? java.util.UUID))) tasks)))))

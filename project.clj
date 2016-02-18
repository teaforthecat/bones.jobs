(defproject bones.jobs "0.1.0-SNAPSHOT"
  :description "Onyx and Onyx-Kafka helpers"
  :url "http://github.com/teaforthecat/bones.jobs"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.onyxplatform/onyx-kafka "0.8.10.0"]
                 [bones.conf "0.1.2"]
                 [com.cognitect/transit-clj "0.8.285"]
                 ]


  :profiles {:test
             {:dependencies [[matcha "0.1.0"]
                             ;; conf's test dependencies are my test dependencies?????????????????
                             ;; not sure what is happening here
                             ;; [clj-yaml "0.4.0"]
                             ;; [clojurewerkz/propertied "1.2.0"]
                             ]}
             :jvm-args ["-Daeron.dir.delete.on.exit"]
             }

  )

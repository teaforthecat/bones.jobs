# bones-jobs

This is an experimental project.

A Clojure library designed to ease the setup and configuration of an
[onyx](http://www.onyxplatform.org) and
[kafka](http://kafka.apache.org) based application.

A component system-map is provided so that all the required components
can be started with one function.

Also, some (one currently; command-job) build functions are provided to
make it as simple as possible to get your functions into the system
and executing.


The command-job will build a topic->function->topic sandwich. The
topics will have "-input" and "-output" append respectively.

The configuration file need not actually exist (you'll get a
warning). It is a required argument though, in order to enforce the
idea that this is meant to be the single entry point to starting the
application. Eventually, this could be used in production with this
form: `(boot-system "production.edn")` in a "main" function. Support for
that will be in a future release.


## Usage

From the repl:
```
(require '[bones.jobs])
(bones.jobs/boot-local-system "test.edn")
(bones.jobs/submit-job (bones.jobs/command-job :some.path-to/a-function))

(with-open [p (bones.jobs/new-producer)]
  (.produce p "some.path-to..a-function-input" key data))

;;not included
(require '[manifold.stream :as ms])
(let [conf (:conf @bones.jobs/sys)]
  (with-open [c (zkc/consumer {"zookeeper.connect" (:zookeeper-addr conf)
                             "group.id" "test"
                             "auto.offset.reset" "smallest"})
              source (-> c
                        (zkc/messages "some.path-to..a-function-output")
                        (ms/->source))]
    (ms/consume println source))

```




## License

Copyright © 2016 Chris Thompson

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

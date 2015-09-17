(ns r-messagebus.core
  "A simple easy to use messaging framework built on top of RabbitMQ that 
   supports many well known enterprise integration patterns."
  (:gen-class)
  (:require [taoensso.timbre :as timbre :refer (tracef debugf infof warnf errorf)]
            [clojure.data.json :as json]
            [r-messagebus.utils :as utils]
            [r-messagebus.consumer :as consumer]
            [r-messagebus.producer :as producer]
            [clojure.core.async :as async :refer [<! <!! chan go thread go-loop]]))

(declare bus-config)

(def default-config {:autostart-consuming true :persistant-store-connection-string "mongodb://localhost" :queue nil
                     :persistant-store-database "RMessageBusPersistantStore" :queue-mappings {} :handlers {} :message-types [] 
                     :befor-consuming-filters [] :after-consuming-filters [] :outgoing-filters [] :machine-name (.getHostName (java.net.InetAddress/getLocalHost))
                     :exception-callback nil :threads 1 :transport-settings {:host "localhost" :max-retries 3 :retry-delay 3000 :username "guest" 
                                                                             :password "guest" :error-queue "errors" :audit-queue "audit" 
                                                                             :auditing-enabled false :heartbeat-queue "heartbeat" :durable true
                                                                             :exclusive false :auto-delete false :queue-arguments nil :errors-disabled false
                                                                             :heartbeat-enabled true :heartbeat-time 120 :purge-on-startup false
                                                                             :ssl-enabled false :ssl-context nil :virtual-host "/" :port 5672
                                                                             :authentication-mechanism "PLAIN"}})
(defn message->bytes 
  [message]
  (byte-array (map byte (json/write-str message))))

(defn start-heartbeat 
  []
  (go-loop [i 0]
    (<! (async/timeout 30000))
    (consumer/send-message (:heartbeat-queue bus-config) 
                           "R.MessageBus.Core.HeartbeatMessage"
                           (message->bytes {:Timestamp (java.util.Date.) :Location (:machine-name bus-config) 
                                            :Name (get-in [:transport-settings :queue] bus-config)
                                            :LatestCpu 0 :LatestMemory 0 :Language "Clojure"
                                            :ConsumerType "RabbitMQ"}))
    (recur (inc i))))

(defn start-aggregators
  []
  (debugf "core/start-aggregators todo"))
  
(def started-consuming (atom false))

(defn event-handler
  [message headers]
  (debugf "todo"))

(defn start-consuming
  []
  (when (not @started-consuming)
    (start-aggregators)
    (consumer/start-consuming bus-config event-handler)
    (reset! started-consuming true)))

(defn start-bus 
  [config]
  (def bus-config (utils/deep-merge default-config config))
  (if (= (get-in [:transport-settings :auditing-enabled] bus-config) true)
    (start-heartbeat))
  (if (= (:autostart-consuming bus-config) true)
    (start-consuming)))

(defn -main
  [& args]
  (start-bus {:queue "clojure.test" :message-types [ "log-command" ] :transport-settings {:host "localhost"}}))

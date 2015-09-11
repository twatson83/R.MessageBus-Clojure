(ns r-messagebus.core
  "A simple easy to use messaging framework built on top of RabbitMQ that 
   supports many well known enterprise integration patterns."
  (:require [taoensso.timbre :as timbre :refer (tracef debugf infof warnf errorf)]
            [clojure.data.json :as json]
            [r-messagebus.consumer :as consumer]
            [r-messagebus.producer :as producer]))

(defmulti message-handlers :type)

(defmethod message-handlers "messages.info"
  [{:keys [type message headers context]}]
  (debugf "t %s, m %s, h %s"))

(defn message->bytes 
  [message]
  (byte-array (map byte (json/write-str message)))))

(defn start-heartbeat 
  []
  (go-loop [i 0]
    (<! (async/timeout 30000))
    (consumer/send-message (:heartbeat-queue config) 
                           "R.MessageBus.Core.HeartbeatMessage"
                           (message->bytes {:Timestamp "" :Location "" :Name (:queue-name config)
                                            :LatestCpu 0 :LatestMemory 0 :Language "Clojure"
                                            :ConsumerType "RabbitMQ"})
    (recur (inc i))))

(defn start-aggregators
  []
  (debugf "core/start-aggregators todo"))
  
(def started-consuming (atom) false)

(defn start-consuming
  []
  (when (not @started-consuming)
    (start-aggregators)
    (consumer/start-consuming config event-handler)
    (reset! start-consuming true)))

(defn create-bus 
  [config]
  (def bus-config config)
  (if (contains? :auditing-enabled)
    (start-heartbeat))
  (if (contains? :autostart-consuming)
    (start-consuming)))

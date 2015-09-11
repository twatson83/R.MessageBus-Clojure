(ns r-messagebus.consumer
  (:import java.util.concurrent.Executors)
  (:require [taoensso.timbre :as timbre :refer (tracef debugf infof warnf errorf)]
            [langohr.core :as rabbitmq
            [langohr.channel :as channel]))

(defn send-message
  [queue type message]
  (debugf "consumer/send-message todo"))

(defn start-consuming
  [config event-handler]
  (def consumer-config (conj (:consumer config) (Executors/newFixedThreadPool (:threads config))))
  (def handler event-handler)
  (def connection (rabbitmq/connect consumer-config))
  (def channel (channel/open connection)))

(defn close
  []
  (rabbitmq/close connection)
  (rabbitmq/close channel))

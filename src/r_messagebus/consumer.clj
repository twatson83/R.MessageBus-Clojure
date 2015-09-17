(ns r-messagebus.consumer
  (:import java.util.concurrent.Executors)
  (:require [taoensso.timbre :as timbre :refer (tracef debugf infof warnf errorf)]
            [clojure.data.json :as json]
            [langohr.core :as rabbitmq]
            [langohr.channel :as channel]
            [langohr.queue :as queue]
            [langohr.exchange :as exchange]
            [langohr.consumers :as consumer]))

(declare bus-config transport-settings handlers connection channel)

(defn send-message
  [queue type message]
  (debugf "consumer/send-message todo"))

(defn configure-retry-queue 
  []
  (let [dead-letter-exchange-name (str (:queue bus-config) ".Retries.DeadLetter")
        retry-queue-name (str (:queue bus-config) ".Retries")] 
    (queue/declare channel retry-queue-name
                   {:exclusive false :auto-delete false :durable (:durable transport-settings) 
                    :arguments {"x-dead-letter-exchange" dead-letter-exchange-name "x-message-ttl" (:retry-delay transport-settings)}})
    (exchange/declare channel dead-letter-exchange-name "direct" {:durable (:durable transport-settings) :auto-delete (:auto-delete transport-settings)})
    (queue/bind channel (:queue bus-config) dead-letter-exchange-name retry-queue-name)))

(defn consume-message-types
  []
  (doall 
   (for [type (:message-types bus-config)]
     (do 
       (exchange/declare channel type "fanout" {:durable true :auto-delete false})
       (queue/bind channel (:queue bus-config) type)))))

(defn configure-error-queue 
  []
  (exchange/declare channel (:error-queue transport-settings) "direct")
  (queue/declare channel (:error-queue transport-settings) {:exclusive false :auto-delete false :durable true})
  (queue/bind channel (:error-queue transport-settings) (:error-queue transport-settings)))

(defn configure-audit-queue
  []
  (exchange/declare channel (:audit-queue transport-settings) "direct")
  (queue/declare channel (:audit-queue transport-settings) {:exclusive false :auto-delete false :durable true})
  (queue/bind channel (:audit-queue transport-settings) (:audit-queue transport-settings)))

(defn consume-message
  [ch meta ^bytes payload]
  (let [message (json/read-str (String. payload "UTF-8"))]
    (debugf "message %s meta %s" message meta)))

(defn start-consuming
  [config event-handler]
  (def bus-config config)
  (def transport-settings (:transport-settings config))
  (def handler event-handler)
  (def connection (rabbitmq/connect {:host (:host transport-settings) :executor (Executors/newFixedThreadPool (:threads config)) :username (:username transport-settings)
                     :password (:password transport-settings) :port (:port transport-settings) :vhost (:virtual-host transport-settings) 
                     :requested-heartbeat (:heartbeat-time transport-settings) :ssl (:ssl-enabled transport-settings)
                     :ssl-context (:ssl-context transport-settings) :authentication-mechanism (:authentication-mechanism transport-settings)}))
  (def channel (channel/open connection))
  (def queue (queue/declare channel (:queue bus-config) {:exclusive (:exclusive transport-settings) :auto-delete (:auto-delete transport-settings)
                                                         :durable (:durable transport-settings) :arguments (:queue-arguments transport-settings)}))
  (configure-retry-queue)
  (configure-error-queue)
  (if (:auditing-enabled transport-settings)
    (configure-audit-queue))
  (if (:purge-on-startup transport-settings)
    (queue/purge channel (:queue bus-config)))
  (consumer/subscribe channel (:queue bus-config) consume-message {:auto-ack true})
  (consume-message-types))

(defn close
  []
  (rabbitmq/close connection)
  (rabbitmq/close channel))

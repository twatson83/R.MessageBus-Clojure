(defproject r-messagebus "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.novemberain/langohr "3.3.0"]
                 [org.clojure/tools.macro "0.1.5"]
                 [com.taoensso/timbre "4.0.2"]

]
  :main ^:skip-aot r-messagebus.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})

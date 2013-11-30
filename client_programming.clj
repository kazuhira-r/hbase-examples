(use '[leiningen.exec :only (deps)])
(deps '[[org.apache.hbase/hbase-client "0.95.2-hadoop2"]])

(ns hbase-client.examples
  (:import (org.apache.hadoop.hbase HBaseConfiguration HTableDescriptor HColumnDescriptor)
           (org.apache.hadoop.hbase.client HBaseAdmin HConnectionManager)
           (org.apache.hadoop.hbase.client Delete Get Put Scan)
           (org.apache.hadoop.hbase.util Bytes)))

;; Configuration
(def conf (HBaseConfiguration.))

(defn to-b [value]
  (Bytes/toBytes (str value)))
(defn to-s [binary]
  (String. binary "UTF-8"))

;; テーブルが未存在の場合、テーブル作成
(let [admin (HBaseAdmin. conf)
      tableDesc (HTableDescriptor. "book")]
  (when-not (.tableExists admin (.getName tableDesc))
    (doto tableDesc
      (.addFamily (HColumnDescriptor. "main"))
      (.addFamily (HColumnDescriptor. "authors"))
      (.addFamily (HColumnDescriptor. "details")))
    (.createTable admin tableDesc)))

;; データ登録
(defn putData [table row-key main authors details]
  (let [ts (System/currentTimeMillis)
        put (Put. (to-b row-key) ts)]
    (.add put (to-b "main") (to-b (name :isbn13)) (to-b (:isbn13 main)))
    (.add put (to-b "main") (to-b (name :title)) (to-b (:title main)))
    (reduce (fn [n author]
              (.add put (to-b "authors") (to-b (str "author-" n)) (to-b author))
              (inc n))
            1
            authors)
    (.add put (to-b "details") (to-b (name :price)) (to-b (:price details)))
    (.add put (to-b "details") (to-b (name :abstraction)) (to-b (:abstraction details)))
    (.put table put)))

;; データ取得
(defn get
  ([table row]
     (let [get (Get. (to-b row))]
       (.get table get)))
  ([table row family]
     (let [get (Get. (to-b row))]
       (.addFamily get (to-b family))
       (.get table get))))

;; 存在確認
(defn exists
  ([table row]
     (let [get (Get. (to-b row))]
       (.exists table get)))
  ([table row family]
     (let [get (Get. (to-b row))]
       (.addFamily get (to-b family))
       (.exists table get))))

;; データ削除
(defn delete
  ([table row]
     (let [delete (Delete. (to-b row))]
       (.delete table delete))
     )
  ([table row family]
     (let [delete (Delete. (to-b row))]
       (.deleteFamily delete (to-b family))
       (.delete table delete))))

;; スキャン
(defn scan
  ([table start-row]
     (let [scan (Scan. (to-b start-row))]
       (with-open [result-scanner (.getScanner table scan)]
         (seq result-scanner))))
  ([table start-row end-row]
     (let [scan (Scan. (to-b start-row) (to-b end-row))]
       (with-open [result-scanner (.getScanner table scan)]
         (seq result-scanner)))))


(with-open [connection (HConnectionManager/createConnection conf)]
  (with-open [table (.getTable connection "book")]
    ;; データ登録
    (putData table
             "book1"
             {:isbn13 "978-4873115665" :title "HBase"}
             ["Lars George" "Sky株式会社 玉川 竜司"]
             {:price 4410 :abstraction "ビッグデータのランダムアクセス系処理に欠かせないHBaseについて、基礎から応用までを詳細に解説。"})
    (putData table
             "book2"
             {:isbn13 "978-4873115290" :title "Cassandra"}
             ["Eben Hewitt""大谷 晋平" "小林 隆"]
             {:price 3570 :abstraction "本書は、NoSQLミドルウェアの代表格であるCassandraについて包括的に解説する書籍です。"})
    (putData table
             "book3"
             {:isbn13 "978-4798128436" :title "Cassandra実用システムインテグレーション"}
             ["中村 寿一" "長田 伊織" "谷内 隼斗" "藤田 洋" "森井 幸希" "岸本 康二"]
             {:price 3360  :abstraction "話題のオープンソースソフトの実用解説書が登場! Cassandra(カサンドラ) は分散KVS(Key-Value Store)の一種で、システムのバックエンドを支えるためのデータストアです。"})
    (putData table
             "book4"
             {:isbn13 "978-4873115900" :title "MongoDBイン・アクション"}
             ["Kyle Banker" "Sky株式会社 玉川 竜司"]
             {:price 3570 :abstraction "本書はMongoDBを学びたいアプリケーション開発者やDBAに向けて、MongoDBの基礎から応用までを包括的に解説する書籍です。"}))


  ;; データの存在確認
  (with-open [table (.getTable connection "book")]
    (println "===== row all family =====")
    (doseq[kv (.list (get table "book1"))]
      (printf
       "row = %s, family = %s, qualifier = %s, value = %s%n"
       (to-s (.getRow kv))
       (to-s (.getFamily kv))
       (to-s (.getQualifier kv))
       (to-s (.getValue kv))))
    
    (println "===== row one family =====")
    (doseq [kv (.list (get table "book2" "main"))]
      (printf
       "row = %s, family = %s, qualifier = %s, value = %s%n"
       (to-s (.getRow kv))
       (to-s (.getFamily kv))
       (to-s (.getQualifier kv))
       (to-s (.getValue kv)))))

  ;; データの存在確認
  (with-open [table (.getTable connection "book")]
    (assert (exists table "book3"))
    (assert (not (exists table "notExistsTable")))
    (assert (exists table "book4" "details")))

  ;; データの削除
  (with-open [table (.getTable connection "book")]
    (delete table "book3")
    (delete table "book4" "details"))

  ;; スキャン
  (with-open [table (.getTable connection "book")]
    (println "===== Scan only Start =====")
    (doseq [res (scan table "book1")]
       (doseq [kv (.list res)]
         (printf
          "row = %s, family = %s, qualifier = %s, value = %s%n"
          (to-s (.getRow kv))
          (to-s (.getFamily kv))
          (to-s (.getQualifier kv))
          (to-s (.getValue kv)))))

    (println "===== Scan Spec Start & End =====")
    (doseq [res (scan table "book1" "book4")]  ;; Endは入らない
       (doseq [kv (.list res)]
         (printf
          "row = %s, family = %s, qualifier = %s, value = %s%n"
          (to-s (.getRow kv))
          (to-s (.getFamily kv))
          (to-s (.getQualifier kv))
          (to-s (.getValue kv)))))))

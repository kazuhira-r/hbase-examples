(use '[leiningen.exec :only (deps)])
(deps '[[org.apache.hbase/hbase-client "0.95.2-hadoop2"]])

(ns hbase.getting-started
  (:import (org.apache.hadoop.hbase HBaseConfiguration HTableDescriptor HColumnDescriptor)
           (org.apache.hadoop.hbase.client HBaseAdmin HTable Put)
           (org.apache.hadoop.hbase.util Bytes)))

(def conf (HBaseConfiguration/create))

;; テーブルが未存在の場合、テーブル作成
(let [admin (HBaseAdmin. conf)
      tableDesc (HTableDescriptor. "testtable")]
  (when-not (.tableExists admin (.getName tableDesc))
    (.addFamily tableDesc (HColumnDescriptor. "colfam1"))
    (.createTable admin tableDesc)))

;; テーブルにデータをPut
(with-open [table (HTable. conf "testtable")]
  (let [put (Put. (Bytes/toBytes "row1"))]
    (doto put
      (.add (Bytes/toBytes "colfam1") (Bytes/toBytes "qual1") (Bytes/toBytes "val1"))
      (.add (Bytes/toBytes "colfam1") (Bytes/toBytes "qual2") (Bytes/toBytes "val2")))
    (.put table put)))

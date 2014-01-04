package org.littlewings.hbase.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, HColumnDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HConnectionManager, HTableInterface, HTablePool}
import org.apache.hadoop.hbase.client.{Delete, Get, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes

import org.scalatest.FunSpec
import org.scalatest.Matchers._

class HBaseGettingStartedSpec extends FunSpec {
  val hbaseBook: Book =
    Book("978-4873115665",
         "HBase",
         Array("Lars George", "Sky株式会社 玉川 竜司"),
         4410,
         "ビッグデータのランダムアクセス系処理に欠かせないHBaseについて、基礎から応用までを詳細に解説。")

  val cassandraBook: Book =
    Book("978-4873115290",
         "Cassandra",
         Array("Eben Hewitt", "大谷 晋平", "小林 隆"),
         3570,
         "本書は、NoSQLミドルウェアの代表格であるCassandraについて包括的に解説する書籍です。")

  val mongoDbBook: Book =
    Book("978-4873115900",
         "MongoDBイン・アクション",
         Array("Kyle Banker", "Sky株式会社 玉川 竜司"),
         3570,
         "本書はMongoDBを学びたいアプリケーション開発者やDBAに向けて、MongoDBの基礎から応用までを包括的に解説する書籍です。")

  describe("HBase Getting Started Spec") {
    it("disable drop table") {
      // テーブルの無効化と削除
      val conf = HBaseConfiguration.create
      val tableName = "book"
      val admin = new HBaseAdmin(conf)
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }

    it("put data") {
      // データ登録
      withHTable { htable =>
        val timestamp = System.currentTimeMillis

        Array(hbaseBook, cassandraBook, mongoDbBook).foreach { book =>
          val put = new Put(Bytes.toBytes(book.isbn13), timestamp)
          put.add(Bytes.toBytes("main"), Bytes.toBytes("isbn13"), Bytes.toBytes(book.isbn13))
          put.add(Bytes.toBytes("main"), Bytes.toBytes("title"), Bytes.toBytes(book.title))

          book.authors.zipWithIndex.foreach { case (a, i) =>
            // author-1, author-2...
            put.add(Bytes.toBytes("authors"), Bytes.toBytes("author-" + (i + 1)), Bytes.toBytes(a))
          }

          // Bytes.toBytes(Int)
          put.add(Bytes.toBytes("details"), Bytes.toBytes("price"), Bytes.toBytes(book.price))
          put.add(Bytes.toBytes("details"), Bytes.toBytes("summary"), Bytes.toBytes(book.summary))

          htable.put(put)
        }
      }
    }

    it("get data all family") {
      // ひとつのRowに含まれる、全カラムファミリーのデータを取得
      withHTable { htable =>
        val get = new Get(Bytes.toBytes(hbaseBook.isbn13))
        val result = htable.get(get)
        val keyValues = result.list

        keyValues should have size 6

        Bytes.toString(keyValues.get(0).getRow) should be (hbaseBook.isbn13)
        Bytes.toString(keyValues.get(0).getFamily) should be ("authors")
        Bytes.toString(keyValues.get(0).getQualifier) should be ("author-1")
        Bytes.toString(keyValues.get(0).getValue) should be (hbaseBook.authors(0))

        Bytes.toString(keyValues.get(1).getRow) should be (hbaseBook.isbn13)
        Bytes.toString(keyValues.get(1).getFamily) should be ("authors")
        Bytes.toString(keyValues.get(1).getQualifier) should be ("author-2")
        Bytes.toString(keyValues.get(1).getValue) should be (hbaseBook.authors(1))

        Bytes.toString(keyValues.get(2).getRow) should be (hbaseBook.isbn13)
        Bytes.toString(keyValues.get(2).getFamily) should be ("details")
        Bytes.toString(keyValues.get(2).getQualifier) should be ("price")
        // ここ、Bytes.toInt！
        Bytes.toInt(keyValues.get(2).getValue) should be (hbaseBook.price)

        Bytes.toString(keyValues.get(3).getRow) should be (hbaseBook.isbn13)
        Bytes.toString(keyValues.get(3).getFamily) should be ("details")
        Bytes.toString(keyValues.get(3).getQualifier) should be ("summary")
        Bytes.toString(keyValues.get(3).getValue) should be (hbaseBook.summary)

        Bytes.toString(keyValues.get(4).getRow) should be (hbaseBook.isbn13)
        Bytes.toString(keyValues.get(4).getFamily) should be ("main")
        Bytes.toString(keyValues.get(4).getQualifier) should be ("isbn13")
        Bytes.toString(keyValues.get(4).getValue) should be (hbaseBook.isbn13)

        Bytes.toString(keyValues.get(5).getRow) should be (hbaseBook.isbn13)
        Bytes.toString(keyValues.get(5).getFamily) should be ("main")
        Bytes.toString(keyValues.get(5).getQualifier) should be ("title")
        Bytes.toString(keyValues.get(5).getValue) should be (hbaseBook.title)
      }
    }

    it("get data one family") {
      // ひとつのRowに含まれる、特定のカラムファミリーのデータを取得
      withHTable { htable =>
        val get = new Get(Bytes.toBytes(cassandraBook.isbn13))
        get.addFamily(Bytes.toBytes("main"))
        val result = htable.get(get)
        val keyValues = result.list

        keyValues should have size 2

        Bytes.toString(keyValues.get(0).getRow) should be (cassandraBook.isbn13)
        Bytes.toString(keyValues.get(0).getFamily) should be ("main")
        Bytes.toString(keyValues.get(0).getQualifier) should be ("isbn13")
        Bytes.toString(keyValues.get(0).getValue) should be (cassandraBook.isbn13)

        Bytes.toString(keyValues.get(1).getRow) should be (cassandraBook.isbn13)
        Bytes.toString(keyValues.get(1).getFamily) should be ("main")
        Bytes.toString(keyValues.get(1).getQualifier) should be ("title")
        Bytes.toString(keyValues.get(1).getValue) should be (cassandraBook.title)
      }
    }

    it("exists row") {
      // Rowの存在確認
      withHTable { htable =>
        val get = new Get(Bytes.toBytes(hbaseBook.isbn13))
        htable.exists(get) should be (true)
      }
    }

    it ("not exists row") {
      // 存在しないRow-Keyの確認
      withHTable { htable =>
        val get = new Get(Bytes.toBytes("not-exists-row"))
        htable.exists(get) should be (false)
      }
    }

    it ("exists column family") {
      // Row-Keyに対する、カラムファミリーが存在するかどうかの確認
      withHTable { htable =>
        val get = new Get(Bytes.toBytes(hbaseBook.isbn13))
        get.addFamily(Bytes.toBytes("details"))
        htable.exists(get) should be (true)
      }
    }

    it("scan all row") {
      // 全行を選択するScan
      withHTable { htable =>
        val scan = new Scan
        val resultScanner = htable.getScanner(scan)

        try {
          // ResultScannerは、実際にはIterable
          // ResultScaner#nextは、Getとの時と同じResultクラスが返る

          // データは、row-keyの昇順に入っている
          // よって、Cassandraの書籍が1番最初にくる
          val cassandraResult = resultScanner.next
          cassandraResult.list should have size 7
          Bytes.toString(cassandraResult.list.get(0).getRow) should be (cassandraBook.isbn13)

          val hbaseResult = resultScanner.next
          hbaseResult.list should have size 6
          Bytes.toString(hbaseResult.list.get(0).getRow) should be (hbaseBook.isbn13)

          val mongoDbResult = resultScanner.next
          mongoDbResult.list should have size 6
          Bytes.toString(mongoDbResult.list.get(0).getRow) should be (mongoDbBook.isbn13)

          resultScanner.next should be (null)
        } finally {
          resultScanner.close()
        }
      }
    }

    it("scan start row spec") {
      // 開始行のみを指定した、スキャン
      withHTable { htable =>
        val scan = new Scan(Bytes.toBytes(hbaseBook.isbn13))
        val resultScanner = htable.getScanner(scan)

        try {
          val hbaseResult = resultScanner.next
          hbaseResult.list should have size 6
          Bytes.toString(hbaseResult.list.get(0).getRow) should be (hbaseBook.isbn13)

          val mongoDbResult = resultScanner.next
          mongoDbResult.list should have size 6
          Bytes.toString(mongoDbResult.list.get(0).getRow) should be (mongoDbBook.isbn13)

          resultScanner.next should be (null)
        } finally {
          resultScanner.close()
        }
      }
    }

    it("scan start & end row spec") {
      // 開始行と終了行を指定した、スキャン
      withHTable { htable =>
        val scan = new Scan(Bytes.toBytes(cassandraBook.isbn13), Bytes.toBytes(mongoDbBook.isbn13))
        val resultScanner = htable.getScanner(scan)

        try {
          val cassandraResult = resultScanner.next
          cassandraResult.list should have size 7
          Bytes.toString(cassandraResult.list.get(0).getRow) should be (cassandraBook.isbn13)

          val hbaseResult = resultScanner.next
          hbaseResult.list should have size 6
          Bytes.toString(hbaseResult.list.get(0).getRow) should be (hbaseBook.isbn13)

          // 最後に指定した行は、結果には含まれない
          resultScanner.next should be (null)
        } finally {
          resultScanner.close()
        }
      }
    }

    it ("delete row") {
      // Row全体の削除
      withHTable { htable =>
        val delete = new Delete(Bytes.toBytes(mongoDbBook.isbn13))
        htable.delete(delete)

        val get = new Get(Bytes.toBytes(mongoDbBook.isbn13))
        htable.exists(get) should be (false)
      }
    }

    it ("delete column family") {
      // Rowの中の、特定のカラムファミリーのみを削除
      withHTable { htable =>
        val delete = new Delete(Bytes.toBytes(cassandraBook.isbn13))
        delete.deleteFamily(Bytes.toBytes("details"))
        htable.delete(delete)

        val get = new Get(Bytes.toBytes(cassandraBook.isbn13))
        htable.exists(get) should be (true)

        get.addFamily(Bytes.toBytes("details"))
        htable.exists(get) should be (false)
      }
    }
  }

  def withHTable(fun: HTableInterface => Unit): Unit = {
    val conf = HBaseConfiguration.create
    val tableName = "book"

    // テーブルがなければ、作成
    createTableIfNonExists(tableName, conf)

    val pool = new HTablePool(conf, 10)
    try {
      val table = pool.getTable(tableName)
      try {
        fun(table)
      } finally {
        table.close()
      }
    } finally {
      pool.close()
    }
  }

  def createTableIfNonExists(tableName: String, conf: Configuration): Unit = {
    val admin = new HBaseAdmin(conf)
    val tableDesc = new HTableDescriptor(tableName)

    if (!admin.tableExists(tableDesc.getName)) {
      tableDesc.addFamily(new HColumnDescriptor("main"))
      tableDesc.addFamily(new HColumnDescriptor("authors"))
      tableDesc.addFamily(new HColumnDescriptor("details"))

      admin.createTable(tableDesc)
    }
  }
}

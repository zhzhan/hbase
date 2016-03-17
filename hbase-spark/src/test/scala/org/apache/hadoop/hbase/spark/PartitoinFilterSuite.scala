package org.apache.hadoop.hbase.spark

import org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf
import org.apache.hadoop.hbase.{TableName, HBaseTestingUtility}
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

case class FilterRangeRecord(
                              col0: Integer,
                              col1: Boolean,
                              col2: Double,
                              col3: Float,
                              col4: Int,
                              col5: Long,
                              col6: Short,
                              col7: String,
                              col8: Byte)

object FilterRangeRecord {
  def apply(i: Int): FilterRangeRecord = {
    FilterRangeRecord(if (i % 2 == 0) i else -i,
      i % 2 == 0,
      if (i % 2 == 0) i.toDouble else -i.toDouble,
      i.toFloat,
      if (i % 2 == 0) i else -i,
      i.toLong,
      i.toShort,
      s"String$i extra",
      i.toByte)
  }
}

class PartitionFilterSuite extends FunSuite with
  BeforeAndAfterEach with BeforeAndAfterAll with Logging {
  @transient var sc: SparkContext = null
  var TEST_UTIL: HBaseTestingUtility = new HBaseTestingUtility

  var sqlContext: SQLContext = null
  var df: DataFrame = null

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> cat))
      .format("org.apache.hadoop.hbase.spark")
      .load()
  }

  override def beforeAll() {

    TEST_UTIL.startMiniCluster
    val sparkConf = new SparkConf
    sparkConf.set(HBaseSparkConf.BLOCK_CACHE_ENABLE, "true")
    sparkConf.set(HBaseSparkConf.BATCH_NUM, "100")
    sparkConf.set(HBaseSparkConf.CACHE_SIZE, "100")

    sc = new SparkContext("local", "test", sparkConf)
    new HBaseContext(sc, TEST_UTIL.getConfiguration)
    sqlContext = new SQLContext(sc)
  }

  override def afterAll() {
    logInfo("shuting down minicluster")
    TEST_UTIL.shutdownMiniCluster()

    sc.stop()
  }

  override def beforeEach(): Unit = {
    DefaultSourceStaticUtils.lastFiveExecutionRules.clear()
  }

  val catalog = s"""{
                    |"table":{"namespace":"default", "name":"rangeTable"},
                    |"rowkey":"key",
                    |"columns":{
                    |"col0":{"cf":"rowkey", "col":"key", "type":"int"},
                    |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
                    |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
                    |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
                    |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
                    |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
                    |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
                    |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
                    |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
                    |}
                    |}""".stripMargin

  test("populate rangeTable") {
    val sql = sqlContext
    import sql.implicits._

    val data = (0 until 32).map { i =>
      FilterRangeRecord(i)
    }
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.hadoop.hbase.spark")
      .save()
  }


  test("rangeTable full query") {
    val df = withCatalog(catalog)
    df.show
    assert(df.count() === 32)
  }

  test("rangeTable rowkey less than 0") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"col0" < 0)
    s.show
    assert(s.count() === 16)
  }

  test("rangeTable int col less than 0") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"col4" < 0)
    s.show
    assert(s.count() === 16)
  }

  test("rangeTable double col less than 0") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"col2" < 3.0)
    s.show
    assert(s.count() === 18)
  }

  test("rangeTable lessequal than -10") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"col0" <= -10)
    s.show
    assert(s.count() === 11)
  }

  test("rangeTable lessequal than -9") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"col0" <= -9)
    s.show
    assert(s.count() === 12)
  }

  test("rangeTable greaterequal than -9") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"col0" >= -9)
    s.show
    assert(s.count() === 21)
  }

  test("rangeTable greaterequal  than 0") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"col0" >= 0)
    s.show
    assert(s.count() === 16)
  }

  test("rangeTable greater than 10") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"col0" > 10)
    s.show
    assert(s.count() === 10)
  }

  test("rangeTable and") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"col0" > -10 && $"col0" <= 10)
    s.show
    assert(s.count() === 11)
  }

  test("or") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"col0" <= -10 || $"col0" > 10)
    s.show
    assert(s.count() === 21)
  }

  test("rangeTable all") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"col0" >= -100)
    s.show
    assert(s.count() === 32)
  }
}

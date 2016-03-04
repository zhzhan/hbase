package org.apache.hadoop.hbase.spark

import org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseTestingUtility}
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}


case class HRecord(
                        col0: String,
                        col1: Boolean,
                        col2: Double,
                        col3: Float,
                        col4: Int,
                        col5: Long,
                        col6: Short,
                        col7: String,
                        col8: Byte)

object HRecord {
  def apply(i: Int, t: String): HRecord = {
    val s = s"""row${"%03d".format(i)}"""
    HRecord(s,
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i: $t",
      i.toByte)
  }
}

class HBaseSourceSuite extends FunSuite with
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
  def catalog = s"""{
                    |"table":{"namespace":"default", "name":"htable1"},
                    |"rowkey":"key",
                    |"columns":{
                    |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
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

  test("populate table") {
    //createTable(tableName, columnFamilies)
    val sql = sqlContext
    import sql.implicits._

    val data = (0 to 255).map { i =>
      HBaseRecord(i, "extra")
    }
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.hadoop.hbase.spark")
      .save()
  }


  test("empty column") {
    val df = withCatalog(catalog)
    df.registerTempTable("table0")
    val c = sqlContext.sql("select count(1) from table0").rdd.collect()(0)(0).asInstanceOf[Long]
    assert(c == 256)
  }

  test("full query") {
    val df = withCatalog(catalog)
    df.show
    assert(df.count() == 256)
  }

  test("filtered query0") {
    val sql = sqlContext
    import sql.implicits._

    val df = withCatalog(catalog)
    val s = df.filter($"col0" <= "row005")
      .select("col0", "col1")
    s.show
    assert(s.count() == 6)
  }

}

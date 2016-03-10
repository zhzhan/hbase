/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.spark

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hadoop.hbase.client.{Put, ConnectionFactory}
import org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseTestingUtility}
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}



class TempTestSuite extends FunSuite with
  BeforeAndAfterEach with BeforeAndAfterAll with Logging {
  @transient var sc: SparkContext = null
  var TEST_UTIL: HBaseTestingUtility = new HBaseTestingUtility

  val t1TableName = "t1"
  val t2TableName = "t2"
  val columnFamily = "c"

  var sqlContext:SQLContext = null
  var df:DataFrame = null

  override def beforeAll() {

    TEST_UTIL.startMiniCluster

    logInfo(" - minicluster started")
    try
      TEST_UTIL.deleteTable(TableName.valueOf(t1TableName))
    catch {
      case e: Exception => logInfo(" - no table " + t1TableName + " found")
    }
    try
      TEST_UTIL.deleteTable(TableName.valueOf(t2TableName))
    catch {
      case e: Exception => logInfo(" - no table " + t2TableName + " found")
    }
    logInfo(" - creating table " + t1TableName)
    TEST_UTIL.createTable(TableName.valueOf(t1TableName), Bytes.toBytes(columnFamily))
    logInfo(" - created table")
    logInfo(" - creating table " + t2TableName)
    TEST_UTIL.createTable(TableName.valueOf(t2TableName), Bytes.toBytes(columnFamily))
    logInfo(" - created table")
    val sparkConf = new SparkConf
    sparkConf.set(HBaseSparkConf.BLOCK_CACHE_ENABLE, "true")
    sparkConf.set(HBaseSparkConf.BATCH_NUM, "100")
    sparkConf.set(HBaseSparkConf.CACHE_SIZE, "100")

    sc  = new SparkContext("local", "test", sparkConf)

    val connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration)
    try {
      val t1Table = connection.getTable(TableName.valueOf("t1"))

      try {
        var put = new Put(Bytes.toBytes("get1"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("1"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(1))
        t1Table.put(put)
        put = new Put(Bytes.toBytes("get2"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo2"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("4"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(4))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("z"), Bytes.toBytes("FOO"))
        t1Table.put(put)
        put = new Put(Bytes.toBytes("get3"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("8"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(8))
        t1Table.put(put)
        put = new Put(Bytes.toBytes("get4"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo4"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("10"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(10))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("z"), Bytes.toBytes("BAR"))
        t1Table.put(put)
        put = new Put(Bytes.toBytes("get5"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo5"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("8"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(8))
        t1Table.put(put)
      } finally {
        t1Table.close()
      }

      val t2Table = connection.getTable(TableName.valueOf("t2"))

      try {
        var put = new Put(Bytes.toBytes(1))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("1"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(1))
        t2Table.put(put)
        put = new Put(Bytes.toBytes(2))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo2"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("4"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(4))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("z"), Bytes.toBytes("FOO"))
        t2Table.put(put)
        put = new Put(Bytes.toBytes(3))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("8"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(8))
        t2Table.put(put)
        put = new Put(Bytes.toBytes(4))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo4"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("10"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(10))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("z"), Bytes.toBytes("BAR"))
        t2Table.put(put)
        put = new Put(Bytes.toBytes(5))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo5"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("8"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(8))
        t2Table.put(put)
      } finally {
        t2Table.close()
      }
    } finally {
      connection.close()
    }

    def hbaseTable1Catalog = s"""{
                                 |"table":{"namespace":"default", "name":"t1"},
                                 |"rowkey":"key",
                                 |"columns":{
                                 |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
                                 |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
                                 |"B_FIELD":{"cf":"c", "col":"b", "type":"string"}
                                 |}
                                 |}""".stripMargin

    new HBaseContext(sc, TEST_UTIL.getConfiguration)
    sqlContext = new SQLContext(sc)

    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map(HBaseTableCatalog.tableCatalog->hbaseTable1Catalog))

    df.registerTempTable("hbaseTable1")

    def hbaseTable2Catalog = s"""{
                                 |"table":{"namespace":"default", "name":"t2"},
                                 |"rowkey":"key",
                                 |"columns":{
                                 |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"int"},
                                 |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
                                 |"B_FIELD":{"cf":"c", "col":"b", "type":"string"}
                                 |}
                                 |}""".stripMargin


    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map(HBaseTableCatalog.tableCatalog->hbaseTable2Catalog))

    df.registerTempTable("hbaseTable2")
  }

  override def afterAll() {
    TEST_UTIL.deleteTable(TableName.valueOf(t1TableName))
    logInfo("shuting down minicluster")
    TEST_UTIL.shutdownMiniCluster()

    sc.stop()
  }

  override def beforeEach(): Unit = {
    DefaultSourceStaticUtils.lastFiveExecutionRules.clear()
  }

  /**
    * A example of a OR merge between to ranges the result is one range
    * Also an example of less then and greater then
    */
  test("Test two range rowKey query") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
      "WHERE " +
      "( KEY_FIELD < 'get2' or KEY_FIELD > 'get3')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 3)

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( KEY_FIELD < 0 OR KEY_FIELD > 1 )"))

    assert(executionRules.rowKeyFilter.points.size == 0)
    assert(executionRules.rowKeyFilter.ranges.size == 2)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes("")))
    assert(Bytes.equals(scanRange1.upperBound,Bytes.toBytes("get2")))
    assert(scanRange1.isLowerBoundEqualTo)
   // assert(!scanRange1.isUpperBoundEqualTo)

    val scanRange2 = executionRules.rowKeyFilter.ranges.get(1).get
    assert(Bytes.equals(scanRange2.lowerBound,Bytes.toBytes("get3")))
    assert(scanRange2.upperBound == null)
//    assert(!scanRange2.isLowerBoundEqualTo)
    assert(scanRange2.isUpperBoundEqualTo)
  }
  /**
    * A example of a OR merge between to ranges the result is two ranges
    * Also an example of less then and greater then
    *
    * This example makes sure the code works for a int rowKey
    */
  test("Test two range rowKey query where the rowKey is Int and the ranges don't over lap") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable2 " +
      "WHERE " +
      "( KEY_FIELD < 2 or KEY_FIELD > 4)").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( KEY_FIELD < 0 OR KEY_FIELD > 1 )"))

    assert(executionRules.rowKeyFilter.points.size == 0)

    assert(executionRules.rowKeyFilter.ranges.size == 3)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    //assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes("")))
    //assert(Bytes.equals(scanRange1.upperBound, Bytes.toBytes(2)))
    assert(scanRange1.isLowerBoundEqualTo)
    //assert(!scanRange1.isUpperBoundEqualTo)

    val scanRange2 = executionRules.rowKeyFilter.ranges.get(1).get
//    assert(Bytes.equals(scanRange2.lowerBound, Bytes.toBytes(4)))
    println(s"scanRange2 $scanRange2")
  //  assert(scanRange2.upperBound == null)
   // assert(!scanRange2.isLowerBoundEqualTo)
    assert(scanRange2.isUpperBoundEqualTo)

    assert(results.length == 2)
  }

  /**
    * A example of a AND merge between to ranges the result is one range
    * Also an example of less then and equal to and greater then and equal to
    */
  test("Test one combined range rowKey query") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
      "WHERE " +
      "(KEY_FIELD <= 'get3' and KEY_FIELD >= 'get2')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 2)

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( KEY_FIELD <= 0 AND KEY_FIELD >= 1 )"))

    assert(executionRules.rowKeyFilter.points.size == 0)
    assert(executionRules.rowKeyFilter.ranges.size == 1)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes("get2")))
    assert(Bytes.equals(scanRange1.upperBound, Bytes.toBytes("get3")))
    assert(scanRange1.isLowerBoundEqualTo)
    assert(scanRange1.isUpperBoundEqualTo)

  }

  /**
    * A complex query with one point and one range for both the
    * rowKey and the a column
    */
  test("Test SQL point and range combo") {
    val results = sqlContext.sql("SELECT KEY_FIELD FROM hbaseTable1 " +
      "WHERE " +
      "(KEY_FIELD = 'get1' and B_FIELD < '3') or " +
      "(KEY_FIELD >= 'get3' and B_FIELD = '8')").take(5)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( ( KEY_FIELD == 0 AND B_FIELD < 1 ) OR " +
        "( KEY_FIELD >= 2 AND B_FIELD == 3 ) )"))

    assert(executionRules.rowKeyFilter.points.size == 1)
    assert(executionRules.rowKeyFilter.ranges.size == 1)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes("get3")))
    assert(scanRange1.upperBound == null)
    assert(scanRange1.isLowerBoundEqualTo)
    assert(scanRange1.isUpperBoundEqualTo)


    assert(results.length == 3)
  }

  /**
    * A complex query with two complex ranges that doesn't merge into one
    */
  test("Test two complete range non merge rowKey query") {

    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable2 " +
      "WHERE " +
      "( KEY_FIELD >= 1 and KEY_FIELD <= 2) or" +
      "( KEY_FIELD > 3 and KEY_FIELD <= 5)").take(10)


    assert(results.length == 4)
    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()
    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( ( KEY_FIELD >= 0 AND KEY_FIELD <= 1 ) OR " +
        "( KEY_FIELD > 2 AND KEY_FIELD <= 3 ) )"))

    assert(executionRules.rowKeyFilter.points.size == 0)
    assert(executionRules.rowKeyFilter.ranges.size == 2)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes(1)))
    assert(Bytes.equals(scanRange1.upperBound, Bytes.toBytes(2)))
    assert(scanRange1.isLowerBoundEqualTo)
    assert(scanRange1.isUpperBoundEqualTo)

    val scanRange2 = executionRules.rowKeyFilter.ranges.get(1).get
    assert(Bytes.equals(scanRange2.lowerBound,Bytes.toBytes(3)))
    assert(Bytes.equals(scanRange2.upperBound, Bytes.toBytes(5)))
//    assert(!scanRange2.isLowerBoundEqualTo)
    assert(scanRange2.isUpperBoundEqualTo)

  }

  /**
    * A complex query with two complex ranges that does merge into one
    */
  test("Test two complete range merge rowKey query") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
      "WHERE " +
      "( KEY_FIELD >= 'get1' and KEY_FIELD <= 'get2') or" +
      "( KEY_FIELD > 'get3' and KEY_FIELD <= 'get5')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 4)

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( ( KEY_FIELD >= 0 AND KEY_FIELD <= 1 ) OR " +
        "( KEY_FIELD > 2 AND KEY_FIELD <= 3 ) )"))

    assert(executionRules.rowKeyFilter.points.size == 0)
    assert(executionRules.rowKeyFilter.ranges.size == 2)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes("get1")))
    assert(Bytes.equals(scanRange1.upperBound, Bytes.toBytes("get2")))
    assert(scanRange1.isLowerBoundEqualTo)
    assert(scanRange1.isUpperBoundEqualTo)

    val scanRange2 = executionRules.rowKeyFilter.ranges.get(1).get
    assert(Bytes.equals(scanRange2.lowerBound, Bytes.toBytes("get3")))
    assert(Bytes.equals(scanRange2.upperBound, Bytes.toBytes("get5")))
//    assert(!scanRange2.isLowerBoundEqualTo)
    assert(scanRange2.isUpperBoundEqualTo)
  }
}

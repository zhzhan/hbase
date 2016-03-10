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

import java.util

import org.apache.hadoop.hbase.spark.FilterOps.FilterOps
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.datasources.hbase.{Field, Utils}
import org.apache.spark.sql.types._


object FilterOps extends Enumeration {
  type FilterOps = Value
  val Greater, GreaterEqual, Less, LessEqual, Equal, Unknown = Value
  var code = 0
  def nextCode: Byte = {
    code += 1
    (code - 1).asInstanceOf[Byte]
  }
  val BooleanEnc = nextCode
  val ShortEnc = nextCode
  val IntEnc = nextCode
  val LongEnc = nextCode
  val FloatEnc = nextCode
  val DoubleEnc = nextCode
  val StringEnc = nextCode
  val BinaryEnc = nextCode
  val TimestampEnc = nextCode
  val UnknownEnc = nextCode

  def compare(c: Int, ops: FilterOps): Boolean = {
    println(s"""compare $c, $ops""")
    ops match {
      case Greater =>  c > 0
      case GreaterEqual =>  c >= 0
      case Less =>  c < 0
      case LessEqual =>  c <= 0
    }
  }

  def encode(dt: DataType,
             value: Any): Array[Byte] = {
    dt match {
      case BooleanType =>
        val result = new Array[Byte](Bytes.SIZEOF_BOOLEAN + 1)
        result(0) = BooleanEnc
        value.asInstanceOf[Boolean] match {
          case true => result(1) = -1: Byte
          case false => result(1) = 0: Byte
        }
        result
      case ShortType =>
        val result = new Array[Byte](Bytes.SIZEOF_SHORT + 1)
        result(0) = ShortEnc
        Bytes.putShort(result, 1, value.asInstanceOf[Short])
        result
      case IntegerType =>
        val result = new Array[Byte](Bytes.SIZEOF_INT + 1)
        result(0) = IntEnc
        Bytes.putInt(result, 1, value.asInstanceOf[Int])
        result
      case LongType|TimestampType =>
        val result = new Array[Byte](Bytes.SIZEOF_LONG + 1)
        result(0) = LongEnc
        Bytes.putLong(result, 1, value.asInstanceOf[Long])
        result
      case FloatType =>
        val result = new Array[Byte](Bytes.SIZEOF_FLOAT + 1)
        result(0) = FloatEnc
        Bytes.putFloat(result, 1, value.asInstanceOf[Float])
        result
      case DoubleType =>
        val result = new Array[Byte](Bytes.SIZEOF_DOUBLE + 1)
        result(0) = DoubleEnc
        Bytes.putDouble(result, 1, value.asInstanceOf[Double])
        result
      case BinaryType =>
        val v = value.asInstanceOf[Array[Bytes]]
        val result = new Array[Byte](v.length + 1)
        result(0) = BinaryEnc
        System.arraycopy(v, 0, result, 1, v.length)
        result
      case StringType =>
        val bytes = Bytes.toBytes(value.asInstanceOf[String])
        val result = new Array[Byte](bytes.length + 1)
        result(0) = StringEnc
        System.arraycopy(bytes, 0, result, 1, bytes.length)
        result
      case _ =>
        val bytes = Bytes.toBytes(value.toString)
        val result = new Array[Byte](bytes.length + 1)
        result(0) = UnknownEnc
        System.arraycopy(bytes, 0, result, 1, bytes.length)
        result
    }
  }

  def filter(input: Array[Byte], offset1: Int, length1: Int,
             filterBytes: Array[Byte], offset2: Int, length2: Int,
             ops: FilterOps): Boolean = {
    filterBytes(offset2) match {
      case ShortEnc =>
        val in = Bytes.toShort(input, offset1)
        val value = Bytes.toShort(filterBytes, offset2 + 1)
        compare(in.compareTo(value), ops)
      case FilterOps.IntEnc =>
        val in = Bytes.toInt(input, offset1)
        val value = Bytes.toInt(filterBytes, offset2 + 1)
        compare(in.compareTo(value), ops)
      case FilterOps.LongEnc|FilterOps.TimestampEnc =>
        val in = Bytes.toInt(input, offset1)
        val value = Bytes.toInt(filterBytes, offset2 + 1)
        compare(in.compareTo(value), ops)
      case FilterOps.FloatEnc =>
        val in = Bytes.toFloat(input, offset1)
        val value = Bytes.toFloat(filterBytes, offset2 + 1)
        compare(in.compareTo(value), ops)
      case FilterOps.DoubleEnc =>
        val in = Bytes.toDouble(input, offset1)
        val value = Bytes.toDouble(filterBytes, offset2 + 1)
        compare(in.compareTo(value), ops)
      case _ =>
        // for String, Byte, Binary, Boolean and other types
        // we can use the order of byte array directly.
        compare(
          Bytes.compareTo(input, offset1, length1, filterBytes, offset2 + 1, length2 - 1), ops)
    }
  }
}

/**
 * Dynamic logic for SQL push down logic there is an instance for most
 * common operations and a pass through for other operations not covered here
 *
 * Logic can be nested with And or Or operators.
 *
 * A logic tree can be written out as a string and reconstructed from that string
 *
 */
trait DynamicLogicExpression {
  def execute(columnToCurrentRowValueMap: util.HashMap[String, ByteArrayComparable],
              valueFromQueryValueArray:Array[Array[Byte]]): Boolean
  def toExpressionString: String = {
    val strBuilder = new StringBuilder
    appendToExpression(strBuilder)
    strBuilder.toString()
  }
  def filterOps: FilterOps = FilterOps.Unknown

  def appendToExpression(strBuilder:StringBuilder)
}

trait CompareTrait {
  self: DynamicLogicExpression =>
  def columnName: String
  def valueFromQueryIndex: Int
  def execute(columnToCurrentRowValueMap:
              util.HashMap[String, ByteArrayComparable],
              valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    val currentRowValue = columnToCurrentRowValueMap.get(columnName)
    val valueFromQuery = valueFromQueryValueArray(valueFromQueryIndex)
    currentRowValue != null &&
      FilterOps.filter(currentRowValue.bytes, currentRowValue.offset, currentRowValue.length,
        valueFromQuery, 0, valueFromQuery.length, filterOps)
  }
}

class AndLogicExpression (val leftExpression:DynamicLogicExpression,
                           val rightExpression:DynamicLogicExpression)
  extends DynamicLogicExpression{
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    leftExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray) &&
      rightExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray)
  }

  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append("( ")
    strBuilder.append(leftExpression.toExpressionString)
    strBuilder.append(" AND ")
    strBuilder.append(rightExpression.toExpressionString)
    strBuilder.append(" )")
  }
}

class OrLogicExpression (val leftExpression:DynamicLogicExpression,
                          val rightExpression:DynamicLogicExpression)
  extends DynamicLogicExpression{
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    leftExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray) ||
      rightExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray)
  }
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append("( ")
    strBuilder.append(leftExpression.toExpressionString)
    strBuilder.append(" OR ")
    strBuilder.append(rightExpression.toExpressionString)
    strBuilder.append(" )")
  }
}

class EqualLogicExpression (val columnName:String,
                            val valueFromQueryIndex:Int,
                            val isNot:Boolean) extends DynamicLogicExpression{
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    val currentRowValue = columnToCurrentRowValueMap.get(columnName)
    val valueFromQuery = valueFromQueryValueArray(valueFromQueryIndex)

    currentRowValue != null &&
      Bytes.equals(valueFromQuery,
        0, valueFromQuery.length, currentRowValue.bytes,
        currentRowValue.offset, currentRowValue.length) != isNot
  }
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    val command = if (isNot) "!=" else "=="
    strBuilder.append(columnName + " " + command + " " + valueFromQueryIndex)
  }
}

class IsNullLogicExpression (val columnName:String,
                             val isNot:Boolean) extends DynamicLogicExpression{
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    val currentRowValue = columnToCurrentRowValueMap.get(columnName)

    (currentRowValue == null) != isNot
  }
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    val command = if (isNot) "isNotNull" else "isNull"
    strBuilder.append(columnName + " " + command)
  }
}

class GreaterThanLogicExpression (override val columnName:String,
                                  override val valueFromQueryIndex:Int)
  extends DynamicLogicExpression with CompareTrait{
  override val filterOps = FilterOps.Greater
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append(columnName + " > " + valueFromQueryIndex)
  }
}

class GreaterThanOrEqualLogicExpression (override val columnName:String,
                                         override val valueFromQueryIndex:Int)
  extends DynamicLogicExpression with CompareTrait{
  override val filterOps = FilterOps.GreaterEqual
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append(columnName + " >= " + valueFromQueryIndex)
  }
}

class LessThanLogicExpression (override val columnName:String,
                               override val valueFromQueryIndex:Int)
  extends DynamicLogicExpression with CompareTrait {
  override val filterOps = FilterOps.Less
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append(columnName + " < " + valueFromQueryIndex)
  }
}

class LessThanOrEqualLogicExpression (val columnName:String,
                                      val valueFromQueryIndex:Int)
  extends DynamicLogicExpression with CompareTrait{
  override val filterOps = FilterOps.LessEqual
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append(columnName + " <= " + valueFromQueryIndex)
  }
}

class PassThroughLogicExpression() extends DynamicLogicExpression {
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray: Array[Array[Byte]]): Boolean = true

  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append("dummy Pass -1")
  }
}

object DynamicLogicExpressionBuilder {
  def build(expressionString:String): DynamicLogicExpression = {

    val expressionAndOffset = build(expressionString.split(' '), 0)
    expressionAndOffset._1
  }

  private def build(expressionArray:Array[String],
                    offSet:Int): (DynamicLogicExpression, Int) = {
    println(s"""expression array ${expressionArray.mkString(":")}""")
    if (expressionArray(offSet).equals("(")) {
      val left = build(expressionArray, offSet + 1)
      val right = build(expressionArray, left._2 + 1)
      if (expressionArray(left._2).equals("AND")) {
        (new AndLogicExpression(left._1, right._1), right._2 + 1)
      } else if (expressionArray(left._2).equals("OR")) {
        (new OrLogicExpression(left._1, right._1), right._2 + 1)
      } else {
        throw new Throwable("Unknown gate:" + expressionArray(left._2))
      }
    } else {
      val command = expressionArray(offSet + 1)
      if (command.equals("<")) {
        (new LessThanLogicExpression(expressionArray(offSet),
          expressionArray(offSet + 2).toInt), offSet + 3)
      } else if (command.equals("<=")) {
        (new LessThanOrEqualLogicExpression(expressionArray(offSet),
          expressionArray(offSet + 2).toInt), offSet + 3)
      } else if (command.equals(">")) {
        (new GreaterThanLogicExpression(expressionArray(offSet),
          expressionArray(offSet + 2).toInt), offSet + 3)
      } else if (command.equals(">=")) {
        (new GreaterThanOrEqualLogicExpression(expressionArray(offSet),
          expressionArray(offSet + 2).toInt), offSet + 3)
      } else if (command.equals("==")) {
        (new EqualLogicExpression(expressionArray(offSet),
          expressionArray(offSet + 2).toInt, false), offSet + 3)
      } else if (command.equals("!=")) {
        (new EqualLogicExpression(expressionArray(offSet),
          expressionArray(offSet + 2).toInt, true), offSet + 3)
      } else if (command.equals("isNull")) {
        (new IsNullLogicExpression(expressionArray(offSet), false), offSet + 2)
      } else if (command.equals("isNotNull")) {
        (new IsNullLogicExpression(expressionArray(offSet), true), offSet + 2)
      } else if (command.equals("Pass")) {
        (new PassThroughLogicExpression, offSet + 3)
      } else {
        throw new Throwable("Unknown logic command:" + command)
      }
    }
  }
}
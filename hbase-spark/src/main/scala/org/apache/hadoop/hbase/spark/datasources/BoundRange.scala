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

package org.apache.hadoop.hbase.spark.datasources

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.spark.hbase._
import org.apache.spark.Logging
import org.apache.spark.unsafe.types.UTF8String

// Data type for range whose size is known.
// lower bound and upperbound for each range.
// If data order is the same as byte order, then left = mid = right.
// For the data type whose order is not the same as byte order, left != mid != right
// In this case, left is max, right is min and mid is the byte of the value.
// By this way, the scan will cover the whole range and will not  miss any data.
// Typically, mid is used only in Equal in which case, the order does not matter.
case class BoundRange(
                       low: Array[Byte],
                       upper: Array[Byte])

// The range in less and greater have to be lexi ordered.
case class BoundRanges(less: Array[BoundRange], greater: Array[BoundRange], value: Array[Byte])

object BoundRange extends Logging{
  def apply(in: Any): Option[BoundRanges] = in match {
    // For short, integer, and long, the order of number is consistent with byte array order
    // regardless of its sign. But the negative number is larger than positive number in byte array.
    case a: Integer =>
      val b =  Bytes.toBytes(a)
      if (a >= 0) {
        logDebug(s"range is 0 to $a and ${Integer.MIN_VALUE} to -1")
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(0: Int), b),
            BoundRange(Bytes.toBytes(Integer.MIN_VALUE),  Bytes.toBytes(-1: Int))),
          Array(BoundRange(b,  Bytes.toBytes(Integer.MAX_VALUE))), b))
      } else {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(Integer.MIN_VALUE), b)),
          Array(BoundRange(Bytes.toBytes(0: Int), Bytes.toBytes(Integer.MAX_VALUE)),
            BoundRange(b, Bytes.toBytes(-1: Integer))), b))
      }
    case a: Long =>
      val b =  Bytes.toBytes(a)
      if (a >= 0) {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(0: Long), b),
            BoundRange(Bytes.toBytes(Long.MinValue),  Bytes.toBytes(-1: Long))),
          Array(BoundRange(b,  Bytes.toBytes(Long.MaxValue))), b))
      } else {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(Long.MinValue), b)),
          Array(BoundRange(Bytes.toBytes(0: Long), Bytes.toBytes(Long.MaxValue)),
            BoundRange(b, Bytes.toBytes(-1: Long))), b))
      }
    case a: Short =>
      val b =  Bytes.toBytes(a)
      if (a >= 0) {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(0: Short), b),
            BoundRange(Bytes.toBytes(Short.MinValue),  Bytes.toBytes(-1: Short))),
          Array(BoundRange(b,  Bytes.toBytes(Short.MaxValue))), b))
      } else {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(Short.MinValue), b)),
          Array(BoundRange(Bytes.toBytes(0: Short), Bytes.toBytes(Short.MaxValue)),
            BoundRange(b, Bytes.toBytes(-1: Short))), b))
      }
    // For both double and float, the order of positive number is consistent
    // with byte array order. But the order of negative number is the reverse
    // order of byte array. Please refer to IEEE-754 and
    // https://en.wikipedia.org/wiki/Single-precision_floating-point_format
    case a: Double =>
      val b =  Bytes.toBytes(a)
      if (a >= 0.0f) {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(0.0d), b),
            BoundRange(Bytes.toBytes(-0.0d),  Bytes.toBytes(Double.MinValue))),
          Array(BoundRange(b,  Bytes.toBytes(Double.MaxValue))), b))
      } else {
        Some(BoundRanges(
          Array(BoundRange(b, Bytes.toBytes(Double.MinValue))),
          Array(BoundRange(Bytes.toBytes(0.0d), Bytes.toBytes(Double.MaxValue)),
            BoundRange(Bytes.toBytes(-0.0d), b)), b))
      }
    case a: Float =>
      val b =  Bytes.toBytes(a)
      if (a >= 0.0f) {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(0.0f), b),
            BoundRange(Bytes.toBytes(-0.0f),  Bytes.toBytes(Float.MinValue))),
          Array(BoundRange(b,  Bytes.toBytes(Float.MaxValue))), b))
      } else {
        Some(BoundRanges(
          Array(BoundRange(b, Bytes.toBytes(Float.MinValue))),
          Array( BoundRange(Bytes.toBytes(0.0f), Bytes.toBytes(Float.MaxValue)),
            BoundRange(Bytes.toBytes(-0.0f), b)), b))
      }
    case a: Array[Byte] =>
      Some(BoundRanges(
        Array(BoundRange(Array.fill(a.length)(ByteMin), a)),
        Array(BoundRange(a, Array.fill(a.length)(ByteMax))), a))
    case a: Byte =>
      val b =  Array(a)
      Some(BoundRanges(
        Array(BoundRange(Array(ByteMin), b)),
        Array(BoundRange(b, Array(ByteMax))), b))
    case a: String =>
      val b =  Bytes.toBytes(a)
      Some(BoundRanges(
        Array(BoundRange(Array.fill(a.length)(ByteMin), b)),
        Array(BoundRange(b, Array.fill(a.length)(ByteMax))), b))
    case a: UTF8String =>
      val b = a.getBytes
      Some(BoundRanges(
        Array(BoundRange(Array.fill(a.numBytes())(ByteMin), b)),
        Array(BoundRange(b, Array.fill(a.numBytes())(ByteMax))), b))
    case _ => None
  }
}

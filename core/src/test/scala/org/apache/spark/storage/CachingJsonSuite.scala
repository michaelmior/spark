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

package org.apache.spark.storage

import org.json4s.jackson.JsonMethods._
import org.scalatest.Assertions
import org.scalatest.exceptions.TestFailedException

import org.apache.spark._

class CachingJsonSuite extends SparkFunSuite {
  import CachingJsonSuite._

  test("caching events can be serialized") {
    val stageCachingInfo = new StageCachingInfo(3, List(
      new RDDCachingInfo(0, 0, StorageLevel.MEMORY_ONLY)
    ))
    val actualJsonString = compact(render(CachingJson.stageCachingInfoToJson(stageCachingInfo)))
    val newStageCachingInfo = CachingJson.stageCachingInfoFromJson(parse(actualJsonString))
    assertJsonStringEquals(stageCachingInfoJsonString, actualJsonString)
    assertEquals(stageCachingInfo, newStageCachingInfo)
  }
}

private[spark] object CachingJsonSuite extends Assertions {
  private def assertJsonStringEquals(expected: String, actual: String) {
    val expectedJson = pretty(parse(expected))
    val actualJson = pretty(parse(actual))
    if (expectedJson != actualJson) {
      // scalastyle:off
      // This prints something useful if the JSON strings don't match
      println("=== EXPECTED ===\n" + expectedJson + "\n")
      println("=== ACTUAL ===\n" + actualJson + "\n")
      // scalastyle:on
      throw new TestFailedException(s"JSON did not equal", 1)
    }
  }

  private def assertRddCachingInfoEquals(rddCaching1: RDDCachingInfo, rddCaching2: RDDCachingInfo) {
    assertEquals(rddCaching1, rddCaching2)
  }

  private def assertEquals(stageCaching1: StageCachingInfo, stageCaching2: StageCachingInfo) {
    assert(stageCaching1.stageId === stageCaching2.stageId)
    assertSeqEquals(stageCaching1.rddCachingInfos, stageCaching2.rddCachingInfos, assertRddCachingInfoEquals)
  }

  private def assertEquals(rddCaching1: RDDCachingInfo, rddCaching2: RDDCachingInfo) {
    assert(rddCaching1.id === rddCaching2.id)
    assert(rddCaching1.partition === rddCaching2.partition)
    assertEquals(rddCaching1.storageLevel, rddCaching2.storageLevel)
  }

  private def assertEquals(level1: StorageLevel, level2: StorageLevel) {
    assert(level1.useDisk === level2.useDisk)
    assert(level1.useMemory === level2.useMemory)
    assert(level1.deserialized === level2.deserialized)
    assert(level1.replication === level2.replication)
  }

  private def assertSeqEquals[T](seq1: Seq[T], seq2: Seq[T], assertEquals: (T, T) => Unit) {
    assert(seq1.length === seq2.length)
    seq1.zip(seq2).foreach { case (t1, t2) =>
      assertEquals(t1, t2)
    }
  }

  private val stageCachingInfoJsonString =
    s"""
      |{
      |  "Stage ID" : 3,
      |  "Cached RDDs" : [
      |    {
      |      "RDD ID" : 0,
      |      "Partition" : 0,
      |      "Storage Level" : {
      |        "Use Disk" : false,
      |        "Use Memory" : true,
      |        "Deserialized" : true,
      |        "Replication" : 1
      |      }
      |   } 
      | ]
      |}
    """.stripMargin
}

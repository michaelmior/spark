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

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark._

private[spark] object CachingJson {
  private implicit val format = DefaultFormats

  def stageCachingInfoToJson(stageCachingInfo: StageCachingInfo): JValue = {
    val rddCachingInfo = JArray(stageCachingInfo.rddCachingInfos.map(rddCachingInfoToJson).toList)
    ("Stage ID" -> stageCachingInfo.stageId) ~
    ("Cached RDDs" -> rddCachingInfo)
  }

  def rddCachingInfoToJson(rddCachingInfo: RDDCachingInfo): JValue = {
    val storageLevel = storageLevelToJson(rddCachingInfo.storageLevel)
    ("RDD ID" -> rddCachingInfo.id) ~
    ("Partition" -> rddCachingInfo.partition) ~
    ("Storage Level" -> storageLevel)
  }

  def storageLevelToJson(storageLevel: StorageLevel): JValue = {
    ("Use Disk" -> storageLevel.useDisk) ~
    ("Use Memory" -> storageLevel.useMemory) ~
    ("Deserialized" -> storageLevel.deserialized) ~
    ("Replication" -> storageLevel.replication)
  }

  def cachingFromJson(json: JValue): HashMap[Int, StageCachingInfo] = {
    val stageCachingInfos = (json \ "Stages").extract[List[JValue]].map(stageCachingInfoFromJson)
    HashMap(stageCachingInfos.map { sci => (sci.stageId, sci) }: _*)
  }

  def stageCachingInfoFromJson(json: JValue): StageCachingInfo = {
    val stageId = (json \ "Stage ID").extract[Int]
    val rddCachingInfos = (json \ "Cached RDDs").extract[List[JValue]].map(rddCachingInfoFromJson)

    new StageCachingInfo(stageId, rddCachingInfos)
  }

  def rddCachingInfoFromJson(json: JValue): RDDCachingInfo = {
    val rddId = (json \ "RDD ID").extract[Int]
    val partition = (json \ "Partition").extract[Int]
    val storageLevel = storageLevelFromJson(json \ "Storage Level")

    new RDDCachingInfo(rddId, partition, storageLevel)
  }

  def storageLevelFromJson(json: JValue): StorageLevel = {
    val useDisk = (json \ "Use Disk").extract[Boolean]
    val useMemory = (json \ "Use Memory").extract[Boolean]
    val deserialized = (json \ "Deserialized").extract[Boolean]
    val replication = (json \ "Replication").extract[Int]
    StorageLevel(useDisk, useMemory, deserialized, replication)
  }
}

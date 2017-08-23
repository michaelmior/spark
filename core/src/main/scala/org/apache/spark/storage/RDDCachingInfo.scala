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

import org.apache.spark.Dependency
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

@DeveloperApi
class RDDCachingInfo(
    val id: Int,
    val partition: Int,
    var storageLevel: StorageLevel)
  extends Ordered[RDDCachingInfo] {

  override def toString: String = {
    import Utils.bytesToString
    ("RDD (%d) StorageLevel: %s").format(id, storageLevel.toString)
  }

  override def compare(that: RDDCachingInfo): Int = {
    this.id - that.id
  }
}

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

package org.apache.spark.rdd

import scala.collection.mutable.{HashMap, HashSet, LinkedHashMap}

import org.apache.spark.internal.config._
import org.apache.spark.util.CallSite

class SimpleReusePredictor extends RDDUsePredictor {
  private val rddUses = HashMap[Int, LinkedHashMap[Int, HashSet[Int]]]()

  override def trackAction(rdd: RDD[_], cs: CallSite): Unit = {
    addUse(rdd, cs)
  }

  override def trackUse(rdd: RDD[_], child: RDD[_]): Unit = {
    addUse(rdd, child.creationSite)
  }

  private def addUse(rdd: RDD[_], callSite: CallSite): Unit = {
    val callSiteTag = rdd.creationSite.longForm.hashCode
    val useMap = rddUses.getOrElseUpdate(callSiteTag, new LinkedHashMap[Int, HashSet[Int]]())
    val useSet = useMap.getOrElseUpdate(rdd.id, new HashSet[Int]())
    useSet.add(callSite.longForm.hashCode)

    val reuses = useSet.size
    rdd.reuseCount = Some(reuses)
    if (reuses > 1) {
      rdd.implicitPersist()
      rdd.lazyUnpersist(Some(reuses))
    }
  }

  override def predictReuse(rdd: RDD[_]): Boolean = {
    val callSiteTag = rdd.creationSite.longForm.hashCode
    if (!rddUses.contains(callSiteTag)) {
      false
    } else {
      rddUses(callSiteTag).headOption match {
        case Some((_, uses)) => uses.size > 1
        case None => false
      }
    }
  }
}

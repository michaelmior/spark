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

package org.apache.spark.scheduler

import scala.collection.mutable.{ArrayBuffer, ArrayStack, HashMap}

import org.apache.spark._
import org.apache.spark.internal.config._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util._

case class IterationLoop(loop: Int, counter: Int)

class IterationManager(
    private[scheduler] val sc: SparkContext)
  extends Logging {

  private val manageCaching = sc.conf.get(ITERATION_MANAGE_CACHING)

  private var currentLoop: ArrayStack[Int] = new ArrayStack[Int]
  private var currentIteration: Int = 0
  private val loopRdds = new HashMap[Int, ArrayBuffer[RDD[_]]]
  private val useCount = new HashMap[(Int, Int), Int]
  private val ancestorRdds = new HashMap[Int, ArrayBuffer[RDD[_]]]

  def startLoop(): Int = {
    val loopId = sc.newLoop()
    currentLoop.push(loopId)
    loopId
  }

  def iterateLoop(loopId: Int): Unit = {
    if (currentIteration == 1) {
      loopRdds(loopId).foreach { rdd =>
        if (rdd.loop.get.counter == 1) {
          // Record RDDs generated in the second loop iteration since this
          // is the first time we can see loop dependencies
          rdd.dependencies.foreach{ dep =>
            useCount((loopId, dep.rdd.callSiteTag)) =
              useCount.getOrElse((loopId, dep.rdd.callSiteTag), 0) + 1
          }
        }
      }
    }

    loopRdds(loopId).foreach { rdd =>
      if (rdd.loop.get.counter < currentIteration &&
          rdd.getStorageLevel != StorageLevel.NONE &&
          rdd.implicitlyPersisted && manageCaching) {
        rdd.lazyUnpersist()
      }
    }

    currentIteration += 1
  }

  def endLoop(loopId: Int): Unit = {
    assert(currentLoop.pop() == loopId, "Error when trying to end loop")
    currentIteration = -1
  }

  def registerRdd(rdd: RDD[_]): Option[IterationLoop] = {
    if (currentLoop.isEmpty) {
      None
    } else {
      val loopId = currentLoop.top
      val ancestors = ancestorRdds.getOrElseUpdate(rdd.callSiteTag, new ArrayBuffer[RDD[_]]())
      ancestors += rdd

      val rdds = loopRdds.getOrElseUpdate(loopId, new ArrayBuffer[RDD[_]]())
      rdds += rdd

      if (currentIteration > 1) {
        useCount.get((loopId, rdd.callSiteTag)) match {
          case Some(count) =>
            if (count > 1 && manageCaching) {
              rdd.implicitPersist()
            }
          case None => ()
        }
      }

      Some(IterationLoop(loopId, currentIteration))
    }
  }

  def unregisterAncestors(rdd: RDD[_], keepPrevious: Int = 0): Seq[RDD[_]] = {
    ancestorRdds.get(rdd.callSiteTag) match {
      case Some(rdds) =>
        val ancestors = new ArrayBuffer[RDD[_]]

        rdds.foreach { ancestor =>
          if (ancestor.loop.get.counter < rdd.loop.get.counter - keepPrevious) {
            ancestors += ancestor
          }
        }

        ancestors
      case None => Seq.empty[RDD[_]]
    }
  }
}

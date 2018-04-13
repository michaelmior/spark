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

import scala.collection.mutable.{ArrayBuffer, ArrayStack, HashMap, HashSet}

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util._

case class IterationLoop(loop: Int, counter: Int)

class IterationManager(
    private[scheduler] val sc: SparkContext)
  extends Logging {

  private val manageCaching = sc.conf.get(ITERATION_MANAGE_CACHING)
  private val outsideCaching = sc.conf.get(ITERATION_OUTSIDE_CACHING)

  private var currentLoop: ArrayStack[Int] = new ArrayStack[Int]
  private var currentIteration: ArrayStack[Int] = new ArrayStack[Int]
  private val loopRdds = new HashMap[Int, ArrayBuffer[RDD[_]]]
  private val useCount = new HashMap[(Int, Int), Int]
  private val ancestorRdds = new HashMap[Int, ArrayBuffer[RDD[_]]]
  private val outsideLoop = new HashMap[Int, HashSet[RDD[_]]]

  def getCurrentLoop(): Option[Int] = {
    currentLoop.headOption
  }

  def getCurrentIteration(): Option[Int] = {
    currentIteration.headOption
  }

  def startLoop(): Int = {
    val loopId = sc.newLoop()
    currentLoop.push(loopId)
    currentIteration.push(0)
    loopId
  }

  private def persistOutsider(rdd: RDD[_], loopId: Int): Unit = {
    if (manageCaching && outsideCaching && rdd.loop.isEmpty && !rdd.implicitlyPersisted) {
      rdd.implicitPersist()
      val outsideRdds = outsideLoop.getOrElseUpdate(loopId, new HashSet[RDD[_]]())
      outsideRdds += rdd
    }
  }

  def iterateLoop(loopId: Int): Unit = {
    assert(currentLoop.top == loopId, "Error iterating loop")
    if (currentIteration.top == 1) {
      loopRdds(loopId).filter { rdd =>
        // Check that this RDD has not been moved outside the loop
        if (rdd.loop.isEmpty) {
          false
        } else if (rdd.loop.get.counter == 1) {
          // Record RDDs generated in the second loop iteration since this
          // is the first time we can see loop dependencies
          rdd.dependencies.foreach{ dep =>
            if (dep.rdd.loop.isEmpty) {
              if (manageCaching && outsideCaching) {
                persistOutsider(dep.rdd, loopId)
              }
            } else {
              val tag = dep.rdd.callSiteTag
              useCount((loopId, tag)) = useCount.getOrElse((loopId, tag), 0) + 1
            }
          }
        }

        true
      }
    }

    loopRdds(loopId).foreach { rdd =>
      if (rdd.loop.isEmpty) {
        // In pyspark an RDD which initially appears to be inside the loop
        // may be correctly identified later as outside the loop
        persistOutsider(rdd, loopId)
        useCount.remove((loopId, rdd.callSiteTag))
      } else if (rdd.loop.get.counter < currentIteration.top &&
          rdd.implicitlyPersisted && manageCaching) {
        rdd.lazyUnpersist()
      }
    }

    currentIteration.push(currentIteration.pop() + 1)
  }

  def endLoop(loopId: Int): Unit = {
    assert(currentLoop.pop() == loopId, "Error when trying to end loop")
    currentIteration.pop()

    // Unpersist any remaining RDDs
    loopRdds(loopId).foreach { rdd =>
      if (rdd.getStorageLevel != StorageLevel.NONE &&
          rdd.implicitlyPersisted && manageCaching) {
        rdd.lazyUnpersist()
      }
    }
    loopRdds.remove(loopId)

    if (currentLoop.isEmpty && outsideLoop.contains(loopId)) {
      outsideLoop(loopId).foreach { rdd =>
        if (rdd.implicitlyPersisted) {
          rdd.lazyUnpersist()
        }
      }
    }
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

      if (currentIteration.top > 1) {
        useCount.get((loopId, rdd.callSiteTag)) match {
          case Some(count) =>
            if (count > 1 && manageCaching && !rdd.implicitlyPersisted) {
              rdd.implicitPersist()
            }
          case None => ()
        }
      }

      Some(IterationLoop(loopId, currentIteration.top))
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

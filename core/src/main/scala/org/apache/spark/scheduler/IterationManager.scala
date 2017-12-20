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

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util._

case class IterationLoop(loop: Int, counter: Int)

class IterationManager(
    private[scheduler] val sc: SparkContext)
  extends Logging {

  private var currentLoop: Option[Int] = None
  private var currentIteration: Int = -1
  private val loopRdds = new HashMap[CallSite, ArrayBuffer[RDD[_]]]

  def startLoop(): Int = {
    val loopId = sc.newLoop()
    currentLoop = Some(loopId)
    loopId
  }

  def iterateLoop(): Unit = {
    currentIteration += 1
  }

  def endLoop(loopId: Int): Unit = {
    assert(currentLoop.get == loopId, "Error when trying to end loop")
    currentLoop = None
    currentIteration = -1
  }

  def registerRdd(rdd: RDD[_]): Option[IterationLoop] = {
    currentLoop match {
      case Some(loopId) =>
        val rdds = loopRdds.getOrElseUpdate(rdd.creationSite, new ArrayBuffer[RDD[_]]())
        rdds += rdd
        Some(IterationLoop(loopId, currentIteration))
      case None => None
    }
  }

  def unregisterAncestors(rdd: RDD[_], keepPrevious: Int = 0): Seq[RDD[_]] = {
    loopRdds.get(rdd.creationSite) match {
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

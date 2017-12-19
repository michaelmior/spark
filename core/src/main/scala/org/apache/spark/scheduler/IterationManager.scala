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

import org.apache.spark._
import org.apache.spark.internal.Logging

case class IterationLoop(loop: Int, counter: Int)

class IterationManager(
    private[scheduler] val sc: SparkContext)
  extends Logging {

  private var _currentLoop: Option[Int] = None
  private var _currentIteration: Int = -1

  def startLoop(): Int = {
    val loopId = sc.newLoop()
    _currentLoop = Some(loopId)
    loopId
  }

  def iterateLoop(): Unit = {
    _currentIteration += 1
  }

  def endLoop(loopId: Int): Unit = {
    assert(_currentLoop.get == loopId, "Error when trying to end loop")
    _currentLoop = None
    _currentIteration = -1
  }

  def registerRdd(rddId: Int): Option[IterationLoop] = {
    _currentLoop match {
      case Some(loopId) => Some(IterationLoop(loopId, _currentIteration))
      case None => None
    }
  }
}

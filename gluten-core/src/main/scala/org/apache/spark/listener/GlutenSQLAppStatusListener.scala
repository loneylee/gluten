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

package org.apache.spark.listener

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.GlutenDriverEndpoint
import org.apache.spark.rpc.GlutenRpcMessages._
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui._

/** Gluten SQL listener. Used for monitor sql on whole life cycle.Create and release resource. */
class GlutenSQLAppStatusListener() extends SparkListener with Logging {

  private val driverEndpointRef = GlutenDriverEndpoint.glutenDriverEndpoint.driverEndpoint

  /**
   * If executor was removed, driver endpoint need to remove executor endpoint ref.\n When execution
   * was end, Can't call executor ref again.
   * @param executorRemoved
   */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    driverEndpointRef.send(GlutenExecutorRemoved(executorRemoved.executorId))
    logTrace(s"Execution ${executorRemoved.executorId} Removed.")
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart => onExecutionStart(e)
    case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
    case _ => // Ignore
  }

  private def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    val executionId = event.executionId.toString
    logTrace(s"Execution $executionId start.")
  }

  /**
   * If execution was end, some backend like CH need to clean resource which is relation to this
   * execution.
   * @param event
   */
  private def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val executionId = event.executionId.toString
    driverEndpointRef.send(GlutenOnExecutionEnd(executionId))
    logTrace(s"Execution $executionId end.")
  }
}

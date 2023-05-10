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

package org.apache.spark.rpc

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.GlutenRpcMessages._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * The gluten driver endpoint is responsible for communicating with the executor. Executor will
 * register with the driver when it starts.
 */
class GlutenDriverEndpoint extends IsolatedRpcEndpoint with Logging {
  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  protected val totalRegisteredExecutors = new AtomicInteger(0)

  // keep executorRef on memory
  private val executorDataMap = new ConcurrentHashMap[String, ExecutorData]

  val driverEndpoint: RpcEndpointRef =
    rpcEnv.setupEndpoint(GlutenRpcConstants.GLUTEN_DRIVER_ENDPOINT_NAME, this)

  override def receive: PartialFunction[Any, Unit] = {
    case GlutenOnExecutionStart(executionId) =>
      executorDataMap.forEach(
        (_, executor) => executor.executorEndpointRef.send(GlutenOnExecutionStart(executionId)))

    case GlutenOnExecutionEnd(executionId) =>
      executorDataMap.forEach(
        (_, executor) => executor.executorEndpointRef.send(GlutenOnExecutionEnd(executionId)))

    case GlutenExecutorRemoved(executorId) =>
      executorDataMap.remove(executorId)
      totalRegisteredExecutors.addAndGet(-1)
      logTrace(s"Executor endpoint ref $executorId is removed.")

    case e =>
      logError(s"Received unexpected message. $e")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case GlutenRegisterExecutor(executorId, executorRef) =>
      if (executorDataMap.contains(executorId)) {
        context.sendFailure(new IllegalStateException(s"Duplicate executor ID: $executorId"))
      } else {
        // If the executor's rpc env is not listening for incoming connections, `hostPort`
        // will be null, and the client connection should be used to contact the executor.
        val executorAddress = if (executorRef.address != null) {
          executorRef.address
        } else {
          context.senderAddress
        }
        logTrace(s"Registered executor $executorRef ($executorAddress) with ID $executorId")

        totalRegisteredExecutors.addAndGet(1)
        val data = new ExecutorData(executorRef)
        // This must be synchronized because variables mutated
        // in this block are read when requesting executors
        GlutenDriverEndpoint.this.synchronized {
          executorDataMap.put(executorId, data)
        }
        logTrace(s"Executor size ${executorDataMap.size()}")
        // Note: some tests expect the reply to come after we put the executor in the map
        context.reply(true)
      }

  }

  override def onStart(): Unit = {
    logTrace("Start GlutenDriverEndpoint.")
  }
}

object GlutenDriverEndpoint {
  lazy val glutenDriverEndpoint: GlutenDriverEndpoint = new GlutenDriverEndpoint
}

private class ExecutorData(val executorEndpointRef: RpcEndpointRef) {}

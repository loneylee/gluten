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

import io.glutenproject.backendsapi.BackendsApiManager
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.rpc.GlutenRpcMessages._
import org.apache.spark.util.ThreadUtils
import org.apache.spark.{SparkConf, SparkEnv}

import scala.util.{Failure, Success}

/**
 * Gluten executor endpoint.
 */
class GlutenExecutorEndpoint(val executorId: String, val conf: SparkConf)
  extends IsolatedRpcEndpoint
  with Logging {
  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  val executorEndpoint: RpcEndpointRef =
    rpcEnv.setupEndpoint(GlutenRpcConstants.GLUTEN_EXECUTOR_ENDPOINT_NAME, this)

  private val driverHost = conf.get(config.DRIVER_HOST_ADDRESS.key, "localhost")
  private val driverPort = conf.getInt(config.DRIVER_PORT.key, 7077)
  private val driverUrl =
    s"spark://${GlutenRpcConstants.GLUTEN_DRIVER_ENDPOINT_NAME}@$driverHost:$driverPort"

  @volatile private var driverEndpointRef: RpcEndpointRef = null

  override def onStart(): Unit = {
    logTrace(s"Start GlutenExecutorEndpoint")
    rpcEnv
      .asyncSetupEndpointRefByURI(driverUrl)
      .flatMap {
        ref =>
          // This is a very fast action so we can use "ThreadUtils.sameThread"
          driverEndpointRef = ref
          ref.ask[Boolean](GlutenRegisterExecutor(executorId, self))
      }(ThreadUtils.sameThread)
      .onComplete {
        case Success(_) => logTrace("Register GlutenExecutor success.")
        case Failure(e) => logError("Register GlutenExecutor error.", e)
      }(ThreadUtils.sameThread)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case GlutenOnExecutionStart(executionId) =>
      BackendsApiManager.getContextApiInstance().onExecutionStart(executionId)

    case GlutenOnExecutionEnd(executionId) =>
      BackendsApiManager.getContextApiInstance().onExecutionEnd(executionId)

    case e =>
      logError(s"Received unexpected message. $e")
  }
}

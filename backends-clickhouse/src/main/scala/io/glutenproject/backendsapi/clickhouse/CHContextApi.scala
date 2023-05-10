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

package io.glutenproject.backendsapi.clickhouse

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.ContextApi
import io.glutenproject.execution.CHBroadcastBuildSideRDD
import io.glutenproject.vectorized.{CHNativeExpressionEvaluator, JniLibLoader}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.{Set, TimeZone}

class CHContextApi extends ContextApi with Logging {

  override def initialize(conf: SparkConf): Unit = {
    val libPath = conf.get(GlutenConfig.GLUTEN_LIB_PATH, StringUtils.EMPTY)
    if (StringUtils.isBlank(libPath)) {
      throw new IllegalArgumentException(
        "Please set spark.gluten.sql.columnar.libpath to enable clickhouse backend")
    }
    // Path based load. Ignore all other loadees.
    JniLibLoader.loadFromPath(libPath, true)

    // Add configs
    conf.set(
      s"${CHBackendSettings.getBackendConfigPrefix()}.runtime_config.timezone",
      conf.get("spark.sql.session.timeZone", TimeZone.getDefault.getID))
    val initKernel = new CHNativeExpressionEvaluator()
    initKernel.initNative(conf)
  }

  override def shutdown(): Unit = {
    val kernel = new CHNativeExpressionEvaluator()
    kernel.finalizeNative()
  }

  override def onExecutionStart(executionId: String): Unit = {
    if (executionId != null
      && !CHContextApi.executionResourceRelation.containsKey(executionId)) {
      CHContextApi.executionResourceRelation
        .computeIfAbsent(executionId, _ => new util.HashSet[String]())
    } else {
      logWarning(s"Execution Id is null. Resources maybe not clean after execution end.")
    }
  }

  override def onExecutionEnd(executionId: String): Unit = {
    if (executionId == null) {
      logWarning(s"Execution id ${executionId} not found. Can't clean resource right now")
    } else {
      val resources = CHContextApi.executionResourceRelation.get(executionId)
      if (resources != null) {
        // clean broadcast hash data
        resources.forEach(resource_id =>
          CHBroadcastBuildSideRDD.buildSideRelationCache.invalidate(resource_id))
        CHContextApi.executionResourceRelation.remove(executionId)
      }
    }
  }

  override def collectExecutionBroadcastHashTableId(
      executionId: String,
      buildHashTableId: String): Unit = {
    if (executionId != null) {
      if (!CHContextApi.executionResourceRelation.containsKey(executionId)) {
        CHContextApi.executionResourceRelation
          .computeIfAbsent(executionId, _ => new util.HashSet[String]())
      }
      CHContextApi.executionResourceRelation.get(executionId).add(buildHashTableId)
    } else {
      logWarning(
        s"Can't not trace broadcast hash table data ${buildHashTableId}" +
          s" because execution id is null." +
          s" Will clean up until expire time.")
    }
  }
}

object CHContextApi extends Logging {
  // key: executionId, value: resourceIds
  private val executionResourceRelation = new ConcurrentHashMap[String, Set[String]]()

}

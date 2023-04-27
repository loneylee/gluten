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
import io.glutenproject.vectorized.{CHNativeExpressionEvaluator, JniLibLoader, StorageJoinRelease}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts

import org.apache.commons.lang3.StringUtils
import org.sparkproject.guava.cache.{Cache, CacheBuilder, CacheLoader, RemovalListener, RemovalNotification}

import java.util
import java.util.{Set, TimeZone}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._

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
      "spark.gluten.timezone",
      conf.get("spark.sql.session.timeZone", TimeZone.getDefault.getID))
    val initKernel = new CHNativeExpressionEvaluator()
    initKernel.initNative(conf)
  }

  override def shutdown(): Unit = {
    val kernel = new CHNativeExpressionEvaluator()
    kernel.finalizeNative()
  }

  override def onExecutionEnd(executionId: String): Unit = {
    if (CHContextApi.executionResourceRelation.containsKey(executionId)) {
      CHContextApi.executionResourceCache.invalidate(executionId)
    } else {
      logError(s"executionId not found: $executionId")
    }
  }

  override def collectExecutionBroadcastHashTableId(
                                                     executionId: String,
                                                     buildHashTableId: String): Unit = {
    if (executionId != null) {
      CHContextApi.executionResourceRelation
        .computeIfAbsent(
          executionId, _ => {
            CHContextApi.executionResourceCache.put(executionId, "")
            new util.HashSet[String]()
          })
      CHContextApi.executionResourceRelation.get(executionId).add(buildHashTableId)
    } else {
      logWarning(s"Execution Id is null. Build hash table maybe not clean after execution end.")
    }
  }
}

object CHContextApi extends Logging {
  // key: executionId, value: resourceIds
  val executionResourceRelation = new ConcurrentHashMap[String, Set[String]]()

  // key: executionId, value: resourceIds
  val executionResourceCache: Cache[String, String] = CacheBuilder
    .newBuilder()
    .expireAfterAccess(3, TimeUnit.DAYS)
    .removalListener(
      new RemovalListener[String, String] {
        override def onRemoval(notification: RemovalNotification[String, String]): Unit = {
          val executionId = notification.getKey
          val release = new StorageJoinRelease()
          executionResourceRelation
            .getOrDefault(executionId, util.Collections.emptySet())
            .forEach(
              resourceId => {
                if (CHBroadcastBuildSideRDD.buildSideRelationCache.containsKey(resourceId)) {
                  CHBroadcastBuildSideRDD.buildSideRelationCache.remove(resourceId)
                  release.cleanBuildHashTable(resourceId)
                  log.trace(s"Clean build hash table $executionId success." +
                    s"Cache size now is ${CHBroadcastBuildSideRDD.buildSideRelationCache.size()}")
                }
              })

          executionResourceRelation.remove(executionId)
        }
      }
    )
    .build[String, String]()
}

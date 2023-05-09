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
package io.glutenproject.execution

import io.glutenproject.vectorized.StorageJoinRelease

import org.apache.spark.internal.Logging

import org.sparkproject.guava.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}

import java.util.concurrent.TimeUnit

object CHBroadcastBuildSideRDD extends Logging {

  // Use for controling to build bhj hash table once.
  val buildSideRelationCache: Cache[String, String] =
    CacheBuilder.newBuilder
      .expireAfterAccess(1, TimeUnit.DAYS)
      .removalListener(
        new RemovalListener[String, String] {
          override def onRemoval(notification: RemovalNotification[String, String]): Unit = {
            val release = new StorageJoinRelease()
            release.cleanBuildHashTable(notification.getKey, notification.getValue.toLong)
            log.trace(
              s"Clean build hash table ${notification.getKey} success." +
                s"Cache size now is ${buildSideRelationCache.size()}")
          }
        }
      )
      .build[String, String]()
}

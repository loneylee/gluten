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

import io.glutenproject.substrait.plan.PlanBuilder
import org.apache.spark.sql.execution.datasources.utils.MergeTreePartitionedFile

case class GlutenMergeTreePartition(
    index: Int,
    engine: String,
    database: String,
    table: String,
    tablePath: String,
    minParts: Long,
    maxParts: Long,
    plan: Array[Byte] = PlanBuilder.EMPTY_PLAN)
  extends BaseGlutenPartition {
  override def preferredLocations(): Array[String] = {
    Array.empty[String]
  }
}

case class NewGlutenMergeTreePartition(
    index: Int,
    engine: String,
    database: String,
    table: String,
    tablePath: String,
    orderByKey: String,
    primaryKey: String,
    partList: Array[MergeTreePartitionedFile],
    plan: Array[Byte] = PlanBuilder.EMPTY_PLAN)
  extends BaseGlutenPartition {
  override def preferredLocations(): Array[String] = {
    Array.empty[String]
  }
}

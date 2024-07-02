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
package org.apache.gluten.backendsapi

import org.apache.gluten.GlutenConfig
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.types.StructField

trait BackendSettingsApi {
  def supportFileFormatRead(
      format: ReadFileFormat,
      fields: Array[StructField],
      partTable: Boolean,
      paths: Seq[String]): ValidationResult = ValidationResult.ok
  def supportWriteFilesExec(
      format: FileFormat,
      fields: Array[StructField],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String]): ValidationResult = ValidationResult.ok
  def supportNativeWrite(fields: Array[StructField]): Boolean = true
  def supportNativeMetadataColumns(): Boolean = false
  def supportNativeRowIndexColumn(): Boolean = false
  def supportNativeInputFileRelatedExpr(): Boolean = false

  def supportExpandExec(): Boolean = false
  def supportSortExec(): Boolean = false
  def supportSortMergeJoinExec(): Boolean = true
  def supportWindowExec(windowFunctions: Seq[NamedExpression]): Boolean = {
    false
  }
  def supportWindowGroupLimitExec(rankLikeFunction: Expression): Boolean = {
    false
  }
  def supportColumnarShuffleExec(): Boolean = {
    GlutenConfig.getConf.enableColumnarShuffle
  }
  def enableJoinKeysRewrite(): Boolean = true
  def supportHashBuildJoinTypeOnLeft: JoinType => Boolean = {
    case _: InnerLike | RightOuter | FullOuter => true
    case _ => false
  }
  def supportHashBuildJoinTypeOnRight: JoinType => Boolean = {
    case _: InnerLike | LeftOuter | FullOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
    case _ => false
  }
  def supportStructType(): Boolean = false
  def fallbackOnEmptySchema(plan: SparkPlan): Boolean = false

  // Whether to fallback aggregate at the same time if its empty-output child is fallen back.
  def fallbackAggregateWithEmptyOutputChild(): Boolean = false

  def disableVanillaColumnarReaders(conf: SparkConf): Boolean =
    !conf.getBoolean(
      GlutenConfig.VANILLA_VECTORIZED_READERS_ENABLED.key,
      GlutenConfig.VANILLA_VECTORIZED_READERS_ENABLED.defaultValue.get)

  def recreateJoinExecOnFallback(): Boolean = false

  /**
   * A shuffle key may be an expression. We would add a projection for this expression shuffle key
   * and make it into a new column which the shuffle will refer to. But we need to remove it from
   * the result columns from the shuffle.
   */
  def supportShuffleWithProject(outputPartitioning: Partitioning, child: SparkPlan): Boolean = false
  def excludeScanExecFromCollapsedStage(): Boolean = false
  def rescaleDecimalArithmetic: Boolean = false

  /**
   * Whether to replace sort agg with hash agg., e.g., sort agg will be used in spark's planning for
   * string type input.
   */
  def replaceSortAggWithHashAgg: Boolean = GlutenConfig.getConf.forceToUseHashAgg

  /** Get the config prefix for each backend */
  def getBackendConfigPrefix: String

  def allowDecimalArithmetic: Boolean = true

  /**
   * After https://github.com/apache/spark/pull/36698, every arithmetic should report the accurate
   * result decimal type and implement `CheckOverflow` by itself. <p/> Regardless of whether there
   * is 36698 or not, this option is used to indicate whether to transform `CheckOverflow`. `false`
   * means the backend will implement `CheckOverflow` by default and no need to transform it.
   */
  def transformCheckOverflow: Boolean = true

  def shuffleSupportedCodec(): Set[String]

  def needOutputSchemaForPlan(): Boolean = false

  /** Apply necessary conversions before passing to native side */
  def resolveNativeConf(nativeConf: java.util.Map[String, String]): Unit = {}

  def insertPostProjectForGenerate(): Boolean = false

  def skipNativeCtas(ctas: CreateDataSourceTableAsSelectCommand): Boolean = false

  def skipNativeInsertInto(insertInto: InsertIntoHadoopFsRelationCommand): Boolean = false

  def alwaysFailOnMapExpression(): Boolean = false

  def requiredChildOrderingForWindow(): Boolean = false

  def requiredChildOrderingForWindowGroupLimit(): Boolean = false

  def staticPartitionWriteOnly(): Boolean = false

  def supportTransformWriteFiles: Boolean = false

  def requiredInputFilePaths(): Boolean = false

  // TODO: Move this to test settings as used in UT only.
  def requireBloomFilterAggMightContainJointFallback(): Boolean = true

  def enableNativeWriteFiles(): Boolean

  def enableNativeArrowReadFiles(): Boolean = false

  def shouldRewriteCount(): Boolean = false

  def supportCartesianProductExec(): Boolean = false

  def supportBroadcastNestedJoinJoinType: JoinType => Boolean = {
    case _: InnerLike | LeftOuter | RightOuter => true
    case _ => false
  }

  def supportSampleExec(): Boolean = false

  /** Merge two phases hash based aggregate if need */
  def mergeTwoPhasesHashBaseAggregateIfNeed(): Boolean = false

  def supportColumnarArrowUdf(): Boolean = false

  def generateHdfsConfForLibhdfs(): Boolean = false

  def needPreComputeRangeFrameBoundary(): Boolean = false
}

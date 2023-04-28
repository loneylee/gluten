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

import io.glutenproject.backendsapi.SparkPlanExecApi
import io.glutenproject.execution._
import io.glutenproject.expression.{AliasBaseTransformer, AliasTransformer, ExpressionTransformer}
import io.glutenproject.expression.CHSha1Transformer
import io.glutenproject.expression.CHSha2Transformer
import io.glutenproject.sql.shims.SparkShimLoader
import io.glutenproject.vectorized.{CHBlockWriterJniWrapper, CHColumnarBatchSerializer}

import org.apache.spark.{ShuffleDependency, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.shuffle.utils.CHShuffleUtil
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.ColumnarAQEShuffleReadExec
import org.apache.spark.sql.execution.datasources.ColumnarToFakeRowStrategy
import org.apache.spark.sql.execution.datasources.GlutenColumnarRules.NativeWritePostRule
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.execution.datasources.v1.ClickHouseFileIndex
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.ClickHouseScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BuildSideRelation, ClickHouseBuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.utils.CHExecUtil
import org.apache.spark.sql.extension.ClickHouseAnalysis
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer

class CHSparkPlanExecApi extends SparkPlanExecApi {

  /**
   * Generate GlutenColumnarToRowExecBase.
   *
   * @param child
   * @return
   */
  override def genColumnarToRowExec(child: SparkPlan): GlutenColumnarToRowExecBase = {
    CHColumnarToRowExec(child);
  }

  /**
   * Generate RowToColumnarExec.
   *
   * @param child
   * @return
   */
  override def genRowToColumnarExec(child: SparkPlan): GlutenRowToColumnarExec = {
    new RowToCHNativeColumnarExec(child)
  }

  /**
   * Generate FilterExecTransformer.
   *
   * @param condition
   *   : the filter condition
   * @param child
   *   : the chid of FilterExec
   * @return
   *   the transformer of FilterExec
   */
  override def genFilterExecTransformer(
      condition: Expression,
      child: SparkPlan): FilterExecBaseTransformer = {
    child match {
      case scan: FileSourceScanExec if scan.relation.location.isInstanceOf[ClickHouseFileIndex] =>
        CHFilterExecTransformer(condition, child)
      case scan: BatchScanExec if scan.batch.isInstanceOf[ClickHouseScan] =>
        CHFilterExecTransformer(condition, child)
      case _ =>
        FilterExecTransformer(condition, child)
    }
  }

  /** Generate HashAggregateExecTransformer. */
  override def genHashAggregateExecTransformer(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): HashAggregateExecBaseTransformer =
    CHHashAggregateExecTransformer(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      child)

  /** Generate ShuffledHashJoinExecTransformer. */
  def genShuffledHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformer =
    CHShuffledHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isSkewJoin)

  /** Generate BroadcastHashJoinExecTransformer. */
  def genBroadcastHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isNullAwareAntiJoin: Boolean = false): BroadcastHashJoinExecTransformer =
    CHBroadcastHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isNullAwareAntiJoin)

  /**
   * Generate Alias transformer.
   *
   * @param child
   *   : The computation being performed
   * @param name
   *   : The name to be associated with the result of computing.
   * @param exprId
   * @param qualifier
   * @param explicitMetadata
   * @return
   *   a transformer for alias
   */
  def genAliasTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): AliasBaseTransformer =
    new AliasTransformer(substraitExprName, child, original)

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  // scalastyle:off argcount
  override def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      childOutputAttributes: Seq[Attribute],
      projectOutputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric]
  ): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    CHExecUtil.genShuffleDependency(
      rdd,
      childOutputAttributes,
      projectOutputAttributes,
      newPartitioning,
      serializer,
      writeMetrics,
      metrics
    )
  }
  // scalastyle:on argcount

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  override def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V] = {
    CHShuffleUtil.genColumnarShuffleWriter(parameters)
  }

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def createColumnarBatchSerializer(
      schema: StructType,
      readBatchNumRows: SQLMetric,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): Serializer = {
    new CHColumnarBatchSerializer(readBatchNumRows, numOutputRows, dataSize)
  }

  /** Create broadcast relation for BroadcastExchangeExec */
  override def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation = {
    val hashedRelationBroadcastMode = mode.asInstanceOf[HashedRelationBroadcastMode]
    val (newChild, newOutput, newBuildKeys) =
      if (
        hashedRelationBroadcastMode.key
          .forall(k => k.isInstanceOf[AttributeReference] || k.isInstanceOf[BoundReference])
      ) {
        (child, child.output, Seq.empty[Expression])
      } else {
        // pre projection in case of expression join keys
        val buildKeys = hashedRelationBroadcastMode.key
        val appendedProjections = new ArrayBuffer[NamedExpression]()
        val preProjectionBuildKeys = buildKeys.zipWithIndex.map {
          case (e, idx) =>
            e match {
              case b: BoundReference => child.output(b.ordinal)
              case o: Expression =>
                val newExpr = Alias(o, "col_" + idx)()
                appendedProjections += newExpr
                newExpr
            }
        }

        val newChild = child match {
          case wt: WholeStageTransformerExec =>
            wt.withNewChildren(
              Seq(ProjectExecTransformer(child.output ++ appendedProjections.toSeq, wt.child)))
          case w: WholeStageCodegenExec =>
            w.withNewChildren(Seq(ProjectExec(child.output ++ appendedProjections.toSeq, w.child)))
          case c: CoalesceBatchesExec =>
            // when aqe is open
            // TODO: remove this after pushdowning preprojection
            WholeStageTransformerExec(
              ProjectExecTransformer(child.output ++ appendedProjections.toSeq, c))(
              ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet())
          case columnarAQEShuffleReadExec: ColumnarAQEShuffleReadExec =>
            // when aqe is open
            // TODO: remove this after pushdowning preprojection
            WholeStageTransformerExec(
              ProjectExecTransformer(
                child.output ++ appendedProjections.toSeq,
                columnarAQEShuffleReadExec))(
              ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet())
          case r2c: RowToCHNativeColumnarExec =>
            WholeStageTransformerExec(
              ProjectExecTransformer(child.output ++ appendedProjections.toSeq, r2c))(
              ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet()
            )
        }
        (
          newChild,
          (child.output ++ appendedProjections.toSeq).map(_.toAttribute),
          preProjectionBuildKeys)
      }
    val countsAndBytes = newChild
      .executeColumnar()
      .mapPartitions {
        iter =>
          var _numRows: Long = 0

          // Use for reading bytes array from block
          val blockNativeWriter = new CHBlockWriterJniWrapper()
          while (iter.hasNext) {
            val batch = iter.next
            blockNativeWriter.write(batch)
            _numRows += batch.numRows
          }
          Iterator((_numRows, blockNativeWriter.collectAsByteArray()))
      }
      .collect

    val batches = countsAndBytes.map(_._2)
    val rawSize = batches.map(_.length).sum
    if (rawSize >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES) {
      throw new SparkException(
        s"Cannot broadcast the table that is larger than 8GB: ${rawSize >> 30} GB")
    }
    numOutputRows += countsAndBytes.map(_._1).sum
    dataSize += rawSize
    ClickHouseBuildSideRelation(mode, newOutput, batches, newBuildKeys)
  }

  /**
   * Generate extended DataSourceV2 Strategies. Currently only for ClickHouse backend.
   *
   * @return
   */
  override def genExtendedDataSourceV2Strategies(): List[SparkSession => Strategy] = {
    List.empty
  }

  /**
   * Generate extended Analyzers. Currently only for ClickHouse backend.
   *
   * @return
   */
  override def genExtendedAnalyzers(): List[SparkSession => Rule[LogicalPlan]] = {
    List(spark => new ClickHouseAnalysis(spark, spark.sessionState.conf))
  }

  /**
   * Generate extended columnar pre-rules.
   *
   * @return
   */
  override def genExtendedColumnarPreRules(): List[SparkSession => Rule[SparkPlan]] = List()

  /**
   * Generate extended columnar post-rules.
   *
   * @return
   */
  override def genExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] =
    List(spark => NativeWritePostRule(spark))

  /**
   * Generate extended Strategies.
   *
   * @return
   */
  override def genExtendedStrategies(): List[SparkSession => Strategy] =
    List(ColumnarToFakeRowStrategy)

  /** Generate an ExpressionTransformer to transform Sha2 expression. */
  override def genSha2Transformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Sha2): ExpressionTransformer = {
    new CHSha2Transformer(substraitExprName, left, right, original)
  }

  /** Generate an ExpressionTransformer to transform Sha1 expression. */
  override def genSha1Transformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Sha1): ExpressionTransformer = {
    new CHSha1Transformer(substraitExprName, child, original)
  }

  /**
   * Generate an BasicScanExecTransformer to transfrom hive table scan. Currently only for CH
   * backend.
   * @param child
   * @return
   */
  override def genHiveTableScanExecTransformer(child: SparkPlan): BasicScanExecTransformer = {
    if (!child.getClass.getSimpleName.equals("HiveTableScanExec")) {
      return null
    }
    var hiveTableRelation = null.asInstanceOf[HiveTableRelation]
    var partitionPruningPred = null.asInstanceOf[Seq[Expression]]
    var planOutput = null.asInstanceOf[Seq[Attribute]]
    var sparkSession = null.asInstanceOf[SparkSession]
    child.getClass.getDeclaredFields.foreach(
      f => {
        f.setAccessible(true)
        f.getName match {
          case "relation" =>
            hiveTableRelation = f.get(child).asInstanceOf[HiveTableRelation]
          case "partitionPruningPred" =>
            partitionPruningPred = f.get(child).asInstanceOf[Seq[Expression]]
          case "output" =>
            planOutput = f.get(child).asInstanceOf[Seq[Attribute]]
          case "sparkSession" =>
            sparkSession = f.get(child).asInstanceOf[SparkSession]
          case _ =>
        }
      })
    if (
      hiveTableRelation == null
      || partitionPruningPred == null
      || planOutput == null
      || sparkSession == null
    ) {
      return null
    }
    val tableMeta = hiveTableRelation.tableMeta
    val fileIndex = new InMemoryFileIndex(
      sparkSession,
      Seq.apply(new Path(tableMeta.location)),
      Map.empty,
      Option.apply(tableMeta.schema))
    val output =
      if (planOutput.nonEmpty) planOutput.asInstanceOf[Seq[AttributeReference]]
      else hiveTableRelation.dataCols
    var hasComplexType = false
    val outputFieldTypes = new ArrayBuffer[StructField]()
    output.foreach(
      x => {
        hasComplexType = if (!hasComplexType) {
          x.dataType.isInstanceOf[StructType] ||
          x.dataType.isInstanceOf[MapType] ||
          x.dataType.isInstanceOf[ArrayType]
        } else hasComplexType
        outputFieldTypes.append(StructField(x.name, x.dataType))
      })
    tableMeta.storage.inputFormat match {
      case Some("org.apache.hadoop.mapred.TextInputFormat") =>
        tableMeta.storage.serde match {
          case Some("org.openx.data.jsonserde.JsonSerDe") =>
            val scan = JsonScan(
              sparkSession,
              fileIndex,
              tableMeta.schema,
              StructType(outputFieldTypes.toArray),
              tableMeta.partitionSchema,
              new CaseInsensitiveStringMap(JavaConverters.mapAsJavaMap(tableMeta.properties)),
              Array.empty,
              partitionPruningPred,
              Seq.empty
            )
            new BatchScanExecTransformer(output, scan, Seq.empty, Seq.empty)
          case _ =>
            val scan = SparkShimLoader.getSparkShims.getTextScan(
              sparkSession,
              fileIndex,
              tableMeta.schema,
              StructType(outputFieldTypes.toArray),
              tableMeta.partitionSchema,
              new CaseInsensitiveStringMap(JavaConverters.mapAsJavaMap(tableMeta.properties)),
              partitionPruningPred,
              Seq.empty
            )
            if (!hasComplexType) {
              new BatchScanExecTransformer(
                output,
                scan,
                Seq.empty,
                Seq.empty,
                tableMeta.storage.properties,
                tableMeta.dataSchema)
            } else {
              null
            }
        }
      case _ => null
    }
  }
}

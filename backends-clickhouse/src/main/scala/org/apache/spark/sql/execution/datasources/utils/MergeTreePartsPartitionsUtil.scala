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
package org.apache.spark.sql.execution.datasources.utils

import io.glutenproject.backendsapi.clickhouse.CHBackendSettings
import io.glutenproject.execution.{GlutenMergeTreePartition, NewGlutenMergeTreePartition}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts
import org.apache.spark.sql.execution.datasources.v2.clickhouse.table.ClickHouseTableV2
import org.apache.spark.util.SparkResourceUtil
import org.apache.spark.util.collection.BitSet

import scala.collection.mutable.ArrayBuffer

case class MergeTreePartitionedFile(
    name: String,
    path: String,
    start: Long,
    length: Long,
    bytesOnDisk: Long) {
  override def toString: String = {
    s"pat name: $name, range: $start-${start + length}"
  }
}

object MergeTreePartsPartitionsUtil extends Logging {

  def getPartsPartitions(
      sparkSession: SparkSession,
      table: ClickHouseTableV2): Seq[InputPartition] = {
    val partsFiles = table.listFiles()

    val partitions = new ArrayBuffer[InputPartition]
    val (database, tableName) = if (table.catalogTable.isDefined) {
      (table.catalogTable.get.identifier.database.get, table.catalogTable.get.identifier.table)
    } else {
      // for file_format.`file_path`
      ("default", "file_format")
    }
    val engine = table.snapshot.metadata.configuration.get("engine").get
    // TODO: remove `substring`
    val tablePath = table.deltaLog.dataPath.toString.substring(6)
    var currentMinPartsNum = -1L
    var currentMaxPartsNum = -1L
    var currentSize = 0L
    var currentFileCnt = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentMinPartsNum > 0L && currentMaxPartsNum >= currentMinPartsNum) {
        val newPartition = GlutenMergeTreePartition(
          partitions.size,
          engine,
          database,
          tableName,
          tablePath,
          currentMinPartsNum,
          currentMaxPartsNum + 1)
        partitions += newPartition
      }
      currentMinPartsNum = -1L
      currentMaxPartsNum = -1L
      currentSize = 0
      currentFileCnt = 0L
    }

    val totalCores = SparkResourceUtil.getTotalCores(sparkSession.sessionState.conf)
    val fileCntPerPartition = math.ceil((partsFiles.size * 1.0) / totalCores).toInt
    val fileCntThreshold = sparkSession.sessionState.conf
      .getConfString(
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD,
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD_DEFAULT
      )
      .toInt

    if (fileCntThreshold > 0 && fileCntPerPartition > fileCntThreshold) {
      // generate `Seq[InputPartition]` by file count
      // Assign files to partitions using "Next Fit Decreasing"
      partsFiles.foreach {
        parts =>
          if (currentFileCnt >= fileCntPerPartition) {
            closePartition()
          }
          // Add the given file to the current partition.
          currentFileCnt += 1
          if (currentMinPartsNum == -1L) {
            currentMinPartsNum = parts.minBlockNumber
          }
          currentMaxPartsNum = parts.maxBlockNumber
      }
    } else {
      // generate `Seq[InputPartition]` by file size
      val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
      val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
      logInfo(
        s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
          s"open cost is considered as scanning $openCostInBytes bytes.")
      // Assign files to partitions using "Next Fit Decreasing"
      partsFiles.foreach {
        parts =>
          if (currentSize + parts.bytesOnDisk > maxSplitBytes) {
            closePartition()
          }
          // Add the given file to the current partition.
          currentSize += parts.bytesOnDisk + openCostInBytes
          if (currentMinPartsNum == -1L) {
            currentMinPartsNum = parts.minBlockNumber
          }
          currentMaxPartsNum = parts.maxBlockNumber
      }
    }
    closePartition()
    partitions
  }

  def getMergeTreePartsPartitions(
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      sparkSession: SparkSession,
      table: ClickHouseTableV2,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean): Seq[InputPartition] = {
    val partsFiles = table.listFiles()

    val partitions = new ArrayBuffer[InputPartition]
    val (database, tableName) = if (table.catalogTable.isDefined) {
      (table.catalogTable.get.identifier.database.get, table.catalogTable.get.identifier.table)
    } else {
      // for file_format.`file_path`
      ("default", "file_format")
    }
    val engine = table.snapshot.metadata.configuration.get("engine").get
    // TODO: remove `substring`
    val tablePath = table.deltaLog.dataPath.toString.substring(6)

    val orderByKeyOption = table.orderByKeyOption
    val primaryKeyOption = table.primaryKeyOption

    // bucket table
    if (table.bucketOption.isDefined) {
      if (bucketedScan) {
        genBucketedInputPartitionSeq(
          engine,
          database,
          tableName,
          tablePath,
          table.bucketOption.get,
          partsFiles,
          partitions,
          optionalBucketSet,
          optionalNumCoalescedBuckets,
          orderByKeyOption,
          primaryKeyOption,
          sparkSession
        )
      } else {
        genInputPartitionSeqWithBucketTable(
          engine,
          database,
          tableName,
          tablePath,
          table.bucketOption.get,
          partsFiles,
          partitions,
          optionalBucketSet,
          sparkSession)
      }
    } else {
      genInputPartitionSeq(
        engine,
        database,
        tableName,
        tablePath,
        partsFiles,
        selectedPartitions,
        partitions,
        orderByKeyOption,
        primaryKeyOption,
        sparkSession
      )
    }
    partitions
  }

  /** Generate bucket partition */
  def genBucketedInputPartitionSeq(
      engine: String,
      database: String,
      tableName: String,
      tablePath: String,
      bucketSpec: BucketSpec,
      partsFiles: Seq[AddMergeTreeParts],
      partitions: ArrayBuffer[InputPartition],
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      orderByKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      sparkSession: SparkSession): Unit = {
    val bucketGroupParts = partsFiles.groupBy(p => Integer.parseInt(p.bucketNum))

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      bucketGroupParts.filter(f => bucketSet.get(f._1))
    } else {
      bucketGroupParts
    }

    if (optionalNumCoalescedBuckets.isDefined) {
      throw new UnsupportedOperationException(
        "Currently CH backend can't support coalesced buckets.")
    }

    val orderByKey =
      if (orderByKeyOption.isDefined && orderByKeyOption.get.nonEmpty) {
        orderByKeyOption.get.mkString(",")
      } else "tuple()"

    val primaryKey =
      if (
        !orderByKey.equals("tuple()") && primaryKeyOption.isDefined &&
          primaryKeyOption.get.nonEmpty
      ) {
        primaryKeyOption.get.mkString(",")
      } else ""

    Seq.tabulate(bucketSpec.numBuckets) {
      bucketId =>
        val currBucketParts = prunedFilesGroupedToBuckets.getOrElse(bucketId, Seq.empty)
        if (!currBucketParts.isEmpty) {
          var currTableName = tableName + "_" + currBucketParts(0).bucketNum
          var currTablePath = tablePath + "/" + currBucketParts(0).bucketNum
          val partList = currBucketParts.map(
            part => {
              MergeTreePartitionedFile(
                part.name.split("/").apply(1),
                part.path,
                0,
                part.marks,
                part.bytesOnDisk)
            }).toArray
          val newPartition = NewGlutenMergeTreePartition(
            partitions.size,
            engine,
            database,
            currTableName,
            currTablePath,
            orderByKey,
            primaryKey,
            partList
          )
          partitions += newPartition
        }
    }
  }

  /** Generate partition from the bucket table */
  def genInputPartitionSeqWithBucketTable(
      engine: String,
      database: String,
      tableName: String,
      tablePath: String,
      bucketSpec: BucketSpec,
      partsFiles: Seq[AddMergeTreeParts],
      partitions: ArrayBuffer[InputPartition],
      optionalBucketSet: Option[BitSet],
      sparkSession: SparkSession): Unit = {
    val bucketGroupParts = partsFiles.groupBy(p => Integer.parseInt(p.bucketNum))

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      bucketGroupParts.filter(f => bucketSet.get(f._1))
    } else {
      bucketGroupParts
    }

    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes

    def closePartition(
        currTableName: String,
        currTablePath: String,
        currentMinPartsNum: Long,
        currentMaxPartsNum: Long): Unit = {
      if (currentMaxPartsNum >= currentMinPartsNum) {
        val newPartition = GlutenMergeTreePartition(
          partitions.size,
          engine,
          database,
          currTableName,
          currTablePath,
          currentMinPartsNum,
          currentMaxPartsNum + 1)
        partitions += newPartition
      }
    }

    Seq.tabulate(bucketSpec.numBuckets) {
      bucketId =>
        val currBucketParts = prunedFilesGroupedToBuckets.getOrElse(bucketId, Seq.empty)
        if (!currBucketParts.isEmpty) {
          var currentMinPartsNum = Long.MaxValue
          var currentMaxPartsNum = -1L
          var currentSize = 0L
          var currTableName = tableName + "_" + currBucketParts(0).bucketNum
          var currTablePath = tablePath + "/" + currBucketParts(0).bucketNum

          currBucketParts.foreach {
            parts =>
              if (currentSize + parts.bytesOnDisk > maxSplitBytes) {
                closePartition(currTableName, currTablePath, currentMinPartsNum, currentMaxPartsNum)
                currentMinPartsNum = Long.MaxValue
                currentMaxPartsNum = -1L
                currentSize = 0L
              }
              // Add the given file to the current partition.
              currentSize += parts.bytesOnDisk + openCostInBytes
              if (currentMinPartsNum >= parts.minBlockNumber) {
                currentMinPartsNum = parts.minBlockNumber
              }
              if (currentMaxPartsNum <= parts.maxBlockNumber) {
                currentMaxPartsNum = parts.maxBlockNumber
              }
          }
          closePartition(currTableName, currTablePath, currentMinPartsNum, currentMaxPartsNum)
        }
    }
  }

  /** Generate partition from the non-bucket table */
  def genNonBuckedInputPartitionSeq(
      engine: String,
      database: String,
      tableName: String,
      tablePath: String,
      partsFiles: Seq[AddMergeTreeParts],
      partitions: ArrayBuffer[InputPartition],
      sparkSession: SparkSession): Unit = {
    var currentMinPartsNum = -1L
    var currentMaxPartsNum = -1L
    var currentSize = 0L
    var currentFileCnt = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentMinPartsNum > 0L && currentMaxPartsNum >= currentMinPartsNum) {
        val newPartition = GlutenMergeTreePartition(
          partitions.size,
          engine,
          database,
          tableName,
          tablePath,
          currentMinPartsNum,
          currentMaxPartsNum + 1)
        partitions += newPartition
      }
      currentMinPartsNum = -1L
      currentMaxPartsNum = -1L
      currentSize = 0
      currentFileCnt = 0L
    }

    val totalCores = SparkResourceUtil.getTotalCores(sparkSession.sessionState.conf)
    val fileCntPerPartition = math.ceil((partsFiles.size * 1.0) / totalCores).toInt
    val fileCntThreshold = sparkSession.sessionState.conf
      .getConfString(
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD,
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD_DEFAULT
      )
      .toInt

    if (fileCntThreshold > 0 && fileCntPerPartition > fileCntThreshold) {
      // generate `Seq[InputPartition]` by file count
      // Assign files to partitions using "Next Fit Decreasing"
      partsFiles.foreach {
        parts =>
          if (currentFileCnt >= fileCntPerPartition) {
            closePartition()
          }
          // Add the given file to the current partition.
          currentFileCnt += 1
          if (currentMinPartsNum == -1L) {
            currentMinPartsNum = parts.minBlockNumber
          }
          currentMaxPartsNum = parts.maxBlockNumber
      }
    } else {
      // generate `Seq[InputPartition]` by file size
      val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
      val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
      logInfo(
        s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
          s"open cost is considered as scanning $openCostInBytes bytes.")
      // Assign files to partitions using "Next Fit Decreasing"
      partsFiles.foreach {
        parts =>
          if (currentSize + parts.bytesOnDisk > maxSplitBytes) {
            closePartition()
          }
          // Add the given file to the current partition.
          currentSize += parts.bytesOnDisk + openCostInBytes
          if (currentMinPartsNum == -1L) {
            currentMinPartsNum = parts.minBlockNumber
          }
          currentMaxPartsNum = parts.maxBlockNumber
      }
    }
    closePartition()
  }

  def genInputPartitionSeq(
      engine: String,
      database: String,
      tableName: String,
      tablePath: String,
      partsFiles: Seq[AddMergeTreeParts],
      selectedPartitions: Array[PartitionDirectory],
      partitions: ArrayBuffer[InputPartition],
      orderByKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      sparkSession: SparkSession): Unit = {
    val selectedPartitionMap = selectedPartitions
      .flatMap(
        p => {
          p.files.map(
            f => {
              (f.getPath.toString, f)
            })
        })
      .toMap

    val selectPartsFiles = partsFiles.filter(part => selectedPartitionMap.contains(part.path))

    val total_marks = selectPartsFiles.map(p => p.marks).sum
    val totalCores = SparkResourceUtil.getTotalCores(sparkSession.sessionState.conf)
    val markCntPerPartition = math.ceil((total_marks * 1.0) / totalCores).toInt
    logInfo(s"Planning scan with bin packing, max mark: $markCntPerPartition")
    val splitFiles = selectPartsFiles.flatMap {
      part =>
        (0L until part.marks by markCntPerPartition).map {
          offset =>
            val remaining = part.marks - offset
            val size = if (remaining > markCntPerPartition) markCntPerPartition else remaining
            val end = if (offset + size == part.marks) part.marks else offset + size - 1
            MergeTreePartitionedFile(
              part.name,
              part.path,
              offset,
              end,
              (size * 1.0 / part.marks * part.bytesOnDisk).toLong)
        }
    }

    var currentSize = 0L
    val currentFiles = new ArrayBuffer[MergeTreePartitionedFile]

    val orderByKey =
      if (orderByKeyOption.isDefined && orderByKeyOption.get.nonEmpty) {
        orderByKeyOption.get.mkString(",")
      } else "tuple()"

    val primaryKey =
      if (
        !orderByKey.equals("tuple()") && primaryKeyOption.isDefined &&
        primaryKeyOption.get.nonEmpty
      ) {
        primaryKeyOption.get.mkString(",")
      } else ""

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        val newPartition = NewGlutenMergeTreePartition(
          partitions.size,
          engine,
          database,
          tableName,
          tablePath,
          orderByKey,
          primaryKey,
          currentFiles.toArray
        )
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    // generate `Seq[InputPartition]` by file size
//    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
//    val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
//    logInfo(
//      s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
//        s"open cost is considered as scanning $openCostInBytes bytes.")
    // Assign files to partitions using "Next Fit Decreasing"
    splitFiles.foreach {
      parts =>
        if (currentSize + parts.length > markCntPerPartition) {
          closePartition()
        }
        // Add the given file to the current partition.
        currentSize += parts.length
        currentFiles += parts
    }
    closePartition()
  }
}

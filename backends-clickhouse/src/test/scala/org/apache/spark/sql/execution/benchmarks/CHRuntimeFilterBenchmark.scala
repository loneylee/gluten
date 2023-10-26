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
package org.apache.spark.sql.execution.benchmarks

import io.glutenproject.benchmarks.GenTPCDSTableScripts
import io.glutenproject.execution.BroadCastHashJoinContext

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.utils.CHExecUtil

import jodd.util.ThreadUtil.sleep

object CHRuntimeFilterBenchmark extends SqlBasedBenchmark with CHSqlBasedBenchmark with Logging {

  protected lazy val appName = "CHHashBuildBenchmark"
  protected lazy val thrdNum = "1"
  protected lazy val memorySize = "4G"
  protected lazy val offheapSize = "4G"

  override def getSparkSession: SparkSession = {

    val conf = getSparkcConf
      .setMaster(s"local[4]")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set("spark.gluten.sql.columnar.columnarToRow", "true")
      .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
      .set("spark.memory.offHeap.size", "8g")
      .set("spark.gluten.sql.validation.logLevel", "ERROR")
      .set("spark.gluten.sql.validation.printStackOnFailure", "true")
      // radically small threshold to force runtime bloom filter
      .set("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB")
      .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
      .set("spark.gluten.enabled", "true")
    SparkSession.builder.config(conf).getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val parquetTables =
      GenTPCDSTableScripts.genTPCDSParquetTables("tpcdsdb", "/data/tpcds-data-sf1/", "", "")

    for (sql <- parquetTables) {
      spark.sql(sql)
    }
    spark.sql("use tpcdsdb;")

    // /home/chang/test/tpch/parquet/s100/supplier 3 * 20 false
//    val (parquetDir, scanSchema, executedCnt) =
//      if (mainArgs.isEmpty) {
//        ("/data/tpch-data/parquet/lineitem", "l_orderkey,l_receiptdate", 5)
//      } else {
//        (mainArgs(0), mainArgs(1), mainArgs(2).toInt)
//      }
    val benchmark =
      new Benchmark(s"test", 100, output = output)
    Range(1, 2).map(
      num => {
        benchmark.addCase(s"build hash table with $num rows with $num hash tables", 5) {
          _ =>
            spark
              .sql("""
                     |with customer_total_return as
                     |(select sr_customer_sk as ctr_customer_sk
                     |,sr_store_sk as ctr_store_sk
                     |,sum(SR_FEE) as ctr_total_return
                     |from store_returns
                     |,date_dim
                     |where sr_returned_date_sk = d_date_sk
                     |and d_year =2000
                     |group by sr_customer_sk
                     |,sr_store_sk)
                     | select  c_customer_id
                     |from customer_total_return ctr1
                     |,store
                     |,customer
                     |where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
                     |from customer_total_return ctr2
                     |where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
                     |and s_store_sk = ctr1.ctr_store_sk
                     |and s_state = 'TN'
                     |and ctr1.ctr_customer_sk = c_customer_sk
                     |order by c_customer_id
                     | LIMIT 100 ;
                     |
                     |""".stripMargin)
              .collect()
        }
      })
    benchmark.run()
    sleep(1)
  }

  private def createBroadcastRelation(
      child: SparkPlan): (Array[Byte], Int, BroadCastHashJoinContext) = {
    val dataSize = SQLMetrics.createSizeMetric(spark.sparkContext, "size of files read")

    val countsAndBytes = child
      .executeColumnar()
      .mapPartitionsInternal(iter => CHExecUtil.toBytes(dataSize, iter))
      .collect()
    (
      countsAndBytes.flatMap(_._2),
      countsAndBytes.map(_._1).sum,
      BroadCastHashJoinContext(Seq(child.output.head), Inner, child.output, "")
    )
  }
}

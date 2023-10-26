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

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, rand, when}

import java.io.File

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseTPCHDecimalParquetSuite extends GlutenClickHouseTPCHAbstractSuite {

  override protected val resourcePath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
      .set("spark.gluten.supported.scala.udfs", "my_add")
      .set("spark.gluten.supported.hive.udfs", "my_add")
      .set("spark.sql.decimalOperations.allowPrecisionLoss", "false")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level", "debug")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_settings.query_plan_enable_optimizations",
        "false")
      .set("spark.gluten.sql.validation.logLevel", "INFO")
      .set("spark.gluten.sql.validation.printStackOnFailure", "true")
      .set("spark.sql.decimalOperations.allowPrecisionLoss", "false")
  }

  override protected val createNullableTables = true

  override protected def createTPCHNullableTables(): Unit = {
    // first process the parquet data to:
    // 1. make every column nullable in schema (optional rather than required)
    // 2. salt some null values randomly
    val saltedTablesPath = tablesPath + "-decimal-salted"
    withSQLConf(vanillaSparkConfs(): _*) {
      Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")
        .map(
          tableName => {
            val originTablePath = tablesPath + "/" + tableName
            spark.read.parquet(originTablePath).createTempView(tableName + "_ori")

            val sql = tableName match {
              case "customer" =>
                s"""
                   |select
                   |  c_custkey,c_name,c_address,c_nationkey,c_phone,cast(c_acctbal as decimal(38,18)),
                   |  c_mktsegment,c_comment
                   |from ${tableName}_ori""".stripMargin
              case "lineitem" =>
                s"""
                   |select
                   |  l_orderkey,l_partkey,l_suppkey,l_linenumber,cast(l_quantity as decimal(38,18)),
                   |  cast(l_extendedprice as decimal(38,18)),cast(l_discount as decimal(38,18)),
                   |  cast(l_tax as decimal(38,18)),l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,
                   |  l_shipinstruct,l_shipmode,l_comment
                   |from ${tableName}_ori """.stripMargin
              case "orders" =>
                s"""
                   |select
                   |  o_orderkey,o_custkey,o_orderstatus,cast(o_totalprice as decimal(38,18)),o_orderdate,
                   |  o_orderpriority,o_clerk,o_shippriority,o_comment
                   |from ${tableName}_ori
                   |""".stripMargin
              case "part" =>
                s"""
                   |select
                   |  p_partkey,p_name,p_mfgr,p_brand,p_type,p_size,p_container,
                   |  cast(p_retailprice as decimal(38,18)),p_comment
                   |from ${tableName}_ori
                   |""".stripMargin
              case "partsupp" =>
                s"""
                   |select
                   |  ps_partkey,ps_suppkey,ps_availqty,cast(ps_supplycost as decimal(38,18)),ps_comment
                   |from ${tableName}_ori
                   |""".stripMargin
              case "supplier" =>
                s"""
                   |select
                   |  s_suppkey,s_name,s_address,s_nationkey,s_phone,cast(s_acctbal as decimal(38,18)),s_comment
                   |from ${tableName}_ori
                   |""".stripMargin
              case _ => s"select * from ${tableName}_ori"
            }

            val df = spark.sql(sql).toDF()
            var salted_df: Option[DataFrame] = None
            for (c <- df.schema) {
              salted_df = Some((salted_df match {
                case Some(x) => x
                case None => df
              }).withColumn(c.name, when(rand() < 0.1, null).otherwise(col(c.name))))
            }

            val currentSaltedTablePath = saltedTablesPath + "/" + tableName
            val file = new File(currentSaltedTablePath)
            if (file.exists()) {
              file.delete()
            }

            salted_df.get.write.parquet(currentSaltedTablePath)
          })
    }

    val customerData = saltedTablesPath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS customer")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS customer (
                 | c_custkey    bigint,
                 | c_name       string,
                 | c_address    string,
                 | c_nationkey  bigint,
                 | c_phone      string,
                 | c_acctbal    decimal(38,18),
                 | c_mktsegment string,
                 | c_comment    string)
                 | USING PARQUET LOCATION '$customerData'
                 |""".stripMargin)

    val lineitemData = saltedTablesPath + "/lineitem"
    spark.sql(s"DROP TABLE IF EXISTS lineitem")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS lineitem (
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      decimal(38,18),
                 | l_extendedprice decimal(38,18),
                 | l_discount      decimal(38,18),
                 | l_tax           decimal(38,18),
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string)
                 | USING PARQUET LOCATION '$lineitemData'
                 |""".stripMargin)

    val nationData = saltedTablesPath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS nation")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS nation (
                 | n_nationkey bigint,
                 | n_name      string,
                 | n_regionkey bigint,
                 | n_comment   string)
                 | USING PARQUET LOCATION '$nationData'
                 |""".stripMargin)

    val regionData = saltedTablesPath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS region")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS region (
                 | r_regionkey bigint,
                 | r_name      string,
                 | r_comment   string)
                 | USING PARQUET LOCATION '$regionData'
                 |""".stripMargin)

    val ordersData = saltedTablesPath + "/orders"
    spark.sql(s"DROP TABLE IF EXISTS orders")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS orders (
                 | o_orderkey      bigint,
                 | o_custkey       bigint,
                 | o_orderstatus   string,
                 | o_totalprice    decimal(38,18),
                 | o_orderdate     date,
                 | o_orderpriority string,
                 | o_clerk         string,
                 | o_shippriority  bigint,
                 | o_comment       string)
                 | USING PARQUET LOCATION '$ordersData'
                 |""".stripMargin)

    val partData = saltedTablesPath + "/part"
    spark.sql(s"DROP TABLE IF EXISTS part")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS part (
                 | p_partkey     bigint,
                 | p_name        string,
                 | p_mfgr        string,
                 | p_brand       string,
                 | p_type        string,
                 | p_size        bigint,
                 | p_container   string,
                 | p_retailprice decimal(38,18),
                 | p_comment     string)
                 | USING PARQUET LOCATION '$partData'
                 |""".stripMargin)

    val partsuppData = saltedTablesPath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS partsupp")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS partsupp (
                 | ps_partkey    bigint,
                 | ps_suppkey    bigint,
                 | ps_availqty   bigint,
                 | ps_supplycost decimal(38,18),
                 | ps_comment    string)
                 | USING PARQUET LOCATION '$partsuppData'
                 |""".stripMargin)

    val supplierData = saltedTablesPath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS supplier")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS supplier (
                 | s_suppkey   bigint,
                 | s_name      string,
                 | s_address   string,
                 | s_nationkey bigint,
                 | s_phone     string,
                 | s_acctbal   decimal(38,18),
                 | s_comment   string)
                 | USING PARQUET LOCATION '$supplierData'
                 |""".stripMargin)

    val result = spark
      .sql(s"""
              | show tables;
              |""".stripMargin)
      .collect()
    assert(result.size == 16)
  }

  override protected def runTPCHQuery(
      queryNum: Int,
      tpchQueries: String = tpchQueries,
      queriesResults: String = queriesResults,
      compareResult: Boolean = true,
      noFallBack: Boolean = true)(customCheck: DataFrame => Unit): Unit = {
    val confName = "spark.gluten.sql.columnar.backend.ch." +
      "runtime_settings.query_plan_enable_optimizations"
    withSQLConf((confName, "true")) {
      compareTPCHQueryAgainstVanillaSpark(queryNum, tpchQueries, customCheck, noFallBack)
    }
    withSQLConf((confName, "false")) {
      compareTPCHQueryAgainstVanillaSpark(queryNum, tpchQueries, customCheck, noFallBack)
    }
  }

  test("collect_set") {
    val a = List(
//            ("spark.gluten.enabled", "false"), ("spark.sql.codegen.wholeStage", "false")
    )
    withSQLConf(a: _*) {
      val df = spark
        .sql(
          """
            |select split(a, ',', 1)  , b from (
            | select
            | l_orderkey a, size(collect_set(l_shipdate)) b from lineitem group by l_orderkey
            | ) tt
            |""".stripMargin
        )
      println(df.queryExecution.optimizedPlan)
      println(df.queryExecution.executedPlan)
      df.show()
    }
  }

  test("test2") {
    val a = List(
            ("spark.gluten.enabled", "false"), ("spark.sql.codegen.wholeStage", "false")
    )
    withSQLConf(a: _*) {
      val df = spark
        .sql(
          """
            | select sum(a) from (select 1 a
            | union all select cast('99999999999999999999.123456789012345678' as decimal(38,18)) a )
            |""".stripMargin
        )
      df.show()
    }
  }

  test("test") {
    val a = List(
//      ("spark.gluten.enabled", "false"), ("spark.sql.codegen.wholeStage", "false")
    )
    withSQLConf(a: _*) {
      val df = spark
        .sql(
          """
            | select sum(a) from (select cast('99999999999999999999.123456789012345678' as decimal(38,18)) a
            | union all select cast('99999999999999999999.123456789012345678' as decimal(38,18)) a )
            |""".stripMargin
        )
      df.show()
    }
  }

  test("test3") {
    val a = List(
            ("spark.gluten.enabled", "false"), ("spark.sql.codegen.wholeStage", "false")
    )
    withSQLConf(a: _*) {
      val df = spark
        .sql(
          """
            | select cast('7.921901' as float)
            |""".stripMargin
        )
      df.show()
    }
  }

  test("TPCH Q1") {
    runTPCHQuery(1)(df => df.show())
  }

  test("TPCH Q2") {
    runTPCHQuery(2)(df => df.show())
  }

  test("TPCH Q3") {
    runTPCHQuery(3)(df => df.show())
  }

  test("TPCH Q4") {
    runTPCHQuery(4)(df => df.show())
  }

  test("TPCH Q5") {
    val a = List(
//            ("spark.gluten.enabled", "false")
      // , ("spark.sql.codegen.wholeStage", "false")
    )
    withSQLConf(a: _*) {
      runTPCHQuery(5)(df => df.show())
    }

  }

  test("TPCH Q6") {
    runTPCHQuery(6)(df => df.show())
  }

  test("TPCH Q7") {
    runTPCHQuery(7)(df => df.show())
  }

  test("TPCH Q8") {
    runTPCHQuery(8)(df => df.show())
  }

  test("TPCH Q9") {
    runTPCHQuery(9)(df => df.show())
  }

  test("TPCH Q10") {
    runTPCHQuery(10)(df => df.show())
  }

  test("TPCH Q11") {
    runTPCHQuery(11)(df => df.show())
  }

  test("TPCH Q12") {
    runTPCHQuery(12)(df => df.show())
  }

  test("TPCH Q13") {
    runTPCHQuery(13)(df => df.show())
  }

  test("TPCH Q14") {
    runTPCHQuery(14)(df => df.show())
  }

  test("TPCH Q15") {
    runTPCHQuery(15)(df => df.show())
  }

  // see issue https://github.com/Kyligence/ClickHouse/issues/93
  test("TPCH Q16") {
    runTPCHQuery(16, noFallBack = false)(df => df.show())
  }

  test("TPCH Q17") {
    runTPCHQuery(17)(df => df.show())
  }

  test("TPCH Q18") {
    runTPCHQuery(18)(df => df.show())
  }

  test("TPCH Q19") {
    runTPCHQuery(19)(df => df.show())
  }

  test("TPCH Q20") {
    runTPCHQuery(20)(df => df.show())
  }

  test("TPCH Q21") {
    runTPCHQuery(21, noFallBack = false)(df => df.show())
  }

  // see issue https://github.com/Kyligence/ClickHouse/issues/93
  test("TPCH Q22") {
    runTPCHQuery(22)(df => df.show())
  }
}
// scalastyle:on line.size.limit

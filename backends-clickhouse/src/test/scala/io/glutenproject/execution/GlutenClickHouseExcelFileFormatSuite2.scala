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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types._

import jodd.util.ThreadUtil.sleep

class GlutenClickHouseExcelFileFormatSuite
  extends GlutenClickHouseFileFormatSuite
  with AdaptiveSparkPlanHelper {

  override protected def createTPCHNullableTables(): Unit = {}

  override protected def createTPCHNotNullTables(): Unit = {}

  protected val zenDataPath: String = rootPath + "csv-data" + "/zen"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.adaptive.enabled", "false")
//      .set("spark.sql.files.maxPartitionBytes", "100000000000")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_settings.date_time_input_format",
        "best_effort_us")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_settings.use_excel_serialization", "false")
//      .set("spark.sql.files.minPartitionNum", "3")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_config.s3.local_cache.max_size",
        "10737418240")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_config.s3.local_cache.enabled", "false")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_config.s3.local_cache.cache_path",
        "/tmp/gluten/s3_local_cache")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache",
        "false")
//      .set(
//        "spark.gluten.sql.columnar.backend.ch.runtime_settings.join_algorithm",
//        "grace_hash")
//      .set(
//        "spark.gluten.sql.columnar.backend.ch.runtime_settings.optimize_aggregation_in_order",
//        "1")
//        .set(
//            "spark.gluten.sql.columnar.backend.ch.runtime_settings.max_bytes_before_external_group_by",
//            "2147483648")
//      .set(
//        "spark.gluten.sql.columnar.backend.ch.runtime_settings.max_bytes_before_external_group_by",
//        "2147483648")
//      .set(
//        "spark.gluten.sql.columnar.backend.ch.runtime_settings.max_memory_usage",
//        "214748364800")
//      .set("spark.storage.storageFraction", "0.3")
      .set("spark.memory.offHeap.size", "12g")
      .set("spark.executor.memory", "2g")
      .set("spark.gluten.sql.parquet.maxmin.index", "true")
//      .set("spark.gluten.sql.columnar.shuffle.preferSpill", "true")
//    https://biaobiao.s3.cn-northwest-1.amazonaws.com.cn/22%E6%B5%8B%E8%AF%95%E6%95%B0%E6%8D%AE-%E6%B5%8B%E8%AF%951.csv
//      .set("spark.hadoop.fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
////      .set("spark.hadoop.fs.s3a.endpoint.region", "cn-northwest-1")
//      .set("spark.hadoop.fs.s3a.access.key", "AKIAW36DVI3VYTLFJWMN")
//      .set("spark.hadoop.fs.s3a.secret.key", "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")
//      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//      .set("spark.hadoop.fs.s3a.path.style.access", "true")
//      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
      // minio
      .set("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:8000")
      .set("spark.hadoop.fs.s3a.access.key", "8NDnohl85ycmpNKgqiUY")
      .set("spark.hadoop.fs.s3a.secret.key", "x6PCBv5p9glkcbegeyLJEohVKhlbztuVolFK7rUB")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    // arn
    //      .set("spark.hadoop.fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
    //      .set("spark.hadoop.fs.s3a.endpoint.region", "cn-northwest-1")
    //      .set("spark.hadoop.fs.s3a.access.key", "AKIAW36DVI3VYTLFJWMN")
    //      .set("spark.hadoop.fs.s3a.secret.key", "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")
    //      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    //      .set("spark.hadoop.fs.s3a.path.style.access", "true")
    //      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    //      .set(
    //        "spark.hadoop.fs.s3a.bucket.aldi-storesample.assumed.role.arn",
    //        "arn:aws-cn:iam::472319870699:role/aldi-poc-role2")
    //      .set("spark.hadoop.fs.s3a.bucket.aldi-storesample.assumed.role.session.name", "test")
    //      .set("spark.hadoop.fs.s3a.bucket.bucket_name.assumed.role.externalId", "kyligencezen")

    // 1. fs.s3a.bucket.bucket_name.assumed.role.arn
    // 2. fs.s3a.bucket.bucket_name.assumed.role.session.name
    // 3. fs.s3a.bucket.bucket_name.endpoint
    // 4. fs.s3a.bucket.bucket_name.assumed.role.externalId (non hadoop official)
//      .set(
//        "spark.hadoop.fs.s3a.aws.credentials.provider",
//        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
//      .set("spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level", "test")

  }
  private var _hiveSpark: SparkSession = _
  override protected def spark: SparkSession = _hiveSpark
  override protected def initializeSession(): Unit = {
    if (_hiveSpark == null) {
      _hiveSpark = SparkSession
        .builder()
        .appName("Gluten-TEST")
        .master(s"local[5]")
        .config(sparkConf)
        .getOrCreate()
    }
  }

  test("omni_channel_sales") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("compay_name", StringType, nullable = true),
        StructField.apply("shop_code", IntegerType, nullable = true),
        StructField.apply("shop_name", StringType, nullable = true),
        StructField.apply("area_name", StringType, nullable = true),
        StructField.apply("pri_channel", StringType, nullable = true),
        StructField.apply("sec_channel", StringType, nullable = true),
        StructField.apply("bill_date", DateType, nullable = true),
        StructField.apply("bill_year", IntegerType, nullable = true),
        StructField.apply("bill_month", IntegerType, nullable = true),
        StructField.apply("bill_no", StringType, nullable = true),
        StructField.apply("source_bill_no", StringType, nullable = true),
        StructField.apply("customer_id", IntegerType, nullable = true),
        StructField.apply("brand_id", IntegerType, nullable = true),
        StructField.apply("brand_name", StringType, nullable = true),
        StructField.apply("goods_id", IntegerType, nullable = true),
        StructField.apply("sku_id", IntegerType, nullable = true),
        StructField.apply("goods_style", StringType, nullable = true),
        StructField.apply("goods_bar", LongType, nullable = true),
        StructField.apply("goods_name", StringType, nullable = true),
        StructField.apply("goods_type", StringType, nullable = true),
        StructField.apply("goods_year", StringType, nullable = true),
        StructField.apply("goods_color", StringType, nullable = true),
        StructField.apply("goods_size", IntegerType, nullable = true),
        StructField.apply("qty", IntegerType, nullable = true),
        StructField.apply("stand_amount", DoubleType, nullable = true),
        StructField.apply("pay_amount", DoubleType, nullable = true),
        StructField.apply("rebate_amount", DoubleType, nullable = true),
        StructField.apply("pay_type", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema)
      .csv(csvDataPath + "/xiaoshou.csv")
      .toDF()

    df.createTempView("omni_channel_sales")

//    spark.sql(
//      """
//      select sum(shop_code) , avg(customer_id) from omni_channel_sales group by compay_name, shop_name
//        |""".stripMargin).show()
    compareResultsAgainstVanillaSpark(
      """
        select sum(shop_code) , avg(customer_id) from omni_channel_sales group by compay_name, shop_name
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )

//    compareResultsAgainstVanillaSpark(
//      """
//        select max(pay_amount),max(rebate_amount), max(goods_bar), pay_type  from omni_channel_sales group by pay_type
//        |""".stripMargin,
//      compareResult = true,
//      _ => {}
//    )
    sleep(100000000)
  }

  test("loan_default_dataset") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("id", IntegerType, nullable = true),
        StructField.apply("year", IntegerType, nullable = true),
        StructField.apply("loan_limit", StringType, nullable = true),
        StructField.apply("gender", StringType, nullable = true),
        StructField.apply("approv_in_adv", StringType, nullable = true),
        StructField.apply("loan_type", StringType, nullable = true),
        StructField.apply("loan_purpose", StringType, nullable = true),
        StructField.apply("credit_worthiness", StringType, nullable = true),
        StructField.apply("open_credit", StringType, nullable = true),
        StructField.apply("business_or_commercial", StringType, nullable = true),
        StructField.apply("loan_amount", IntegerType, nullable = true),
        StructField.apply("rate_of_interest", DoubleType, nullable = true),
        StructField.apply("interest_rate_spread", DoubleType, nullable = true),
        StructField.apply("upfront_charges", DoubleType, nullable = true),
        StructField.apply("term", IntegerType, nullable = true),
        StructField.apply("neg_ammortization", StringType, nullable = true),
        StructField.apply("interest_only", StringType, nullable = true),
        StructField.apply("lump_sum_payment", StringType, nullable = true),
        StructField.apply("property_value", IntegerType, nullable = true),
        StructField.apply("construction_type", StringType, nullable = true),
        StructField.apply("occupancy_type", StringType, nullable = true),
        StructField.apply("secured_by", StringType, nullable = true),
        StructField.apply("total_units", StringType, nullable = true),
        StructField.apply("income", IntegerType, nullable = true),
        StructField.apply("credit_type", StringType, nullable = true),
        StructField.apply("credit_score", IntegerType, nullable = true),
        StructField.apply("co_applicant_credit_type", StringType, nullable = true),
        StructField.apply("age", StringType, nullable = true),
        StructField.apply("submission_of_application", StringType, nullable = true),
        StructField.apply("ltv", DoubleType, nullable = true),
        StructField.apply("region", StringType, nullable = true),
        StructField.apply("security_type", StringType, nullable = true),
        StructField.apply("status", IntegerType, nullable = true),
        StructField.apply("dtir1", IntegerType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema)
      .csv("s3a://test" + "/loan_default_dataset.csv")
      .toDF()

    df.createTempView("loan_default_dataset")

    compareResultsAgainstVanillaSpark(
      """
      select count(*) the_effect_of_income_on_loan_behavior from loan_default_dataset
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )
//    spark
//      .sql("""
//    select avg(income) the_effect_of_income_on_loan_behavior from loan_default_dataset
//             |""".stripMargin)
//      .show()

//    compareResultsAgainstVanillaSpark(
//      """
//      select avg(income) the_effect_of_income_on_loan_behavior from loan_default_dataset
//        |""".stripMargin,
//      compareResult = true,
//      _ => {}
//    )
//    sleep(1000000)
  }

  test("test_Product") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("productkey", StringType, nullable = true),
        StructField.apply("color", StringType, nullable = true),
        StructField.apply("class", StringType, nullable = true),
        StructField.apply("size", StringType, nullable = true),
        StructField.apply("productline", StringType, nullable = true),
        StructField.apply("status", StringType, nullable = true),
        StructField.apply("sales", DoubleType, nullable = true),
        StructField.apply("l100", IntegerType, nullable = true),
        StructField.apply("l1000", IntegerType, nullable = true),
        StructField.apply("l5000", IntegerType, nullable = true),
        StructField.apply("l10000", IntegerType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema)
      .csv(csvDataPath + "/Product.csv")
      .toDF()

    df.createTempView("test_Product")

    compareResultsAgainstVanillaSpark(
      """
    select L1000
        |from test_Product
        |group by L1000
        |order by L1000
        |limit 2147483647
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )

  }

  test("22-6") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("客户编码", StringType, nullable = true),
        StructField.apply("报价时间", DateType, nullable = true),
        StructField.apply("sku", StringType, nullable = true),
        StructField.apply("金额", DoubleType, nullable = true),
        StructField.apply("交期", IntegerType, nullable = true),
        StructField.apply("年份", IntegerType, nullable = true),
        StructField.apply("季度", StringType, nullable = true),
        StructField.apply("月份", IntegerType, nullable = true),
        StructField.apply("成交金额", DoubleType, nullable = true),
        StructField.apply("实际金额", IntegerType, nullable = true),
        StructField.apply("数量", IntegerType, nullable = true),
        StructField.apply("成交数量", IntegerType, nullable = true),
        StructField.apply("是否历史价格", StringType, nullable = true),
        StructField.apply("行业", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema)
      .csv(csvDataPath + "/22-11.csv")
      .toDF()

    df.createTempView("AAA22")
    spark
      .sql("""
             | select `月份`, avg(`成交金额`) `aa`
             |from `AAA22`
             |group by `月份`
             |limit 1000
             |""".stripMargin)
      .show()

    sleep(10000000)

  }

  test("22-s3-1") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("订单编号", StringType, nullable = true),
        StructField.apply("客户名称", StringType, nullable = true),
        StructField.apply("注册时间", DateType, nullable = true),
        StructField.apply("客户等级", StringType, nullable = true),
        StructField.apply("报价时间", DateType, nullable = true),
        StructField.apply("型号", StringType, nullable = true),
        StructField.apply("数量", IntegerType, nullable = true),
        StructField.apply("单价", DoubleType, nullable = true),
        StructField.apply("金额", DoubleType, nullable = true),
        StructField.apply("交期", IntegerType, nullable = true),
        StructField.apply("订单时间", DateType, nullable = true),
        StructField.apply("成交数量", IntegerType, nullable = true),
        StructField.apply("成交单价", StringType, nullable = true),
        StructField.apply("成交金额", IntegerType, nullable = true),
        StructField.apply("已报价次数", IntegerType, nullable = true),
        StructField.apply("年份", IntegerType, nullable = true),
        StructField.apply("季度", StringType, nullable = true),
        StructField.apply("月份", IntegerType, nullable = true),
        StructField.apply("周数", IntegerType, nullable = true),
        StructField.apply("报价交期阶段", StringType, nullable = true),
        StructField.apply("业务组", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema)
      .csv("s3a://biaobiao/aa.csv")
      .toDF()

    df.createTempView("aaa6")
    val sql =
      """
        | select `年份`, sum(`成交金额`) aa from `aaa6` where EXTRACT(day from `订单时间`) = EXTRACT(day from to_date('2022-06-01', 'yyyy-MM-dd'))  group by `年份` limit 500
        |""".stripMargin

    var time = System.nanoTime()
    spark.sql(sql).show()
    println(System.nanoTime() - time)

//    time = System.nanoTime()
//    withSQLConf(vanillaSparkConfs(): _*) {
//          spark.sql(sql).show()
//    }

//    println(System.nanoTime() - time)
  }

  test("22-minio-1") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("订单编号", StringType, nullable = true),
        StructField.apply("客户名称", StringType, nullable = true),
        StructField.apply("注册时间", DateType, nullable = true),
        StructField.apply("客户等级", StringType, nullable = true),
        StructField.apply("报价时间", DateType, nullable = true),
        StructField.apply("型号", StringType, nullable = true),
        StructField.apply("数量", IntegerType, nullable = true),
        StructField.apply("单价", DoubleType, nullable = true),
        StructField.apply("金额", DoubleType, nullable = true),
        StructField.apply("交期", IntegerType, nullable = true),
        StructField.apply("订单时间", DateType, nullable = true),
        StructField.apply("成交数量", IntegerType, nullable = true),
        StructField.apply("成交单价", StringType, nullable = true),
        StructField.apply("成交金额", IntegerType, nullable = true),
        StructField.apply("已报价次数", IntegerType, nullable = true),
        StructField.apply("年份", IntegerType, nullable = true),
        StructField.apply("季度", StringType, nullable = true),
        StructField.apply("月份", IntegerType, nullable = true),
        StructField.apply("周数", IntegerType, nullable = true),
        StructField.apply("报价交期阶段", StringType, nullable = true),
        StructField.apply("业务组", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema)
      .csv("s3a://test.zen.qa/Export+vydaných+faktur+pro+účetní.csv")
      .toDF()

    df.createTempView("aaa6")
    val sql =
      """
        | select `年份`, sum(`成交金额`) aa from `aaa6` where EXTRACT(day from `订单时间`) = EXTRACT(day from to_date('2022-06-01', 'yyyy-MM-dd'))  group by `年份` limit 500
        |""".stripMargin

    var time = System.nanoTime()
    spark.sql(sql).show()
    println(System.nanoTime() - time)

    time = System.nanoTime()
    withSQLConf(vanillaSparkConfs(): _*) {
      spark.sql(sql).show()
    }

    println(System.nanoTime() - time)
  }

  test("22-6-1") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("订单编号", StringType, nullable = true),
        StructField.apply("客户名称", StringType, nullable = true),
        StructField.apply("注册时间", DateType, nullable = true),
        StructField.apply("客户等级", StringType, nullable = true),
        StructField.apply("报价时间", DateType, nullable = true),
        StructField.apply("型号", StringType, nullable = true),
        StructField.apply("数量", IntegerType, nullable = true),
        StructField.apply("单价", DoubleType, nullable = true),
        StructField.apply("金额", DoubleType, nullable = true),
        StructField.apply("交期", IntegerType, nullable = true),
        StructField.apply("订单时间", DateType, nullable = true),
        StructField.apply("成交数量", IntegerType, nullable = true),
        StructField.apply("成交单价", StringType, nullable = true),
        StructField.apply("成交金额", IntegerType, nullable = true),
        StructField.apply("已报价次数", IntegerType, nullable = true),
        StructField.apply("年份", IntegerType, nullable = true),
        StructField.apply("季度", StringType, nullable = true),
        StructField.apply("月份", IntegerType, nullable = true),
        StructField.apply("周数", IntegerType, nullable = true),
        StructField.apply("报价交期阶段", StringType, nullable = true),
        StructField.apply("业务组", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema)
      .csv(csvDataPath + "/aa.csv")
      .toDF()

    df.createTempView("aaa6")
    val sql =
      """
        | select `年份`, sum(`成交金额`) aa from `aaa6` where EXTRACT(day from `订单时间`) = EXTRACT(day from to_date('2022-06-01', 'yyyy-MM-dd'))  group by `年份` limit 500
        |""".stripMargin

    var time = System.nanoTime()
    spark.sql(sql).show()
    println(System.nanoTime() - time)

    time = System.nanoTime()
    withSQLConf(vanillaSparkConfs(): _*) {
      spark.sql(sql).show()
    }

    println(System.nanoTime() - time)
  }

  test("advertisement_analysis_lifazhu") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("date", DateType, nullable = true),
        StructField.apply("campaign", StringType, nullable = true),
        StructField.apply("adgroup", StringType, nullable = true),
        StructField.apply("ad", StringType, nullable = true),
        StructField.apply("cost", DoubleType, nullable = true),
        StructField.apply("impressions", IntegerType, nullable = true),
        StructField.apply("clicks", IntegerType, nullable = true),
        StructField.apply("conversions", IntegerType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema)
      .csv(csvDataPath + "/advertisement_analysis_lifazhu.csv")
      .toDF()

    df.createTempView("advertisement_analysis_lifazhu")

//    compareResultsAgainstVanillaSpark(
//      """
//        |select "date", campaign, adgroup, ad, cost, impressions, clicks, conversions
//        |        from advertisement_analysis_lifazhu
//        |        group by "date", campaign, adgroup, ad, cost, impressions, clicks, conversions
//        |""".stripMargin,
//      compareResult = true,
//      _ => {}
//    )

    compareResultsAgainstVanillaSpark(
      """
        |select *
        |from (select ad, sum(cost) / sum(clicks) CPC, sum(cost) / sum(conversions) CPA, row_number() over (order by sum(cost) / sum(clicks) desc) CPC_RANK, row_number() over (order by sum(cost) / sum(conversions) desc) CPA_RANK
        |    from (select "date", campaign, adgroup, ad, cost, impressions, clicks, conversions
        |        from advertisement_analysis_lifazhu
        |        group by "date", campaign, adgroup, ad, cost, impressions, clicks, conversions) t
        |    group by ad) t1
        |    order by CPA_RANK asc
        |""".stripMargin,
//      where CPC_RANK <= 5 or CPA_RANK <= 5
      compareResult = true,
      _ => {}
    )

  }

  test("default_92u921k8") {
    val schema1 = StructType.apply(
      Seq(
        StructField.apply("云星空代码", StringType, nullable = true),
        StructField.apply("产品", StringType, nullable = true),
        StructField.apply("毫升", DoubleType, nullable = true),
        StructField.apply("装箱", IntegerType, nullable = true),
        StructField.apply("bottle_瓶", IntegerType, nullable = true),
        StructField.apply("到货周期", StringType, nullable = true),
        StructField.apply("库存标准_箱_淡季", DoubleType, nullable = true),
        StructField.apply("库存标准_箱_旺季", DoubleType, nullable = true),
        StructField.apply("库存系数_天_淡季", IntegerType, nullable = true),
        StructField.apply("库存系数_天_旺季", IntegerType, nullable = true)
      ))

    val schema2 = StructType.apply(
      Seq(
        StructField.apply("物料编码", StringType, nullable = true),
        StructField.apply("物料名称", StringType, nullable = true),
        StructField.apply("规格型号", StringType, nullable = true),
        StructField.apply("品牌", StringType, nullable = true),
        StructField.apply("仓库名称", StringType, nullable = true),
        StructField.apply("批号", StringType, nullable = true),
        StructField.apply("供应商批号", StringType, nullable = true),
        StructField.apply("生产日期", DateType, nullable = true),
        StructField.apply("有效期至", DateType, nullable = true),
        StructField.apply("库存主单位", StringType, nullable = true),
        StructField.apply("库存量_主单位_", IntegerType, nullable = true),
        StructField.apply("库存状态", StringType, nullable = true),
        StructField.apply("库存组织", StringType, nullable = true),
        StructField.apply("货主类型", StringType, nullable = true),
        StructField.apply("货主编码", IntegerType, nullable = true),
        StructField.apply("货主名称", StringType, nullable = true),
        StructField.apply("入库日期", DateType, nullable = true)
      ))

    val df1 = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema1)
      .csv(csvDataPath + "/daizhi.csv")
      .toDF()

    val df2 = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema2)
      .csv(csvDataPath + "/jishi.csv")
      .toDF()

    df1.createTempView("t1")
    df2.createTempView("t2")

    compareResultsAgainstVanillaSpark(
      """
        |with t as (
        |select *
        |from t1
        |  left join t2 on t1.`云星空代码` = t2.`物料编码`
        |  )
        |  select `入库日期`, `物料名称`, `库存系数_天_淡季`
        |  from t
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )

//    compareResultsAgainstVanillaSpark(
//      """
//        |with
//        |`_v_D呆滞分析1` as (
//        |select *
//        |from t1
//        |  left join t2 on t1.`云星空代码` = t2.`物料编码`
//        |)
//        |select `入库日期`, count(`库存量_主单位_`) `呆滞预警分析1`
//        |from `_v_D呆滞分析1` `D呆滞分析1`
//        |where `库存系数_天_旺季` = 60
//        |    and (`库存系数_天_淡季` in (30, 45) and `库存标准_箱_淡季` >= 1 and `库存标准_箱_淡季` <= 600 )
//        |    and (`库存标准_箱_旺季` >= 1 and `库存标准_箱_旺季` <= 950.7 and `物料名称` in ('62响皇家礼炮苏格兰威士忌 1L 1*1', '亚伯乐斯佩塞单一麦芽苏格兰威士忌 16年双桶陈酿 700ml 1*3', '亚伯乐斯佩塞单一麦芽苏格兰威士忌 18年 双雪莉桶熟成 700ml 1*6', '亚伯乐斯佩塞单一麦芽苏格兰威士忌14年双桶陈酿 700ml 1*6', '力加茴香开胃酒（配制酒） 700ml 1*6', '帝国田园珍藏卡瓦起泡葡萄酒 750ml 1*6', '帝国田园珍藏干红葡萄酒 750ml 1*6', '必富达粉红金酒风味配制酒 700ml 1*6', '格兰威特单一麦芽苏格兰威士忌 18年陈酿 700ml 1*6', '百龄坛12年苏格兰威士忌 50ml 1*120', '百龄坛17年苏格兰威士忌-无盒装 700ml 1*6', '百龄坛17年苏格兰威士忌-有盒装 700ml 1*6', '百龄坛21年苏格兰威士忌-含内盒 700ml 1*6', '皇家礼炮21年苏格兰威士忌-无盒装 700ml 1*6', '皇家礼炮21年苏格兰威士忌-有盒装 700ml 1*6', '皇家礼炮24年苏格兰威士忌-有盒装 700ml 1*6', '皇家礼炮38年苏格兰威士忌 500ml 1*3', '绝对风味伏特加（青柠味） 700ml 1*6', '美丽时光·巴黎艺术布拉森桃红香槟 750ml 1*6', '美丽时光·巴黎艺术经典白中白香槟 750ml 1*6', '芝华士·XV 15年苏格兰威士忌（鎏金版） 700ml 1*12', '马爹利XO干邑白兰地 1.5L 1*6', '马爹利XO干邑白兰地-不含摇架和倒酒器 3L 1*3', '马爹利蓝带傲创干邑白兰地-含内盒 700ml 1*6', '马爹利蓝带干邑白兰地-不含倒酒器 3L 1*4','马爹利蓝带干邑白兰地-无盒装 1.5L 1*6', '马爹利蓝带干邑白兰地-无盒装 3L 1*4', '马爹利蓝带干邑白兰地-无盒装 700ml 1*12', '马爹利蓝带干邑白兰地-有盒装 700ml 1*12', '马爹利鼎盛VSOP干邑白兰地 50ml 1*96', '马爹利鼎盛VSOP干邑白兰地 700ml 1*12', '（日店版）格兰威特单一麦芽苏格兰威士忌 醇萃12年雪莉桶陈酿 700ml 1*12') and (物料编码 in ('P0010012', 'P0010014', 'P0010278', 'P0010338', 'P0010368', 'P0010401', 'P0010423', 'P0010470', 'P0010474', 'P0010475', 'P0010476', 'P0010479', 'P0010480', 'P0010509', 'P0010520', 'P0010521', 'P0010525', 'P0010526', 'P0010537', 'P0010555', 'P0010589', 'P0010677', 'P0010692', 'P0010702', 'P0010709', 'P0010710', 'P0010711', 'P0010762', 'P0010976', 'P0010993', 'P0430011', 'P0430015')
//        |and (`库存量_主单位_` >= 0 and `库存量_主单位_` <= 1925)))
//        |group by `入库日期`
//        |order by `入库日期` desc
//        |""".stripMargin,
//      compareResult = true,
//      _ => {}
//    )
  }

  test("retail_stores_by_transaction") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("order_id", StringType, nullable = true),
        StructField.apply("order_date", DateType, nullable = true),
        StructField.apply("order_date_year", IntegerType, nullable = true),
        StructField.apply("order_date_month", IntegerType, nullable = true),
        StructField.apply("order_date_day", IntegerType, nullable = true),
        StructField.apply("customer_name", StringType, nullable = true),
        StructField.apply("store_region", StringType, nullable = true),
        StructField.apply("store_city", StringType, nullable = true),
        StructField.apply("item_category", StringType, nullable = true),
        StructField.apply("item_unit_price", IntegerType, nullable = true),
        StructField.apply("item_discount", FloatType, nullable = true),
        StructField.apply("order_quantity", IntegerType, nullable = true),
        StructField.apply("order_net_profit", FloatType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema)
      .csv(csvDataPath + "/retail_stores_by_transaction.csv")
      .toDF()

    df.createTempView("retail_stores_by_transaction")
    val sql =
      """
        | select order_date, sum(order_net_profit) / count(distinct order_id) aa from retail_stores_by_transaction
        | group by order_date order by order_date asc
        |""".stripMargin

    var time = System.nanoTime()
    time = System.nanoTime()
    spark.sql(sql).show()

    println("gluten time: " + (System.nanoTime() - time) + "ns")

//    time = System.nanoTime()
//    withSQLConf(vanillaSparkConfs(): _*) {
//      spark.sql(sql).show()
//    }
//    println("spark time: " + (System.nanoTime() - time) + "ns")
  }

  test("decimal") {
    val demo_retail_stores_customer = StructType.apply(
      Seq(
        StructField.apply("order_date_year", IntegerType, nullable = true),
        StructField.apply("order_date_month", IntegerType, nullable = true),
        StructField.apply("customer_name", StringType, nullable = true),
        StructField.apply("store_region", StringType, nullable = true),
        StructField.apply("repeat_purchase_count", IntegerType, nullable = true),
        StructField.apply("cross_sell_count", IntegerType, nullable = true),
        StructField.apply("customer_lifetime_revenue", DoubleType, nullable = true)
      ))

    val demo_dim_customer = StructType.apply(
      Seq(
        StructField.apply("customer_name", IntegerType, nullable = true),
        StructField.apply("customer_age", IntegerType, nullable = true),
        StructField.apply("customer_edu_level", StringType, nullable = true),
        StructField.apply("customer_income_range", StringType, nullable = true),
        StructField.apply("customer_family_size", IntegerType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(demo_retail_stores_customer)
      .csv(csvDataPath + "/demo_retail_stores_customer.csv")
      .toDF()
      .createTempView("demo_retail_stores_customer")

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(demo_dim_customer)
      .csv(csvDataPath + "/demo_dim_customer.csv")
      .toDF()
      .createTempView("demo_dim_customer")

    val sql =
      """
        |with _v_view_customer AS
        |    (SELECT demo_retail_stores_customer.order_date_year,
        |         demo_retail_stores_customer.order_date_month,
        |         demo_retail_stores_customer.customer_name demo_retail_stores_customer_customer_name,
        |         demo_retail_stores_customer.store_region,
        |         demo_retail_stores_customer.repeat_purchase_count,
        |         demo_retail_stores_customer.cross_sell_count,
        |         demo_retail_stores_customer.customer_lifetime_revenue,
        |         demo_dim_customer.customer_name demo_dim_customer_customer_name,
        |         demo_dim_customer.customer_age,
        |         demo_dim_customer.customer_edu_level,
        |         demo_dim_customer.customer_income_range,
        |         demo_dim_customer.customer_family_size
        |    FROM demo_retail_stores_customer
        |    LEFT JOIN demo_dim_customer
        |        ON demo_retail_stores_customer.customer_name = demo_dim_customer.customer_name )
        |SELECT sum(Demo_hltest) total_repurchase,
        |         sum(Demo_hltest) * 0.05 repurchase_increase,
        |         avg(Demo_hltest) avg_repurchase,
        |         avg(Demo_hltest) * 0.05 avg_repurchase_increase,
        |         sum(f1) repurchase_in_east_region_low_income,
        |         sum(f1) * 0.05 repurchase_increase_in_east_region_low_income,
        |         avg(f2) avg_repurchase_in_east_region_low_income,
        |         avg(f2) * 0.05 avg_repurchase_increase_in_east_region_low_income,
        |         sum(f1) * 0.05 * 1000 sales_increase_in_east_region_low_income
        |FROM
        |    (SELECT sum(repeat_purchase_count) Demo_hltest,
        |         sum(repeat_purchase_count) f1,
        |         sum(repeat_purchase_count) f2
        |    FROM _v_view_customer view_customer
        |    GROUP BY  order_date_year, order_date_month, demo_retail_stores_customer_customer_name, store_region, customer_age, customer_edu_level, customer_income_range, customer_family_size) t1
        |""".stripMargin

    var time = System.nanoTime()
    time = System.nanoTime()
    spark.sql(sql).show()

    println("gluten time: " + (System.nanoTime() - time) + "ns")

    //    time = System.nanoTime()
    //    withSQLConf(vanillaSparkConfs(): _*) {
    //      spark.sql(sql).show()
    //    }
    //    println("spark time: " + (System.nanoTime() - time) + "ns")
  }

  test("credit_loans") {
    val credit_loans = StructType.apply(
      Seq(
        StructField.apply("user_id", StringType, nullable = true),
        StructField.apply("loan_channel", StringType, nullable = true),
        StructField.apply("loan_paid_time", TimestampType, nullable = true),
        StructField.apply("loan_amount", IntegerType, nullable = true),
        StructField.apply("loan_paid_up_amount", IntegerType, nullable = true),
        StructField.apply("loan_overdue_amount", IntegerType, nullable = true),
        StructField.apply("overdue_flag", BooleanType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(credit_loans)
      .csv(csvDataPath + "/credit_loans.csv")
      .toDF()
      .createTempView("credit_loans")

    val sql =
      """
        |select sum(loan_overdue_amount) aa  from credit_loans where overdue_flag
        |""".stripMargin

    var time = System.nanoTime()
    time = System.nanoTime()
    spark.sql(sql).collect()

    println("gluten time: " + (System.nanoTime() - time) + "ns")
  }

  test("KY-2742 cloud_billing_sample_cn") {
    val schema_cloud_billing_sample_cn = StructType.apply(
      Seq(
        StructField.apply("成本中心", StringType, nullable = true),
        StructField.apply("平台", StringType, nullable = true),
        StructField.apply("项目名称", StringType, nullable = true),
        StructField.apply("所有人", StringType, nullable = true),
        StructField.apply("成本_人民币_", DoubleType, nullable = true),
        StructField.apply("日期", DateType, nullable = true),
        StructField.apply("年", IntegerType, nullable = true),
        StructField.apply("月", IntegerType, nullable = true),
        StructField.apply("成本子类别", StringType, nullable = true),
        StructField.apply("成本类别", StringType, nullable = true),
        StructField.apply("crr", StringType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema_cloud_billing_sample_cn)
      .csv(zenDataPath + "/cloud_billing_sample_cn.csv")
      .toDF()
      .createTempView("cloud_billing_sample_cn")

    val schema_retail_stores_by_transaction = StructType.apply(
      Seq(
        StructField.apply("订单编号", StringType, nullable = true),
        StructField.apply("订单日期", DateType, nullable = true),
        StructField.apply("订单年份", IntegerType, nullable = true),
        StructField.apply("订单月份", IntegerType, nullable = true),
        StructField.apply("订单天数", IntegerType, nullable = true),
        StructField.apply("客户名称", StringType, nullable = true),
        StructField.apply("门店区域", StringType, nullable = true),
        StructField.apply("门店城市", StringType, nullable = true),
        StructField.apply("商品分类", StringType, nullable = true),
        StructField.apply("商品单价", IntegerType, nullable = true),
        StructField.apply("商品折扣", DoubleType, nullable = true),
        StructField.apply("商品数量", IntegerType, nullable = true),
        StructField.apply("订单净利润", DoubleType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema_retail_stores_by_transaction)
      .csv(zenDataPath + "/retail_stores_CN.csv")
      .toDF()
      .createTempView("retail_stores_by_transaction")

    val sql2 =
      """
        | select
        |     coalesce(t.`商品折扣`, t4.`公共维度2`, t7.`商品折扣`) pd2,
        |     coalesce(t.`基本指标时间维度`, 0) + coalesce(t4.`复合指标不一样的数据源`, 0) + coalesce(t7.`复合指标`, 0) aa
        |     from (
        |         select
        |             `商品折扣`,
        |             sum(`商品折扣`) as `基本指标时间维度`
        |         from retail_stores_by_transaction
        |         group by `商品折扣`
        |      ) t
        |      full join (
        |          select
        |              coalesce(t1.`成本_人民币_`, t3.`商品折扣`) `公共维度2`,
        |              coalesce(t1.`基本指标不一样的数据源`, 0) + coalesce(t3.`基本指标数值`, 0) `复合指标不一样的数据源`
        |          from (
        |              select
        |                  `成本_人民币_`,
        |                  sum(`月`) as  `基本指标不一样的数据源`
        |              from cloud_billing_sample_cn
        |              where `年` >= 2021
        |                  and `年` <= 2022
        |                  and (`月` >= 1 and `月` <= 12)
        |              group by `成本_人民币_`
        |          ) t1
        |          full join (
        |              select
        |                  `商品折扣`,
        |                  sum(`订单年份`) `基本指标数值`
        |              from retail_stores_by_transaction
        |              where `商品数量` >= 1 and `商品数量` <= 12
        |              group by `商品折扣`
        |          ) t3 on t1.`成本_人民币_` = t3.`商品折扣`
        |      ) t4 on t.`商品折扣` = t4.`公共维度2`
        |      full join (
        |          select
        |              `商品折扣`,
        |              coalesce(sum(`订单年份`), 0) + coalesce(sum(`商品折扣`), 0) `复合指标`
        |          from retail_stores_by_transaction
        |          where `商品折扣` >= 0.1 and `商品折扣` <= 0.35
        |          group by `商品折扣`
        |      ) t7 on coalesce(t.`商品折扣`, t4.`公共维度2`) = t7.`商品折扣`
        |      order by 1 limit 20
        |""".stripMargin

    val sql =
      """
        |select
        |              coalesce(t1.`成本_人民币_`, t3.`商品折扣`) `公共维度2`,
        |              coalesce(t1.`基本指标不一样的数据源`, 0) + coalesce(t3.`基本指标数值`, 0) `复合指标不一样的数据源`
        |          from (
        |              select
        |                  `成本_人民币_`,
        |                  sum(`月`) as  `基本指标不一样的数据源`
        |              from cloud_billing_sample_cn
        |              where `年` >= 2021
        |                  and `年` <= 2022
        |                  and (`月` >= 1 and `月` <= 12)
        |              group by `成本_人民币_`
        |          ) t1
        |          full join (
        |              select
        |                  `商品折扣`,
        |                  sum(`订单年份`) `基本指标数值`
        |              from retail_stores_by_transaction
        |              where `商品数量` >= 1 and `商品数量` <= 12
        |              group by `商品折扣`
        |          ) t3 on t1.`成本_人民币_` = t3.`商品折扣`
        |""".stripMargin

    var time = System.nanoTime()
    time = System.nanoTime()
    val df = spark.sql(sql).toDF()

    print(df.queryExecution.executedPlan)
    print(df.queryExecution.logical)
    df.show()
    println("gluten time: " + (System.nanoTime() - time) + "ns")

    //    time = System.nanoTime()
    //    withSQLConf(vanillaSparkConfs(): _*) {
    //      spark.sql(sql).show()
    //    }
    //    println("spark time: " + (System.nanoTime() - time) + "ns")
  }

  test("KY-2741 retail_stores_transaction") {
    val schema_retail_stores_transaction = StructType.apply(
      Seq(
        StructField.apply("order_id", StringType, nullable = true),
        StructField.apply("order_date", DateType, nullable = true),
        StructField.apply("order_date_year", IntegerType, nullable = true),
        StructField.apply("order_date_month", IntegerType, nullable = true),
        StructField.apply("order_date_day", IntegerType, nullable = true),
        StructField.apply("customer_name", StringType, nullable = true),
        StructField.apply("store_region", StringType, nullable = true),
        StructField.apply("store_city", StringType, nullable = true),
        StructField.apply("item_category", StringType, nullable = true),
        StructField.apply("item_unit_price", IntegerType, nullable = true),
        StructField.apply("item_discount", DoubleType, nullable = true),
        StructField.apply("order_quantity", IntegerType, nullable = true),
        StructField.apply("order_net_profit", DoubleType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema_retail_stores_transaction)
      .csv(zenDataPath + "/retail_stores_transaction.csv")
      .toDF()
      .createTempView("retail_stores_transaction")

    val sql =
      """
        | select
        |     t0.store_region_grp `地区`,
        |     t0.item_category_grp `品类`,
        |     t0.`总销售额`
        | from (
        |     select
        |         store_region_grp,
        |         item_category_grp,
        |         sum(`$f13`) `总销售额`
        |     from (
        |         select
        |             order_date
        |             order_date_grp,
        |             order_date_year order_date_year_grp,
        |             order_date_month order_date_month_grp,
        |             store_region store_region_grp,
        |             store_city store_city_grp,
        |             item_category item_category_grp,
        |             item_unit_price * (coalesce(1, 0) - coalesce(item_discount, 0)) * order_quantity `$f13`
        |        from retail_stores_transaction) t
        |    group by store_region_grp, item_category_grp) t0
        |    inner join (
        |        select
        |            store_region_grp store_region
        |        from (
        |            select
        |                store_region_grp,
        |                    sum(`$f13`) Total_Sales
        |                from (
        |                    select
        |                        order_date order_date_grp,
        |                        order_date_year order_date_year_grp,
        |                        order_date_month order_date_month_grp,
        |                        store_region store_region_grp, store_city store_city_grp,
        |                        item_category item_category_grp,
        |                        item_unit_price * (coalesce(1, 0) - coalesce(item_discount, 0)) * order_quantity `$f13`
        |                    from retail_stores_transaction) t1
        |                group by store_region_grp
        |                order by 2 desc
        |                limit 3)
        |         t3) t4 on t0.store_region_grp = t4.store_region
        |     order by t0.store_region_grp, t0.`总销售额` desc
        |""".stripMargin
    val sql2 = "select * from retail_stores_transaction"
    var time = System.nanoTime()
    time = System.nanoTime()
//    val df = spark.sql(sql).toDF()

    var i = 30000
    while (i > 0) {
      val df = spark.sql(sql2).toDF()
//      print(df.queryExecution.executedPlan)
      df.show()
      i = i - 1
      println(i)
    }
    println("gluten time: " + (System.nanoTime() - time) + "ns")
  }

  test("KY-2744") {
    val FactInternetSales = StructType.apply(
      Seq(
        StructField.apply("internettransid", StringType, nullable = true),
        StructField.apply("productkey", IntegerType, nullable = true),
        StructField.apply("orderdatekey", IntegerType, nullable = true),
        StructField.apply("duedatekey", IntegerType, nullable = true),
        StructField.apply("shipdatekey", IntegerType, nullable = true),
        StructField.apply("customerkey", IntegerType, nullable = true),
        StructField.apply("orderquantity", IntegerType, nullable = true),
        StructField.apply("salesamount", DoubleType, nullable = true),
        StructField.apply("totalproductcost", DoubleType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(FactInternetSales)
      .csv(zenDataPath + "/FactInternetSales.csv")
      .toDF()
      .createTempView("FactInternetSales")

    val DimCustomer = StructType.apply(
      Seq(
        StructField.apply("customerkey", IntegerType, nullable = true),
        StructField.apply("geographykey", IntegerType, nullable = true),
        StructField.apply("customeralternatekey", StringType, nullable = true),
        StructField.apply("title", StringType, nullable = true),
        StructField.apply("firstname", StringType, nullable = true),
        StructField.apply("middlename", StringType, nullable = true),
        StructField.apply("lastname", StringType, nullable = true),
        StructField.apply("namestyle", IntegerType, nullable = true),
        StructField.apply("birthdate", DateType, nullable = true),
        StructField.apply("maritalstatus", StringType, nullable = true),
        StructField.apply("suffix", StringType, nullable = true),
        StructField.apply("gender", StringType, nullable = true),
        StructField.apply("emailaddress", StringType, nullable = true),
        StructField.apply("yearlyincome", DoubleType, nullable = true),
        StructField.apply("totalchildren", IntegerType, nullable = true),
        StructField.apply("numberchildrenathome", IntegerType, nullable = true),
        StructField.apply("englisheducation", StringType, nullable = true),
        StructField.apply("spanisheducation", StringType, nullable = true),
        StructField.apply("frencheducation", StringType, nullable = true),
        StructField.apply("englishoccupation", StringType, nullable = true),
        StructField.apply("spanishoccupation", StringType, nullable = true),
        StructField.apply("frenchoccupation", StringType, nullable = true),
        StructField.apply("houseownerflag", IntegerType, nullable = true),
        StructField.apply("numbercarsowned", IntegerType, nullable = true),
        StructField.apply("addressline1", StringType, nullable = true),
        StructField.apply("addressline2", StringType, nullable = true),
        StructField.apply("phone", StringType, nullable = true),
        StructField.apply("datefirstpurchase", DateType, nullable = true),
        StructField.apply("commutedistance", StringType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(DimCustomer)
      .csv(zenDataPath + "/DimCustomer.csv")
      .toDF()
      .createTempView("DimCustomer")

    val DimProduct = StructType.apply(
      Seq(
        StructField.apply("productkey", IntegerType, nullable = true),
        StructField.apply("productsubcategorykey", IntegerType, nullable = true),
        StructField.apply("englishproductname", StringType, nullable = true),
        StructField.apply("color", StringType, nullable = true),
        StructField.apply("sizerange", StringType, nullable = true),
        StructField.apply("productline", StringType, nullable = true),
        StructField.apply("class", StringType, nullable = true),
        StructField.apply("style", StringType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(DimProduct)
      .csv(zenDataPath + "/DimProduct.csv")
      .toDF()
      .createTempView("DimProduct")

    val DimProductSubCategory = StructType.apply(
      Seq(
        StructField.apply("productsubcategorykey", IntegerType, nullable = true),
        StructField.apply("englishproductsubcategoryname", StringType, nullable = true),
        StructField.apply("productcategorykey", IntegerType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(DimProductSubCategory)
      .csv(zenDataPath + "/DimProductSubCategory.csv")
      .toDF()
      .createTempView("DimProductSubCategory")

    val DimProductCategory = StructType.apply(
      Seq(
        StructField.apply("productcategorykey", IntegerType, nullable = true),
        StructField.apply("englishproductcategoryname", StringType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(DimProductCategory)
      .csv(zenDataPath + "/DimProductCategory.csv")
      .toDF()
      .createTempView("DimProductCategory")

    val DimDate = StructType.apply(
      Seq(
        StructField.apply("datekey", IntegerType, nullable = true),
        StructField.apply("fulldatealternatekey", DateType, nullable = true),
        StructField.apply("daynumberofweek", IntegerType, nullable = true),
        StructField.apply("englishdaynameofweek", StringType, nullable = true),
        StructField.apply("daynumberofmonth", IntegerType, nullable = true),
        StructField.apply("daynumberofyear", IntegerType, nullable = true),
        StructField.apply("weeknumberofyear", IntegerType, nullable = true),
        StructField.apply("englishmonthname", StringType, nullable = true),
        StructField.apply("monthnumberofyear", IntegerType, nullable = true),
        StructField.apply("calendarquarterfullname", StringType, nullable = true),
        StructField.apply("calendarquarter", IntegerType, nullable = true),
        StructField.apply("calendaryear", IntegerType, nullable = true),
        StructField.apply("calendarsemester", IntegerType, nullable = true),
        StructField.apply("fiscalquarter", IntegerType, nullable = true),
        StructField.apply("fiscalyear", IntegerType, nullable = true),
        StructField.apply("fiscalsemester", IntegerType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(DimDate)
      .csv(zenDataPath + "/DimDate.csv")
      .toDF()
      .createTempView("DimDate")

//    val sql =
//      """
//        | with
//        |_v_View_InternetSales as (
//        |select
//        |       DimCustomer.BirthDate
//        |from FactInternetSales
//        |  left join DimCustomer on FactInternetSales.CustomerKey = DimCustomer.CustomerKey
//        |)
//        |select BirthDate
//        |from _v_View_InternetSales View_InternetSales
//        |limit 10
//        |
//        |""".stripMargin

    val sql =
      """
        | with
        |_v_View_InternetSales as (
        |select
        |       DimCustomer.BirthDate
        |from FactInternetSales
        |  left join DimCustomer on FactInternetSales.CustomerKey = DimCustomer.CustomerKey
        |)
        |select BirthDate
        |from _v_View_InternetSales View_InternetSales
        |group by BirthDate
        |order by BirthDate
        |limit 2147483647
        |
        |""".stripMargin

    var time = System.nanoTime()
    time = System.nanoTime()
    val df = spark.sql(sql).toDF()

    print(df.queryExecution.executedPlan)
    df.show()
    println("gluten time: " + (System.nanoTime() - time) + "ns")
  }

  test("KY-2721 s3") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("adjust_amt_cost_price", FloatType, nullable = true),
        StructField.apply("adjust_amt_retail_price", FloatType, nullable = true),
        StructField.apply("adjust_qty", FloatType, nullable = true),
        StructField.apply("brand_code", StringType, nullable = true),
        StructField.apply("brand_name", StringType, nullable = true),
        StructField.apply("brand_type", StringType, nullable = true),
        StructField.apply("buy_mbr_count_o2o_regist_30day", IntegerType, nullable = true),
        StructField.apply("buy_mbr_count_regist_30day", FloatType, nullable = true),
        StructField.apply("buy_mbr_count_regist_7day", FloatType, nullable = true),
        StructField.apply("buy_mbr_count_regist_day", IntegerType, nullable = true),
        StructField.apply("buy_new_mbr_repurchase_date", StringType, nullable = true),
        StructField.apply("buying_manager", StringType, nullable = true),
        StructField.apply("case_size", StringType, nullable = true),
        StructField.apply("channel_name", StringType, nullable = true),
        StructField.apply("channel_type_b2c", StringType, nullable = true),
        StructField.apply("channel_type_online_offline_cn", StringType, nullable = true),
        StructField.apply("cn_week_day_name", StringType, nullable = true),
        StructField.apply("cooked_disposed_qty", StringType, nullable = true),
        StructField.apply("cooked_stock_qty_added", StringType, nullable = true),
        StructField.apply("cooked_stock_qty_current", StringType, nullable = true),
        StructField.apply("date_en_week_day_shortnm", StringType, nullable = true),
        StructField.apply("disposal_amt_quality", StringType, nullable = true),
        StructField.apply("disposal_amt_write_off", StringType, nullable = true),
        StructField.apply("disposal_cost_quality", StringType, nullable = true),
        StructField.apply("disposal_cost_write_off", StringType, nullable = true),
        StructField.apply("disposal_qty_quality", StringType, nullable = true),
        StructField.apply("disposal_qty_write_off", StringType, nullable = true),
        StructField.apply("donation_amt_food_donation", StringType, nullable = true),
        StructField.apply("donation_cost_food_donation", StringType, nullable = true),
        StructField.apply("donation_qty_food_donation", StringType, nullable = true),
        StructField.apply("dt", StringType, nullable = true),
        StructField.apply("en_week_day_name", StringType, nullable = true),
        StructField.apply("etl_date", StringType, nullable = true),
        StructField.apply("expiration_date", StringType, nullable = true),
        StructField.apply("forecast_sales_store", StringType, nullable = true),
        StructField.apply("full_price_sales_amt_excl_tax", StringType, nullable = true),
        StructField.apply("full_price_sales_qty", StringType, nullable = true),
        StructField.apply("hour_period", StringType, nullable = true),
        StructField.apply("inventory_count_amt_inventory_in_out", StringType, nullable = true),
        StructField.apply("inventory_count_cost_inventory_in_out", StringType, nullable = true),
        StructField.apply("inventory_count_qty_inventory_in_out", StringType, nullable = true),
        StructField.apply("is_valid", StringType, nullable = true),
        StructField.apply("item_category", StringType, nullable = true),
        StructField.apply("item_category_name_zh", StringType, nullable = true),
        StructField.apply("item_name_en", StringType, nullable = true),
        StructField.apply("item_name_zh", StringType, nullable = true),
        StructField.apply("item_subcategory", StringType, nullable = true),
        StructField.apply("item_subcategory_name_zh", StringType, nullable = true),
        StructField.apply("known_loss_cost", StringType, nullable = true),
        StructField.apply("known_loss_qty", StringType, nullable = true),
        StructField.apply("known_loss_sales", StringType, nullable = true),
        StructField.apply("last_selling_point", StringType, nullable = true),
        StructField.apply("leave_liability", StringType, nullable = true),
        StructField.apply("loss_category_sales", StringType, nullable = true),
        StructField.apply("loss_category_sales_excl_new_store", StringType, nullable = true),
        StructField.apply("markdown_cost", StringType, nullable = true),
        StructField.apply("markdown_price_one", StringType, nullable = true),
        StructField.apply("markdown_price_three", StringType, nullable = true),
        StructField.apply("markdown_price_two", StringType, nullable = true),
        StructField.apply("markdown_qty", StringType, nullable = true),
        StructField.apply("markdown_sales_excl_tax_one", StringType, nullable = true),
        StructField.apply("markdown_sales_excl_tax_three", StringType, nullable = true),
        StructField.apply("markdown_sales_excl_tax_two", StringType, nullable = true),
        StructField.apply("markdown_units_one", StringType, nullable = true),
        StructField.apply("markdown_units_three", StringType, nullable = true),
        StructField.apply("markdown_units_two", StringType, nullable = true),
        StructField.apply("mbr_consumption_transaction", StringType, nullable = true),
        StructField.apply("merchant_close_day", StringType, nullable = true),
        StructField.apply("merchant_code_short_name", StringType, nullable = true),
        StructField.apply("merchant_name_en", StringType, nullable = true),
        StructField.apply("merchant_name_zh", StringType, nullable = true),
        StructField.apply("merchant_short_name", StringType, nullable = true),
        StructField.apply("merchant_stop_sale_day", StringType, nullable = true),
        StructField.apply("merchant_type", StringType, nullable = true),
        StructField.apply("month_id", StringType, nullable = true),
        StructField.apply("month_shortnm_en", StringType, nullable = true),
        StructField.apply("new_mbr_count", StringType, nullable = true),
        StructField.apply("new_mbr_count_cee", StringType, nullable = true),
        StructField.apply("new_member_buy_qty", StringType, nullable = true),
        StructField.apply("openning_day", StringType, nullable = true),
        StructField.apply("operating_category_name", StringType, nullable = true),
        StructField.apply("ordered_stock_qty", StringType, nullable = true),
        StructField.apply("ot_hour", StringType, nullable = true),
        StructField.apply("partial_decrease_adjustment_amt", StringType, nullable = true),
        StructField.apply("partial_decrease_adjustment_cost", StringType, nullable = true),
        StructField.apply("partial_decrease_adjustment_qty", StringType, nullable = true),
        StructField.apply("partial_increase_adjustment_amt", StringType, nullable = true),
        StructField.apply("partial_increase_adjustment_cost", StringType, nullable = true),
        StructField.apply("partial_increase_adjustment_qty", StringType, nullable = true),
        StructField.apply("pay_method_name", StringType, nullable = true),
        StructField.apply("payment_amt_pay_method", StringType, nullable = true),
        StructField.apply("pk_bi_channel_id", StringType, nullable = true),
        StructField.apply("pk_channel_type_online_offline_en", StringType, nullable = true),
        StructField.apply("pk_date_id", StringType, nullable = true),
        StructField.apply("pk_hour", StringType, nullable = true),
        StructField.apply("pk_inventory_markdown_type", StringType, nullable = true),
        StructField.apply("pk_is_self_flag", StringType, nullable = true),
        StructField.apply("pk_item_category_name", StringType, nullable = true),
        StructField.apply("pk_item_code", StringType, nullable = true),
        StructField.apply("pk_item_sub_category_name", StringType, nullable = true),
        StructField.apply("pk_mbr_id", StringType, nullable = true),
        StructField.apply("pk_merchant_code", StringType, nullable = true),
        StructField.apply("pk_operating_category", StringType, nullable = true),
        StructField.apply("pk_pay_method_code", StringType, nullable = true),
        StructField.apply("pltf_pkf_excl_refund_tax_jddj", StringType, nullable = true),
        StructField.apply("product_status", StringType, nullable = true),
        StructField.apply("product_status_name", StringType, nullable = true),
        StructField.apply("prom_price", StringType, nullable = true),
        StructField.apply("pt_fk_pk_date", StringType, nullable = true),
        StructField.apply("pt_hours", StringType, nullable = true),
        StructField.apply("push_order", StringType, nullable = true),
        StructField.apply("raw_stock_qty", StringType, nullable = true),
        StructField.apply("receipt_stock_qty", StringType, nullable = true),
        StructField.apply("refund_transaction_qty_itemtot", StringType, nullable = true),
        StructField.apply("refund_transaction_qty_new_mbr", StringType, nullable = true),
        StructField.apply("retail_price", StringType, nullable = true),
        StructField
          .apply("sales_amt_delivery_categorytot_penetration", StringType, nullable = true),
        StructField.apply(
          "sales_amt_delivery_excel_refund_tax_subcategorytot_penetration",
          StringType,
          nullable = true),
        StructField.apply("sales_amt_delivery_excl_refund_tax", StringType, nullable = true),
        StructField
          .apply("sales_amt_delivery_excl_refund_tax_offline", StringType, nullable = true),
        StructField.apply("sales_amt_delivery_excl_refund_tax_online", StringType, nullable = true),
        StructField.apply("sales_amt_delivery_excl_refund_tax_optot", StringType, nullable = true),
        StructField.apply(
          "sales_amt_delivery_excl_tax_categorytot_penetration",
          StringType,
          nullable = true),
        StructField.apply(
          "sales_amt_delivery_excl_tax_refund_subcategorytot_penetration",
          StringType,
          nullable = true),
        StructField.apply(
          "sales_amt_delivery_excl_tax_subcategorytot_penetration",
          StringType,
          nullable = true),
        StructField
          .apply("sales_amt_delivery_last_day_excl_refund_tax", StringType, nullable = true),
        StructField
          .apply("sales_amt_delivery_last_month_excl_refund_tax", StringType, nullable = true),
        StructField
          .apply("sales_amt_delivery_last_week_excl_refund_tax", StringType, nullable = true),
        StructField.apply(
          "sales_amt_delivery_last_week_excl_refund_tax_all_ss",
          StringType,
          nullable = true),
        StructField
          .apply("sales_amt_delivery_last_year_excl_refund_tax", StringType, nullable = true),
        StructField
          .apply("sales_amt_delivery_subcategorytot_penetration", StringType, nullable = true),
        StructField.apply(
          "sales_amt_excel_tax_refund_subcategorytot_penetration",
          StringType,
          nullable = true),
        StructField
          .apply("sales_amt_excl_refund_tax_categorytot_penetration", StringType, nullable = true),
        StructField
          .apply("sales_amt_for_sampling_delivery_excl_refund_tax", StringType, nullable = true),
        StructField.apply(
          "sales_amt_for_sampling_delivery_last_week_excl_refund_tax",
          StringType,
          nullable = true),
        StructField.apply("sales_amt_pay_excl_refund_tax", StringType, nullable = true),
        StructField.apply("sales_amt_pay_new_mbr_excl_refund_tax", StringType, nullable = true),
        StructField.apply("sales_amt_refund_tax_penetration", StringType, nullable = true),
        StructField
          .apply("sales_amt_return_tax_categorytot_penetration", StringType, nullable = true),
        StructField.apply(
          "sales_amt_week_diff_for_sampling_delivery_excl_refund_tax",
          StringType,
          nullable = true),
        StructField.apply("sales_qty_delivery", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_excl_refund", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_excl_refund_online", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_excl_refund_pos", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_excl_refund_pos_self", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_last_moth_excl_refund", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_last_week_excl_refund", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_last_year_excl_refund", StringType, nullable = true),
        StructField
          .apply("sales_qty_for_sampling_delivery_excl_refund", StringType, nullable = true),
        StructField.apply(
          "sales_qty_for_sampling_delivery_last_week_excl_refund",
          StringType,
          nullable = true),
        StructField.apply("sales_qty_pay_excl_refund", StringType, nullable = true),
        StructField.apply("satisfy_num", StringType, nullable = true),
        StructField.apply("satisfy_num_control", StringType, nullable = true),
        StructField.apply("secondment_hours", StringType, nullable = true),
        StructField.apply("shelf_lite", StringType, nullable = true),
        StructField.apply("sick_leave", StringType, nullable = true),
        StructField.apply("standard_working_hours", StringType, nullable = true),
        StructField.apply("start_date_week_name_start_monday", StringType, nullable = true),
        StructField.apply("start_date_week_name_start_wednesday", StringType, nullable = true),
        StructField.apply("stock_qty_10pm", StringType, nullable = true),
        StructField.apply("stock_qty_8pm", StringType, nullable = true),
        StructField.apply("supply_delivery_actual_qty", StringType, nullable = true),
        StructField.apply("total_hours", StringType, nullable = true),
        StructField.apply("total_satisfy_num", StringType, nullable = true),
        StructField.apply("total_satisfy_num_control", StringType, nullable = true),
        StructField.apply("transaction_qty_delivery_excl_return", StringType, nullable = true),
        StructField.apply(
          "transaction_qty_delivery_excl_return_categorytot_penetration",
          StringType,
          nullable = true),
        StructField
          .apply("transaction_qty_delivery_excl_return_itemtot", StringType, nullable = true),
        StructField.apply(
          "transaction_qty_delivery_excl_return_itemtot_offline",
          StringType,
          nullable = true),
        StructField.apply(
          "transaction_qty_delivery_excl_return_itemtot_online",
          StringType,
          nullable = true),
        StructField.apply(
          "transaction_qty_delivery_excl_return_itemtot_penetration",
          StringType,
          nullable = true),
        StructField.apply(
          "transaction_qty_delivery_excl_return_itemtot_yesterday",
          StringType,
          nullable = true),
        StructField.apply(
          "transaction_qty_delivery_return_categorytot_penetration",
          StringType,
          nullable = true),
        StructField
          .apply("transaction_qty_delivery_return_tax_penetration", StringType, nullable = true),
        StructField.apply(
          "transaction_qty_delivery_return_tax_subcategorytot_penetration",
          StringType,
          nullable = true),
        StructField
          .apply("transaction_qty_excl_returnd_operating_penetration", StringType, nullable = true),
        StructField.apply(
          "transaction_qty_excl_returnd_operatingtot_penetration",
          StringType,
          nullable = true),
        StructField.apply("transaction_qty_pay_excl_return_itemtot", StringType, nullable = true),
        StructField.apply("transaction_qty_pay_new_mbr", StringType, nullable = true),
        StructField.apply("transaction_qty_pay_new_mbr_excl_refund", StringType, nullable = true),
        StructField.apply("unknown_loss_cost", StringType, nullable = true),
        StructField.apply("unknown_loss_qty", StringType, nullable = true),
        StructField.apply("unknown_loss_sales", StringType, nullable = true),
        StructField.apply("unrestricted_stock_qty_day_before", StringType, nullable = true),
        StructField.apply("week_name_start_monday", StringType, nullable = true),
        StructField.apply("week_name_start_wednesday", StringType, nullable = true),
        StructField.apply("week_start_monday", StringType, nullable = true),
        StructField.apply("week_start_wednesday", StringType, nullable = true),
        StructField.apply("work_hour_actual", StringType, nullable = true),
        StructField.apply("year_id", StringType, nullable = true)
      ))

    val df = spark.read
      .schema(schema)
      .parquet("s3a://biaobiao/")
      .toDF()

    df.createTempView("aoleqi")
    val sql =
      """
        | select * from `aoleqi` limit 100
        |""".stripMargin

    var time = System.nanoTime()
    spark.sql(sql).show()
    println(System.nanoTime() - time)
  }

  test("KY-2721 minio") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("adjust_amt_cost_price", FloatType, nullable = true),
        StructField.apply("adjust_amt_retail_price", FloatType, nullable = true),
        StructField.apply("adjust_qty", FloatType, nullable = true),
        StructField.apply("brand_code", StringType, nullable = true),
        StructField.apply("brand_name", StringType, nullable = true),
        StructField.apply("brand_type", StringType, nullable = true),
        StructField.apply("buy_mbr_count_o2o_regist_30day", IntegerType, nullable = true),
        StructField.apply("buy_mbr_count_regist_30day", FloatType, nullable = true),
        StructField.apply("buy_mbr_count_regist_7day", FloatType, nullable = true),
        StructField.apply("buy_mbr_count_regist_day", IntegerType, nullable = true),
        StructField.apply("buy_new_mbr_repurchase_date", StringType, nullable = true),
        StructField.apply("buying_manager", StringType, nullable = true),
        StructField.apply("case_size", StringType, nullable = true),
        StructField.apply("channel_name", StringType, nullable = true),
        StructField.apply("channel_type_b2c", StringType, nullable = true),
        StructField.apply("channel_type_online_offline_cn", StringType, nullable = true),
        StructField.apply("cn_week_day_name", StringType, nullable = true),
        StructField.apply("cooked_disposed_qty", StringType, nullable = true),
        StructField.apply("cooked_stock_qty_added", StringType, nullable = true),
        StructField.apply("cooked_stock_qty_current", StringType, nullable = true),
        StructField.apply("date_en_week_day_shortnm", StringType, nullable = true),
        StructField.apply("disposal_amt_quality", StringType, nullable = true),
        StructField.apply("disposal_amt_write_off", StringType, nullable = true),
        StructField.apply("disposal_cost_quality", StringType, nullable = true),
        StructField.apply("disposal_cost_write_off", StringType, nullable = true),
        StructField.apply("disposal_qty_quality", StringType, nullable = true),
        StructField.apply("disposal_qty_write_off", StringType, nullable = true),
        StructField.apply("donation_amt_food_donation", StringType, nullable = true),
        StructField.apply("donation_cost_food_donation", StringType, nullable = true),
        StructField.apply("donation_qty_food_donation", StringType, nullable = true),
        StructField.apply("dt", StringType, nullable = true),
        StructField.apply("en_week_day_name", StringType, nullable = true),
        StructField.apply("etl_date", StringType, nullable = true),
        StructField.apply("expiration_date", StringType, nullable = true),
        StructField.apply("forecast_sales_store", StringType, nullable = true),
        StructField.apply("full_price_sales_amt_excl_tax", StringType, nullable = true),
        StructField.apply("full_price_sales_qty", StringType, nullable = true),
        StructField.apply("hour_period", StringType, nullable = true),
        StructField.apply("inventory_count_amt_inventory_in_out", StringType, nullable = true),
        StructField.apply("inventory_count_cost_inventory_in_out", StringType, nullable = true),
        StructField.apply("inventory_count_qty_inventory_in_out", StringType, nullable = true),
        StructField.apply("is_valid", StringType, nullable = true),
        StructField.apply("item_category", StringType, nullable = true),
        StructField.apply("item_category_name_zh", StringType, nullable = true),
        StructField.apply("item_name_en", StringType, nullable = true),
        StructField.apply("item_name_zh", StringType, nullable = true),
        StructField.apply("item_subcategory", StringType, nullable = true),
        StructField.apply("item_subcategory_name_zh", StringType, nullable = true),
        StructField.apply("known_loss_cost", StringType, nullable = true),
        StructField.apply("known_loss_qty", StringType, nullable = true),
        StructField.apply("known_loss_sales", StringType, nullable = true),
        StructField.apply("last_selling_point", StringType, nullable = true),
        StructField.apply("leave_liability", StringType, nullable = true),
        StructField.apply("loss_category_sales", StringType, nullable = true),
        StructField.apply("loss_category_sales_excl_new_store", StringType, nullable = true),
        StructField.apply("markdown_cost", StringType, nullable = true),
        StructField.apply("markdown_price_one", StringType, nullable = true),
        StructField.apply("markdown_price_three", StringType, nullable = true),
        StructField.apply("markdown_price_two", StringType, nullable = true),
        StructField.apply("markdown_qty", StringType, nullable = true),
        StructField.apply("markdown_sales_excl_tax_one", StringType, nullable = true),
        StructField.apply("markdown_sales_excl_tax_three", StringType, nullable = true),
        StructField.apply("markdown_sales_excl_tax_two", StringType, nullable = true),
        StructField.apply("markdown_units_one", StringType, nullable = true),
        StructField.apply("markdown_units_three", StringType, nullable = true),
        StructField.apply("markdown_units_two", StringType, nullable = true),
        StructField.apply("mbr_consumption_transaction", StringType, nullable = true),
        StructField.apply("merchant_close_day", StringType, nullable = true),
        StructField.apply("merchant_code_short_name", StringType, nullable = true),
        StructField.apply("merchant_name_en", StringType, nullable = true),
        StructField.apply("merchant_name_zh", StringType, nullable = true),
        StructField.apply("merchant_short_name", StringType, nullable = true),
        StructField.apply("merchant_stop_sale_day", StringType, nullable = true),
        StructField.apply("merchant_type", StringType, nullable = true),
        StructField.apply("month_id", StringType, nullable = true),
        StructField.apply("month_shortnm_en", StringType, nullable = true),
        StructField.apply("new_mbr_count", StringType, nullable = true),
        StructField.apply("new_mbr_count_cee", StringType, nullable = true),
        StructField.apply("new_member_buy_qty", StringType, nullable = true),
        StructField.apply("openning_day", StringType, nullable = true),
        StructField.apply("operating_category_name", StringType, nullable = true),
        StructField.apply("ordered_stock_qty", StringType, nullable = true),
        StructField.apply("ot_hour", StringType, nullable = true),
        StructField.apply("partial_decrease_adjustment_amt", StringType, nullable = true),
        StructField.apply("partial_decrease_adjustment_cost", StringType, nullable = true),
        StructField.apply("partial_decrease_adjustment_qty", StringType, nullable = true),
        StructField.apply("partial_increase_adjustment_amt", StringType, nullable = true),
        StructField.apply("partial_increase_adjustment_cost", StringType, nullable = true),
        StructField.apply("partial_increase_adjustment_qty", StringType, nullable = true),
        StructField.apply("pay_method_name", StringType, nullable = true),
        StructField.apply("payment_amt_pay_method", StringType, nullable = true),
        StructField.apply("pk_bi_channel_id", StringType, nullable = true),
        StructField.apply("pk_channel_type_online_offline_en", StringType, nullable = true),
        StructField.apply("pk_date_id", StringType, nullable = true),
        StructField.apply("pk_hour", StringType, nullable = true),
        StructField.apply("pk_inventory_markdown_type", StringType, nullable = true),
        StructField.apply("pk_is_self_flag", StringType, nullable = true),
        StructField.apply("pk_item_category_name", StringType, nullable = true),
        StructField.apply("pk_item_code", StringType, nullable = true),
        StructField.apply("pk_item_sub_category_name", StringType, nullable = true),
        StructField.apply("pk_mbr_id", StringType, nullable = true),
        StructField.apply("pk_merchant_code", StringType, nullable = true),
        StructField.apply("pk_operating_category", StringType, nullable = true),
        StructField.apply("pk_pay_method_code", StringType, nullable = true),
        StructField.apply("pltf_pkf_excl_refund_tax_jddj", StringType, nullable = true),
        StructField.apply("product_status", StringType, nullable = true),
        StructField.apply("product_status_name", StringType, nullable = true),
        StructField.apply("prom_price", StringType, nullable = true),
        StructField.apply("pt_fk_pk_date", StringType, nullable = true),
        StructField.apply("pt_hours", StringType, nullable = true),
        StructField.apply("push_order", StringType, nullable = true),
        StructField.apply("raw_stock_qty", StringType, nullable = true),
        StructField.apply("receipt_stock_qty", StringType, nullable = true),
        StructField.apply("refund_transaction_qty_itemtot", StringType, nullable = true),
        StructField.apply("refund_transaction_qty_new_mbr", StringType, nullable = true),
        StructField.apply("retail_price", StringType, nullable = true),
        StructField
          .apply("sales_amt_delivery_categorytot_penetration", StringType, nullable = true),
        StructField.apply(
          "sales_amt_delivery_excel_refund_tax_subcategorytot_penetration",
          StringType,
          nullable = true),
        StructField.apply("sales_amt_delivery_excl_refund_tax", StringType, nullable = true),
        StructField
          .apply("sales_amt_delivery_excl_refund_tax_offline", StringType, nullable = true),
        StructField.apply("sales_amt_delivery_excl_refund_tax_online", StringType, nullable = true),
        StructField.apply("sales_amt_delivery_excl_refund_tax_optot", StringType, nullable = true),
        StructField.apply(
          "sales_amt_delivery_excl_tax_categorytot_penetration",
          StringType,
          nullable = true),
        StructField.apply(
          "sales_amt_delivery_excl_tax_refund_subcategorytot_penetration",
          StringType,
          nullable = true),
        StructField.apply(
          "sales_amt_delivery_excl_tax_subcategorytot_penetration",
          StringType,
          nullable = true),
        StructField
          .apply("sales_amt_delivery_last_day_excl_refund_tax", StringType, nullable = true),
        StructField
          .apply("sales_amt_delivery_last_month_excl_refund_tax", StringType, nullable = true),
        StructField
          .apply("sales_amt_delivery_last_week_excl_refund_tax", StringType, nullable = true),
        StructField.apply(
          "sales_amt_delivery_last_week_excl_refund_tax_all_ss",
          StringType,
          nullable = true),
        StructField
          .apply("sales_amt_delivery_last_year_excl_refund_tax", StringType, nullable = true),
        StructField
          .apply("sales_amt_delivery_subcategorytot_penetration", StringType, nullable = true),
        StructField.apply(
          "sales_amt_excel_tax_refund_subcategorytot_penetration",
          StringType,
          nullable = true),
        StructField
          .apply("sales_amt_excl_refund_tax_categorytot_penetration", StringType, nullable = true),
        StructField
          .apply("sales_amt_for_sampling_delivery_excl_refund_tax", StringType, nullable = true),
        StructField.apply(
          "sales_amt_for_sampling_delivery_last_week_excl_refund_tax",
          StringType,
          nullable = true),
        StructField.apply("sales_amt_pay_excl_refund_tax", StringType, nullable = true),
        StructField.apply("sales_amt_pay_new_mbr_excl_refund_tax", StringType, nullable = true),
        StructField.apply("sales_amt_refund_tax_penetration", StringType, nullable = true),
        StructField
          .apply("sales_amt_return_tax_categorytot_penetration", StringType, nullable = true),
        StructField.apply(
          "sales_amt_week_diff_for_sampling_delivery_excl_refund_tax",
          StringType,
          nullable = true),
        StructField.apply("sales_qty_delivery", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_excl_refund", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_excl_refund_online", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_excl_refund_pos", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_excl_refund_pos_self", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_last_moth_excl_refund", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_last_week_excl_refund", StringType, nullable = true),
        StructField.apply("sales_qty_delivery_last_year_excl_refund", StringType, nullable = true),
        StructField
          .apply("sales_qty_for_sampling_delivery_excl_refund", StringType, nullable = true),
        StructField.apply(
          "sales_qty_for_sampling_delivery_last_week_excl_refund",
          StringType,
          nullable = true),
        StructField.apply("sales_qty_pay_excl_refund", StringType, nullable = true),
        StructField.apply("satisfy_num", StringType, nullable = true),
        StructField.apply("satisfy_num_control", StringType, nullable = true),
        StructField.apply("secondment_hours", StringType, nullable = true),
        StructField.apply("shelf_lite", StringType, nullable = true),
        StructField.apply("sick_leave", StringType, nullable = true),
        StructField.apply("standard_working_hours", StringType, nullable = true),
        StructField.apply("start_date_week_name_start_monday", StringType, nullable = true),
        StructField.apply("start_date_week_name_start_wednesday", StringType, nullable = true),
        StructField.apply("stock_qty_10pm", StringType, nullable = true),
        StructField.apply("stock_qty_8pm", StringType, nullable = true),
        StructField.apply("supply_delivery_actual_qty", StringType, nullable = true),
        StructField.apply("total_hours", StringType, nullable = true),
        StructField.apply("total_satisfy_num", StringType, nullable = true),
        StructField.apply("total_satisfy_num_control", StringType, nullable = true),
        StructField.apply("transaction_qty_delivery_excl_return", StringType, nullable = true),
        StructField.apply(
          "transaction_qty_delivery_excl_return_categorytot_penetration",
          StringType,
          nullable = true),
        StructField
          .apply("transaction_qty_delivery_excl_return_itemtot", StringType, nullable = true),
        StructField.apply(
          "transaction_qty_delivery_excl_return_itemtot_offline",
          StringType,
          nullable = true),
        StructField.apply(
          "transaction_qty_delivery_excl_return_itemtot_online",
          StringType,
          nullable = true),
        StructField.apply(
          "transaction_qty_delivery_excl_return_itemtot_penetration",
          StringType,
          nullable = true),
        StructField.apply(
          "transaction_qty_delivery_excl_return_itemtot_yesterday",
          StringType,
          nullable = true),
        StructField.apply(
          "transaction_qty_delivery_return_categorytot_penetration",
          StringType,
          nullable = true),
        StructField
          .apply("transaction_qty_delivery_return_tax_penetration", StringType, nullable = true),
        StructField.apply(
          "transaction_qty_delivery_return_tax_subcategorytot_penetration",
          StringType,
          nullable = true),
        StructField
          .apply("transaction_qty_excl_returnd_operating_penetration", StringType, nullable = true),
        StructField.apply(
          "transaction_qty_excl_returnd_operatingtot_penetration",
          StringType,
          nullable = true),
        StructField.apply("transaction_qty_pay_excl_return_itemtot", StringType, nullable = true),
        StructField.apply("transaction_qty_pay_new_mbr", StringType, nullable = true),
        StructField.apply("transaction_qty_pay_new_mbr_excl_refund", StringType, nullable = true),
        StructField.apply("unknown_loss_cost", StringType, nullable = true),
        StructField.apply("unknown_loss_qty", StringType, nullable = true),
        StructField.apply("unknown_loss_sales", StringType, nullable = true),
        StructField.apply("unrestricted_stock_qty_day_before", StringType, nullable = true),
        StructField.apply("week_name_start_monday", StringType, nullable = true),
        StructField.apply("week_name_start_wednesday", StringType, nullable = true),
        StructField.apply("week_start_monday", StringType, nullable = true),
        StructField.apply("week_start_wednesday", StringType, nullable = true),
        StructField.apply("work_hour_actual", StringType, nullable = true),
        StructField.apply("year_id", StringType, nullable = true)
      ))

    val df = spark.read
      .schema(schema)
      .parquet("s3a://test.zen.qa//parquet227.parquet")
      .toDF()

    df.createTempView("parquet227")
    val sql =
      """
        | select * from `parquet227` limit 100
        |""".stripMargin

    var time = System.nanoTime()
    spark.sql(sql).show()
    println(System.nanoTime() - time)
  }

  test("KY-2721 minio2") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("l_orderkey", LongType, nullable = true),
        StructField.apply("l_partkey", LongType, nullable = true),
        StructField.apply("l_suppkey", LongType, nullable = true),
        StructField.apply("l_linenumber", LongType, nullable = true),
        StructField.apply("l_quantity", DoubleType, nullable = true),
        StructField.apply("l_extendedprice", DoubleType, nullable = true),
        StructField.apply("l_discount", DoubleType, nullable = true),
        StructField.apply("l_tax", DoubleType, nullable = true),
        StructField.apply("l_returnflag", StringType, nullable = true),
        StructField.apply("l_linestatus", StringType, nullable = true),
        StructField.apply("l_shipdate", DateType, nullable = true),
        StructField.apply("l_commitdate", DateType, nullable = true),
        StructField.apply("l_receiptdate", DateType, nullable = true),
        StructField.apply("l_shipinstruct", StringType, nullable = true),
        StructField.apply("l_shipmode", StringType, nullable = true),
        StructField.apply("l_comment", StringType, nullable = true)
      ))

    val df = spark.read
      .schema(schema)
      .parquet(
        "s3a://test.zen.qa/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet")
      .toDF()

    df.createTempView("lineitem")
    val sql =
      """
        | select * from `lineitem` limit 100
        |""".stripMargin

    var time = System.nanoTime()
    spark.sql(sql).show()
    println(System.nanoTime() - time)

    time = System.nanoTime()
    withSQLConf(vanillaSparkConfs(): _*) {
      spark.sql(sql).show()
    }

    println(System.nanoTime() - time)
  }

  test("KY-2734 interval") {
    val schema_retail_stores_transaction = StructType.apply(
      Seq(
        StructField.apply("日期", StringType, nullable = true),
        StructField.apply("分组", StringType, nullable = true),
        StructField.apply("最大间隔时间", IntegerType, nullable = true),
        StructField.apply("最小间隔时间", IntegerType, nullable = true),
        StructField.apply("人均间隔时间", DoubleType, nullable = true),
        StructField.apply("间隔时间中位数", IntegerType, nullable = true),
        StructField.apply("间隔时间第一四分位数", IntegerType, nullable = true),
        StructField.apply("间隔时间第三四分位数", IntegerType, nullable = true),
        StructField.apply("平均间隔时间", DoubleType, nullable = true),
        StructField.apply("间隔数", IntegerType, nullable = true),
        StructField.apply("人均间隔数", DoubleType, nullable = true),
        StructField.apply("人数", IntegerType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema_retail_stores_transaction)
      .csv("s3a://test.zen.qa/interval/")
      .toDF()
      .createTempView("interval")

    val sql =
      """
        |select count(*) count_all
        |from interval
        |limit 1
        |""".stripMargin

    var time = System.nanoTime()
    time = System.nanoTime()
    val df = spark.sql(sql).toDF()

    print(df.queryExecution.executedPlan)
    df.show()
    println("gluten time: " + (System.nanoTime() - time) + "ns")
  }

  test("test_blank_2") {
    val schema_retail_stores_transaction = StructType.apply(
      Seq(
        StructField.apply("id", IntegerType, nullable = true),
        StructField.apply("col1", DateType, nullable = true),
        StructField.apply("col2", TimestampType, nullable = true),
        StructField.apply("col3", DateType, nullable = true),
        StructField.apply("col4", TimestampType, nullable = true),
        StructField.apply("col5", BooleanType, nullable = true),
        StructField.apply("col6", BooleanType, nullable = true),
        StructField.apply("col7", IntegerType, nullable = true),
        StructField.apply("col8", StringType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema_retail_stores_transaction)
      .csv(zenDataPath + "/后面空格.csv")
      .toDF()
      .createTempView("test_blank_2")

    val sql =
      """
        |select * from test_blank_2
        |""".stripMargin

    var time = System.nanoTime()
    time = System.nanoTime()
    val df = spark.sql(sql).toDF()

    print(df.queryExecution.executedPlan)
    df.show()
    println("gluten time: " + (System.nanoTime() - time) + "ns")
  }

  test("transaction_data_1") {
    val transaction_data_1 = StructType.apply(
      Seq(
        StructField.apply("region", StringType, nullable = true),
        StructField.apply("country", StringType, nullable = true),
        StructField.apply("item_type", StringType, nullable = true),
        StructField.apply("sales_channel", StringType, nullable = true),
        StructField.apply("order_priority", StringType, nullable = true),
        StructField.apply("order_year", IntegerType, nullable = true),
        StructField.apply("order_month", StringType, nullable = true),
        StructField.apply("order_date", DateType, nullable = true),
        StructField.apply("order_id", IntegerType, nullable = true),
        StructField.apply("ship_date", DateType, nullable = true),
        StructField.apply("units_sold", IntegerType, nullable = true),
        StructField.apply("unit_price", DoubleType, nullable = true),
        StructField.apply("unit_cost", DoubleType, nullable = true),
        StructField.apply("total_revenue", DoubleType, nullable = true),
        StructField.apply("total_cost", DoubleType, nullable = true),
        StructField.apply("total_profit", DoubleType, nullable = true)
      ))

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(transaction_data_1)
      .csv(zenDataPath + "/transaction_data_1.csv")
      .toDF()
      .createTempView("transaction_data_1")

    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(transaction_data_1)
      .csv(zenDataPath + "/transaction_data_2.csv")
      .toDF()
      .createTempView("transaction_data_2")

    val sql =
      """
        |with
        |_v_tran_1_join_tran_2 as (
        |select transaction_data_1.region transaction_data_1_region, transaction_data_1.country transaction_data_1_country,
        | transaction_data_1.item_type transaction_data_1_item_type, transaction_data_1.sales_channel transaction_data_1_sales_channel,
        | transaction_data_1.order_priority transaction_data_1_order_priority,
        | transaction_data_1.order_year transaction_data_1_order_year,
        | transaction_data_1.order_month transaction_data_1_order_month,
        | transaction_data_1.order_date transaction_data_1_order_date,
        |  transaction_data_1.order_id transaction_data_1_order_id, transaction_data_1.ship_date transaction_data_1_ship_date,
        | transaction_data_1.units_sold transaction_data_1_units_sold,
        | transaction_data_1.unit_price transaction_data_1_unit_price, transaction_data_1.unit_cost transaction_data_1_unit_cost,
        |  transaction_data_1.total_revenue transaction_data_1_total_revenue,
        |  transaction_data_1.total_cost transaction_data_1_total_cost, transaction_data_1.total_profit transaction_data_1_total_profit,
        |  transaction_data_2.region transaction_data_2_region, transaction_data_2.country transaction_data_2_country,
        |  transaction_data_2.item_type transaction_data_2_item_type,
        |  transaction_data_2.sales_channel transaction_data_2_sales_channel,
        |   transaction_data_2.order_priority transaction_data_2_order_priority,
        |   transaction_data_2.order_year transaction_data_2_order_year,
        |    transaction_data_2.order_month transaction_data_2_order_month,
        |    transaction_data_2.order_date transaction_data_2_order_date,
        |    transaction_data_2.order_id transaction_data_2_order_id, transaction_data_2.ship_date transaction_data_2_ship_date,
        |    transaction_data_2.units_sold transaction_data_2_units_sold,
        |    transaction_data_2.unit_price transaction_data_2_unit_price,
        |    transaction_data_2.unit_cost transaction_data_2_unit_cost, transaction_data_2.total_revenue transaction_data_2_total_revenue,
        |     transaction_data_2.total_cost transaction_data_2_total_cost,
        |     transaction_data_2.total_profit transaction_data_2_total_profit
        |from transaction_data_1
        |  left join transaction_data_2 on transaction_data_1.region = transaction_data_2.region
        |)
        |
        |select transaction_data_1_order_date, sum(transaction_data_1_order_year) big
        |from _v_tran_1_join_tran_2 tran_1_join_tran_2
        |group by transaction_data_1_order_date
        |order by transaction_data_1_order_date desc
        |limit 1000
        |""".stripMargin

    var time = System.nanoTime()
    time = System.nanoTime()

//    print(df.queryExecution.executedPlan)
    var i = 1000000000
    while (i > 0) {
      val df = spark.sql(sql).toDF()
      df.show()
      i = i - 1
      println(i)
    }

    println("gluten time: " + (System.nanoTime() - time) + "ns")
  }

  test("parquet pur") {
    val transaction_data_1 = StructType.apply(
      Seq(
        StructField.apply("14", StringType, nullable = true),
        StructField.apply("9", LongType, nullable = true),
        StructField.apply("100000", LongType, nullable = true),
        StructField.apply("100002", DoubleType, nullable = true)
      ))

    spark.read
      .schema(transaction_data_1)
      .parquet(zenDataPath + "/part-00000-9a727bd0-f601-4b09-b3c3-5a3f6fc500c6-c000.snappy.parquet")
      .toDF()
      .createTempView("p1")

    val sql =
      """
        | select count(*) from p1
        | -- where `14`='EUROPE'
        |""".stripMargin

    var time = System.nanoTime()
    time = System.nanoTime()
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      df => {
        df.show()
      })

    println("gluten time: " + (System.nanoTime() - time) + "ns")
    sleep(10000000)
  }

}

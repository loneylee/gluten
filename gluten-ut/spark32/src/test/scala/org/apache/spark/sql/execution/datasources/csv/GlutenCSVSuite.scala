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
package org.apache.spark.sql.execution.datasources.csv

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, FakeFileSystemRequiringDSOption, GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.internal.SQLConf

class GlutenCSVSuite extends CSVSuite with GlutenSQLTestsBaseTrait {

  /** Returns full path to the given file in the resource folder */
  override protected def testFile(fileName: String): String = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources").toString + "/" + fileName
  }
  private val carsFile = "test-data/cars.csv"
  private val carsAltFile = "test-data/cars-alternative.csv"

  override def sparkConf: SparkConf =
    super
      .sparkConf
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_settings.date_time_input_format",
        "best_effort_us")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_settings.use_excel_serialization", "true")

  private def verifyCars(
                          df: DataFrame,
                          withHeader: Boolean,
                          numCars: Int = 3,
                          numFields: Int = 5,
                          checkHeader: Boolean = true,
                          checkValues: Boolean = true,
                          checkTypes: Boolean = false): Unit = {

    val numColumns = numFields
    val numRows = if (withHeader) numCars else numCars + 1
    // schema
    assert(df.schema.fieldNames.length === numColumns)
    assert(df.count === numRows)

    if (checkHeader) {
      if (withHeader) {
        assert(df.schema.fieldNames === Array("year", "make", "model", "comment", "blank"))
      } else {
        assert(df.schema.fieldNames === Array("_c0", "_c1", "_c2", "_c3", "_c4"))
      }
    }

    if (checkValues) {
      val yearValues = List("2012", "1997", "2015")
      val actualYears = if (!withHeader) "year" :: yearValues else yearValues
      val years = if (withHeader) df.select("year").collect() else df.select("_c0").collect()

      years.zipWithIndex.foreach { case (year, index) =>
        if (checkTypes) {
          assert(year === Row(actualYears(index).toInt))
        } else {
          assert(year === Row(actualYears(index)))
        }
      }
    }
  }

  test("2 simple csv test") {
    val cars = spark
      .read
      .format("csv")
      .option("header", "false")
      .load(testFile(carsFile))

    verifyCars(cars, withHeader = false, checkTypes = false)
  }

  test(s"2 Propagate Hadoop configs from csv options to underlying file system") {
    withSQLConf(
      "fs.file.impl" -> classOf[FakeFileSystemRequiringDSOption].getName,
      "fs.file.impl.disable.cache" -> "true") {
      Seq(false, true).foreach { mergeSchema =>
        withTempPath { dir =>
          val path = dir.getAbsolutePath
          val conf = Map("ds_option" -> "value", "mergeSchema" -> mergeSchema.toString)
          inputDataset
            .write
            .options(conf)
            .format(dataSourceFormat)
            .save(path)
          Seq(path, "file:" + path.stripPrefix("file:")).foreach { p =>
            val readback = spark
              .read
              .options(conf)
              .format(dataSourceFormat)
              .load(p)
            // Checks that read doesn't throw the exception from `FakeFileSystemRequiringDSOption`
            readback.write.mode("overwrite").format("noop").save()
          }
        }
      }
    }
  }

  test("1 test with alternative delimiter and quote") {
    val cars = spark.read
      .format("csv")
      .options(Map("quote" -> "\'", "delimiter" -> "|", "header" -> "true"))
      .load(testFile(carsAltFile))

    verifyCars(cars, withHeader = true)
  }
}

class GlutenCSVv1Suite extends GlutenCSVSuite {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "csv")
}

class GlutenCSVv2Suite extends GlutenCSVSuite {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}

class GlutenCSVLegacyTimeParserSuite extends GlutenCSVSuite {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.LEGACY_TIME_PARSER_POLICY, "legacy")
}

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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.GlutenTestsTrait
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{Decimal, _}

import java.sql.Date

class GlutenCastSuite extends CastSuite with GlutenTestsTrait {


  override def conf: SQLConf = super.conf

  override def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): CastBase = {
    v match {
      case lit: Expression =>
        logDebug(s"Cast from: ${lit.dataType.typeName}, to: ${targetType.typeName}")
        Cast(lit, targetType, timeZoneId)
      case _ =>
        val lit = Literal(v)
        logDebug(s"Cast from: ${lit.dataType.typeName}, to: ${targetType.typeName}")
        Cast(lit, targetType, timeZoneId)
    }
  }

  // Register UDT For test("SPARK-32828")
  UDTRegistration.register(classOf[IExampleBaseType].getName, classOf[ExampleBaseTypeUDT].getName)
  UDTRegistration.register(classOf[IExampleSubType].getName, classOf[ExampleSubTypeUDT].getName)

  test(GLUTEN_TEST + "missing cases - from boolean") {
    (DataTypeTestUtils.numericTypeWithoutDecimal + BooleanType).foreach {
      t =>
        t match {
          case BooleanType =>
            checkEvaluation(cast(cast(true, BooleanType), t), true)
            checkEvaluation(cast(cast(false, BooleanType), t), false)
          case _ =>
            checkEvaluation(cast(cast(true, BooleanType), t), 1)
            checkEvaluation(cast(cast(false, BooleanType), t), 0)
        }

    }
  }

  test(GLUTEN_TEST + "missing cases - from byte") {
    DataTypeTestUtils.numericTypeWithoutDecimal.foreach {
      t =>
        checkEvaluation(cast(cast(0, ByteType), t), 0)
        checkEvaluation(cast(cast(-1, ByteType), t), -1)
        checkEvaluation(cast(cast(1, ByteType), t), 1)
    }
  }

  test(GLUTEN_TEST + "missing cases - from short") {
    DataTypeTestUtils.numericTypeWithoutDecimal.foreach {
      t =>
        checkEvaluation(cast(cast(0, ShortType), t), 0)
        checkEvaluation(cast(cast(-1, ShortType), t), -1)
        checkEvaluation(cast(cast(1, ShortType), t), 1)
    }
  }

  test("missing cases - date self check") {
    val d = Date.valueOf("1970-01-01")
    checkEvaluation(cast(d, DateType), d)
  }

  test("3 casting to fixed-precision decimals") {
    withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
      assert(cast(Decimal("1003"), DecimalType(3, -1)).nullable)
      assert(cast(Decimal("1003"), DecimalType(4, -1)).nullable === false)
      assert(cast(Decimal("995"), DecimalType(2, -1)).nullable)
      assert(cast(Decimal("995"), DecimalType(3, -1)).nullable === false)

      checkEvaluation(cast(Decimal("1003"), DecimalType(3, -1)), Decimal(1000))
      checkEvaluation(cast(Decimal("1003"), DecimalType(2, -2)), Decimal(1000))
      checkEvaluation(cast(Decimal("1003"), DecimalType(1, -2)), null)
      checkEvaluation(cast(Decimal("1003"), DecimalType(2, -1)), null)

      checkEvaluation(cast(Decimal("995"), DecimalType(3, -1)), Decimal(1000))
      checkEvaluation(cast(Decimal("995"), DecimalType(2, -2)), Decimal(1000))
      checkEvaluation(cast(Decimal("995"), DecimalType(2, -1)), null)
      checkEvaluation(cast(Decimal("995"), DecimalType(1, -2)), null)
    }
  }

  test("2 casting to fixed-precision decimals") {
//    assert(cast(123, DecimalType.USER_DEFAULT).nullable === false)
//    assert(cast(10.03f, DecimalType.SYSTEM_DEFAULT).nullable)
//    assert(cast(10.03, DecimalType.SYSTEM_DEFAULT).nullable)
//    assert(cast(Decimal(10.03), DecimalType.SYSTEM_DEFAULT).nullable === false)
//
//    assert(cast(123, DecimalType(2, 1)).nullable)
//    assert(cast(10.03f, DecimalType(2, 1)).nullable)
//    assert(cast(10.03, DecimalType(2, 1)).nullable)
//    assert(cast(Decimal(10.03), DecimalType(2, 1)).nullable)
//
//    assert(cast(123, DecimalType.IntDecimal).nullable === false)
//    assert(cast(10.03f, DecimalType.FloatDecimal).nullable)
//    assert(cast(10.03, DecimalType.DoubleDecimal).nullable)
//    assert(cast(Decimal(10.03), DecimalType(4, 2)).nullable === false)
//    assert(cast(Decimal(10.03), DecimalType(5, 3)).nullable === false)
//
//    assert(cast(Decimal(10.03), DecimalType(3, 1)).nullable)
//    assert(cast(Decimal(10.03), DecimalType(4, 1)).nullable === false)
//    assert(cast(Decimal(9.95), DecimalType(2, 1)).nullable)
//    assert(cast(Decimal(9.95), DecimalType(3, 1)).nullable === false)
//
//    assert(cast(true, DecimalType.SYSTEM_DEFAULT).nullable === false)
//    assert(cast(true, DecimalType(1, 1)).nullable)

//    checkEvaluation(cast(10.03, DecimalType.SYSTEM_DEFAULT), Decimal(10.03))
    checkEvaluation(cast(10.03, DecimalType(4, 2)), Decimal(10.03))
//    checkEvaluation(cast(10.03, DecimalType(3, 1)), Decimal(10.0))
//    checkEvaluation(cast(10.03, DecimalType(2, 0)), Decimal(10))
//    checkEvaluation(cast(10.03, DecimalType(1, 0)), null)
//    checkEvaluation(cast(10.03, DecimalType(2, 1)), null)
//    checkEvaluation(cast(10.03, DecimalType(3, 2)), null)
//    checkEvaluation(cast(Decimal(10.03), DecimalType(3, 1)), Decimal(10.0))
//    checkEvaluation(cast(Decimal(10.03), DecimalType(3, 2)), null)

//    checkEvaluation(cast(10.05, DecimalType.SYSTEM_DEFAULT), Decimal(10.05))
//    checkEvaluation(cast(10.05, DecimalType(4, 2)), Decimal(10.05))
//    checkEvaluation(cast(10.05, DecimalType(3, 1)), Decimal(10.1))
//    checkEvaluation(cast(10.05, DecimalType(2, 0)), Decimal(10))
//    checkEvaluation(cast(10.05, DecimalType(1, 0)), null)
//    checkEvaluation(cast(10.05, DecimalType(2, 1)), null)
//    checkEvaluation(cast(10.05, DecimalType(3, 2)), null)
//    checkEvaluation(cast(Decimal(10.05), DecimalType(3, 1)), Decimal(10.1))
//    checkEvaluation(cast(Decimal(10.05), DecimalType(3, 2)), null)
//
//    checkEvaluation(cast(9.95, DecimalType(3, 2)), Decimal(9.95))
//    checkEvaluation(cast(9.95, DecimalType(3, 1)), Decimal(10.0))
//    checkEvaluation(cast(9.95, DecimalType(2, 0)), Decimal(10))
//    checkEvaluation(cast(9.95, DecimalType(2, 1)), null)
//    checkEvaluation(cast(9.95, DecimalType(1, 0)), null)
//    checkEvaluation(cast(Decimal(9.95), DecimalType(3, 1)), Decimal(10.0))
//    checkEvaluation(cast(Decimal(9.95), DecimalType(1, 0)), null)
//
//    checkEvaluation(cast(-9.95, DecimalType(3, 2)), Decimal(-9.95))
//    checkEvaluation(cast(-9.95, DecimalType(3, 1)), Decimal(-10.0))
//    checkEvaluation(cast(-9.95, DecimalType(2, 0)), Decimal(-10))
//    checkEvaluation(cast(-9.95, DecimalType(2, 1)), null)
//    checkEvaluation(cast(-9.95, DecimalType(1, 0)), null)
//    checkEvaluation(cast(Decimal(-9.95), DecimalType(3, 1)), Decimal(-10.0))
//    checkEvaluation(cast(Decimal(-9.95), DecimalType(1, 0)), null)
//
//    checkEvaluation(cast(Decimal("1003"), DecimalType.SYSTEM_DEFAULT), Decimal(1003))
//    checkEvaluation(cast(Decimal("1003"), DecimalType(4, 0)), Decimal(1003))
//    checkEvaluation(cast(Decimal("1003"), DecimalType(3, 0)), null)
//
//    checkEvaluation(cast(Decimal("995"), DecimalType(3, 0)), Decimal(995))
//
//    checkEvaluation(cast(Double.NaN, DecimalType.SYSTEM_DEFAULT), null)
//    checkEvaluation(cast(1.0 / 0.0, DecimalType.SYSTEM_DEFAULT), null)
//    checkEvaluation(cast(Float.NaN, DecimalType.SYSTEM_DEFAULT), null)
//    checkEvaluation(cast(1.0f / 0.0f, DecimalType.SYSTEM_DEFAULT), null)
//
//    checkEvaluation(cast(Double.NaN, DecimalType(2, 1)), null)
//    checkEvaluation(cast(1.0 / 0.0, DecimalType(2, 1)), null)
//    checkEvaluation(cast(Float.NaN, DecimalType(2, 1)), null)
//    checkEvaluation(cast(1.0f / 0.0f, DecimalType(2, 1)), null)
//
//    checkEvaluation(cast(true, DecimalType(2, 1)), Decimal(1))
//    checkEvaluation(cast(true, DecimalType(1, 1)), null)
//
    withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
//      assert(cast(Decimal("1003"), DecimalType(3, -1)).nullable)
//      assert(cast(Decimal("1003"), DecimalType(4, -1)).nullable === false)
//      assert(cast(Decimal("995"), DecimalType(2, -1)).nullable)
//      assert(cast(Decimal("995"), DecimalType(3, -1)).nullable === false)
//
//      checkEvaluation(cast(Decimal("1003"), DecimalType(3, -1)), Decimal(1000))
//      checkEvaluation(cast(Decimal("1003"), DecimalType(2, -2)), Decimal(1000))
//      checkEvaluation(cast(Decimal("1003"), DecimalType(1, -2)), null)
//      checkEvaluation(cast(Decimal("1003"), DecimalType(2, -1)), null)
//
//      checkEvaluation(cast(Decimal("995"), DecimalType(3, -1)), Decimal(1000))
//      checkEvaluation(cast(Decimal("995"), DecimalType(2, -2)), Decimal(1000))
//      checkEvaluation(cast(Decimal("995"), DecimalType(2, -1)), null)
//      checkEvaluation(cast(Decimal("995"), DecimalType(1, -2)), null)
    }
  }
}

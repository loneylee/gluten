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

import org.apache.spark.sql.GlutenTestsTrait
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{Decimal, DecimalType, DoubleType, LongType}

class GlutenArithmeticExpressionSuite extends ArithmeticExpressionSuite with GlutenTestsTrait {

  private def testDecimalAndDoubleType(testFunc: (Int => Any) => Unit): Unit = {
//    testFunc(_.toDouble)
    testFunc(Decimal(_))
  }

  test("2 / (Divide) basic") {
    testDecimalAndDoubleType { convert =>
      val left = Literal(convert(2))
      val right = Literal(convert(1))
      checkEvaluation(Divide(left, right), convert(2))
      checkEvaluation(Divide(Literal.create(null, left.dataType), right), null)
      checkEvaluation(Divide(left, Literal.create(null, right.dataType)), null)
//      checkEvaluation(Divide(left, Literal(convert(0))), null) // divide by zero
    }

    Seq("false").foreach { failOnError =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> failOnError) {
        Seq(DoubleType, DecimalType.SYSTEM_DEFAULT).foreach { tpe =>
          checkConsistencyBetweenInterpretedAndCodegenAllowingException(Divide(_, _), tpe, tpe)
        }
      }
    }
  }

  val positiveShort: Short = (Byte.MaxValue + 1).toShort
  val negativeShort: Short = (Byte.MinValue - 1).toShort

  val positiveShortLit: Literal = Literal(positiveShort)
  val negativeShortLit: Literal = Literal(negativeShort)

  val positiveInt: Int = Short.MaxValue + 1
  val negativeInt: Int = Short.MinValue - 1

  val positiveIntLit: Literal = Literal(positiveInt)
  val negativeIntLit: Literal = Literal(negativeInt)

  val positiveLong: Long = Int.MaxValue + 1L
  val negativeLong: Long = Int.MinValue - 1L

  val positiveLongLit: Literal = Literal(positiveLong)
  val negativeLongLit: Literal = Literal(negativeLong)

  private def testDecimalAndLongType(testFunc: (Int => Any) => Unit): Unit = {
    testFunc(_.toLong)
    testFunc(Decimal(_))
  }

  test("2 / (Divide) for Long and Decimal type") {
    testDecimalAndLongType { convert =>
      val left = Literal(convert(1))
      val right = Literal(convert(2))
      checkEvaluation(IntegralDivide(left, right), 0L)
      checkEvaluation(IntegralDivide(Literal.create(null, left.dataType), right), null)
      checkEvaluation(IntegralDivide(left, Literal.create(null, right.dataType)), null)
      checkEvaluation(IntegralDivide(left, Literal(convert(0))), null) // divide by zero
    }
    checkEvaluation(IntegralDivide(positiveLongLit, negativeLongLit), 0L)

    Seq("true", "false").foreach { failOnError =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> failOnError) {
        Seq(LongType, DecimalType.SYSTEM_DEFAULT).foreach { tpe =>
          checkConsistencyBetweenInterpretedAndCodegenAllowingException(
            IntegralDivide(_, _), tpe, tpe)
        }
      }
    }
  }

  private def testNumericDataTypes(testFunc: (Int => Any) => Unit): Unit = {
//    testFunc(_.toByte)
//    testFunc(_.toShort)
//    testFunc(identity)
//    testFunc(_.toLong)
//    testFunc(_.toFloat)
//    testFunc(_.toDouble)
    testFunc(Decimal(_))
  }

  test("2 % (Remainder)") {
    testNumericDataTypes { convert =>
      val left = Literal(convert(1))
      val right = Literal(convert(2))
      checkEvaluation(Remainder(left, right), convert(1))
      checkEvaluation(Remainder(Literal.create(null, left.dataType), right), null)
      checkEvaluation(Remainder(left, Literal.create(null, right.dataType)), null)
      checkEvaluation(Remainder(left, Literal(convert(0))), null) // mod by 0
    }

  }

}

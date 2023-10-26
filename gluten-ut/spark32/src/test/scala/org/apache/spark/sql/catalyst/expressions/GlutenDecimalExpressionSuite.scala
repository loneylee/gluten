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
import org.apache.spark.sql.types.{Decimal, LongType}

class GlutenDecimalExpressionSuite extends DecimalExpressionSuite with GlutenTestsTrait {

  test("2 MakeDecimal") {
//    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
//      checkEvaluation(MakeDecimal(Literal(101L), 3, 1), Decimal("10.1"))
//      checkEvaluation(MakeDecimal(Literal.create(null, LongType), 3, 1), null)
//      val overflowExpr = MakeDecimal(Literal.create(1000L, LongType), 3, 1)
//      checkEvaluation(overflowExpr, null)
//      checkEvaluationWithMutableProjection(overflowExpr, null)
//      evaluateWithoutCodegen(overflowExpr, null)
//      checkEvaluationWithUnsafeProjection(overflowExpr, null)
//    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkEvaluation(MakeDecimal(Literal(101L), 3, 1), Decimal("10.1"))
      checkEvaluation(MakeDecimal(Literal.create(null, LongType), 3, 1), null)
      val overflowExpr = MakeDecimal(Literal.create(1000L, LongType), 3, 1)
      intercept[ArithmeticException](checkEvaluationWithMutableProjection(overflowExpr, null))
      intercept[ArithmeticException](evaluateWithoutCodegen(overflowExpr, null))
      intercept[ArithmeticException](checkEvaluationWithUnsafeProjection(overflowExpr, null))
    }
  }

}

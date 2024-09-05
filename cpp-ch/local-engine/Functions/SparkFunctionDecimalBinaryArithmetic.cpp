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
#include "SparkFunctionDecimalBinaryArithmetic.h"

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
extern const Event FileSegmentWaitReadBufferMicroseconds;
extern const Event FileSegmentReadMicroseconds;
extern const Event FileSegmentCacheWriteMicroseconds;
extern const Event FileSegmentPredownloadMicroseconds;
extern const Event FileSegmentUsedBytes;

extern const Event CachedReadBufferReadFromSourceMicroseconds;
extern const Event CachedReadBufferReadFromCacheMicroseconds;
extern const Event CachedReadBufferCacheWriteMicroseconds;
extern const Event CachedReadBufferReadFromSourceBytes;
extern const Event CachedReadBufferReadFromCacheBytes;
extern const Event CachedReadBufferCacheWriteBytes;
extern const Event CachedReadBufferCreateBufferMicroseconds;

extern const Event CachedReadBufferReadFromCacheHits;
extern const Event CachedReadBufferReadFromCacheMisses;

extern const Event BackupReadMetadataMicroseconds;
extern const Event BackupWriteMetadataMicroseconds;
extern const Event BackupEntriesCollectorMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int TYPE_MISMATCH;
extern const int LOGICAL_ERROR;
}

// template <typename, typename> struct MinusImpl;
bool decimalCheckArithmeticOverflow(ContextPtr context);
}

namespace ProfileEvents
{
extern const Event FileSegmentWaitReadBufferMicroseconds;
extern const Event FileSegmentReadMicroseconds;
extern const Event FileSegmentCacheWriteMicroseconds;
extern const Event FileSegmentPredownloadMicroseconds;
extern const Event FileSegmentUsedBytes;

extern const Event CachedReadBufferReadFromSourceMicroseconds;
extern const Event CachedReadBufferReadFromCacheMicroseconds;
extern const Event CachedReadBufferCacheWriteMicroseconds;
extern const Event CachedReadBufferReadFromSourceBytes;
extern const Event CachedReadBufferReadFromCacheBytes;
extern const Event CachedReadBufferCacheWriteBytes;
extern const Event CachedReadBufferCreateBufferMicroseconds;

extern const Event BackupReadMetadataMicroseconds;
extern const Event BackupWriteMetadataMicroseconds;
extern const Event BackupEntriesCollectorMicroseconds;

}

namespace local_engine
{
using namespace DB;

namespace
{
enum class OpCase : uint8_t
{
    Vector,
    LeftConstant,
    RightConstant
};

template <bool is_plus_minus, bool is_multiply, bool is_division>
DataTypePtr getReturnType(const IDataType & left, const IDataType & right)
{
    const size_t p1 = getDecimalPrecision(left);
    const size_t s1 = getDecimalScale(left);
    const size_t p2 = getDecimalPrecision(right);
    const size_t s2 = getDecimalScale(right);

    DataTypePtr return_type;
    size_t precision;
    size_t scale;
    if constexpr (is_plus_minus)
    {
        scale = std::max(s1, s2);
        precision = scale + std::max(p1 - s1, p2 - s2) + 1;
    }
    else if constexpr (is_multiply)
    {
        precision = p1 + p2 + 1;
        scale = s1 + s2;
    }
    else if constexpr (is_division)
    {
        scale = std::max(static_cast<size_t>(6), s1 + p2 + 1);
        precision = p1 - s1 + s2 + scale;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not supported.");
    }

    precision = std::min(precision, DataTypeDecimal256::maxPrecision());
    scale = std::min(scale, DataTypeDecimal256::maxPrecision());
    return createDecimal<DataTypeDecimal>(precision, scale);
}


template <typename Operation>
struct SparkDecimalBinaryOperation
{
private:
    static constexpr bool is_plus_minus = SparkIsOperation<Operation>::plus || SparkIsOperation<Operation>::minus;
    static constexpr bool is_multiply = SparkIsOperation<Operation>::multiply;
    static constexpr bool is_division = SparkIsOperation<Operation>::division;

public:
    template <typename A, typename B, typename R>
    static ColumnPtr executeDecimal(const ColumnsWithTypeAndName & arguments, const A & left, const B & right, const R & result)
    {
        using LeftDataType = std::decay_t<decltype(left)>; // e.g. DataTypeDecimal<Decimal32>
        using RightDataType = std::decay_t<decltype(right)>; // e.g. DataTypeDecimal<Decimal32>
        using ResultDataType = std::decay_t<decltype(result)>; // e.g. DataTypeDecimal<Decimal32>

        using ColVecLeft = ColumnVectorOrDecimal<typename LeftDataType::FieldType>;
        using ColVecRight = ColumnVectorOrDecimal<typename RightDataType::FieldType>;

        const ColumnPtr left_col = arguments[0].column;
        const ColumnPtr right_col = arguments[1].column;

        const auto * const col_left_raw = left_col.get();
        const auto * const col_right_raw = right_col.get();

        const size_t col_left_size = col_left_raw->size();

        const ColumnConst * const col_left_const = checkAndGetColumnConst<ColVecLeft>(col_left_raw);
        const ColumnConst * const col_right_const = checkAndGetColumnConst<ColVecRight>(col_right_raw);

        const ColVecLeft * const col_left = checkAndGetColumn<ColVecLeft>(col_left_raw);
        const ColVecRight * const col_right = checkAndGetColumn<ColVecRight>(col_right_raw);

        return executeDecimalImpl<LeftDataType, RightDataType, ResultDataType>(
            left, right, col_left_const, col_right_const, col_left, col_right, col_left_size, result);
    }

private:
    // ResultDataType e.g. DataTypeDecimal<Decimal32>
    template <class LeftDataType, class RightDataType, class ResultDataType>
    static ColumnPtr executeDecimalImpl(
        const auto & left,
        const auto & right,
        const ColumnConst * const col_left_const,
        const ColumnConst * const col_right_const,
        const auto * const col_left,
        const auto * const col_right,
        size_t col_left_size,
        const ResultDataType & resultDataType)
    {
        using LeftFieldType = typename LeftDataType::FieldType;
        using RightFieldType = typename RightDataType::FieldType;
        using ResultFieldType = typename ResultDataType::FieldType;

        using NativeResultType = NativeType<ResultFieldType>;
        using ColVecResult = ColumnVectorOrDecimal<ResultFieldType>;

        using ResultFieldType = typename ResultDataType::FieldType;
        NativeResultType scale_left = [&]
        {
            if constexpr (is_multiply)
                return NativeResultType{1};
            else if constexpr (is_division)
                return ResultDataType::getScaleMultiplier(resultDataType.getScale() - left.getScale() + right.getScale());
            else
                return resultDataType.scaleFactorFor(left, is_division).value;
        }();

        const NativeResultType scale_right = [&]
        {
            if constexpr (is_multiply || is_division)
                return NativeResultType{1};
            else
                return resultDataType.scaleFactorFor(right, is_division).value;
        }();

        ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(col_left_size, false);
        ColumnUInt8::Container * vec_null_map_to = &col_null_map_to->getData();

        typename ColVecResult::MutablePtr col_res = ColVecResult::create(0, resultDataType.getScale());
        auto & vec_res = col_res->getData();
        vec_res.resize(col_left_size);

        Stopwatch watch3(CLOCK_MONOTONIC);
        NativeResultType max_value = intExp10OfSize<NativeResultType>(resultDataType.getPrecision());

        if (col_left && col_right)
        {
            process<OpCase::Vector>(
                col_left->getData(), col_right->getData(), vec_res, scale_left, scale_right, *vec_null_map_to, max_value);
            watch3.stop();
            ProfileEvents::increment(ProfileEvents::BackupEntriesCollectorMicroseconds, watch3.elapsedMicroseconds());
            return ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
        }
        else if (col_left_const && col_right)
        {
            LeftFieldType const_left = col_left_const->getValue<LeftFieldType>();
            process<OpCase::LeftConstant>(const_left, col_right->getData(), vec_res, scale_left, scale_right, *vec_null_map_to, max_value);
            watch3.stop();
            ProfileEvents::increment(ProfileEvents::BackupEntriesCollectorMicroseconds, watch3.elapsedMicroseconds());
            return ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
        }
        else if (col_left && col_right_const)
        {
            RightFieldType const_right = col_right_const->getValue<RightFieldType>();
            process<OpCase::RightConstant>(col_left->getData(), const_right, vec_res, scale_left, scale_right, *vec_null_map_to, max_value);
            watch3.stop();
            ProfileEvents::increment(ProfileEvents::BackupEntriesCollectorMicroseconds, watch3.elapsedMicroseconds());
            return ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not supported.");
    }

    template <OpCase op_case, typename ResultContainerType, typename NativeResultType>
    static static void NO_INLINE process(
        const auto & a,
        const auto & b,
        ResultContainerType & result_container,
        NativeResultType scale_a,
        NativeResultType scale_b,
        ColumnUInt8::Container & vec_null_map_to,
        const NativeResultType & max_value)
    {
        size_t size;
        if constexpr (op_case == OpCase::LeftConstant)
            size = b.size();
        else
            size = a.size();

        if constexpr (op_case == OpCase::Vector)
        {
            for (size_t i = 0; i < size; ++i)
            {
                NativeResultType res;
                if (calculate(
                        unwrap<op_case, OpCase::LeftConstant>(a, i),
                        unwrap<op_case, OpCase::RightConstant>(b, i),
                        scale_a,
                        scale_b,
                        res,
                        max_value))
                    result_container[i] = res;
                else
                    vec_null_map_to[i] = static_cast<UInt8>(1);
            }
        }
        else if constexpr (op_case == OpCase::LeftConstant)
        {
            auto scaled_a = applyScaled(unwrap<op_case, OpCase::LeftConstant>(a, 0), scale_a);
            scale_a = static_cast<NativeResultType>(0);
            for (size_t i = 0; i < size; ++i)
            {
                NativeResultType res;
                if (calculate(scaled_a, unwrap<op_case, OpCase::RightConstant>(b, i), scale_a, scale_b, res, max_value))
                    result_container[i] = res;
                else
                    vec_null_map_to[i] = static_cast<UInt8>(1);
            }
        }
        else if constexpr (op_case == OpCase::RightConstant)
        {
            auto scaled_b = applyScaled(unwrap<op_case, OpCase::RightConstant>(b, 0), scale_b);
            scale_b = static_cast<NativeResultType>(0);
            for (size_t i = 0; i < size; ++i)
            {
                NativeResultType res;
                if (calculate(unwrap<op_case, OpCase::LeftConstant>(a, i), scaled_b, scale_a, scale_b, res, max_value))
                    result_container[i] = res;
                else
                    vec_null_map_to[i] = static_cast<UInt8>(1);
            }
        }
    }

    // ResultNativeType = Int32/64/128/256
    template <typename LeftNativeType, typename RightNativeType, typename NativeResultType>
    static NO_SANITIZE_UNDEFINED bool calculate(
        LeftNativeType l,
        RightNativeType r,
        NativeResultType scale_left,
        NativeResultType scale_right,
        NativeResultType & res,
        const NativeResultType & max_value)
    {
        static_assert(is_plus_minus || is_multiply || is_division);
        // 对于spark来讲，提升精度的时候一定不会overflow，因为spark的result type总是比原生的大
        NativeResultType scaled_l = applyScaled(l, scale_left);
        NativeResultType scaled_r = applyScaled(r, scale_right);

        auto success = Operation::template apply<NativeResultType>(scaled_l, scaled_r, res);
        if (!success)
            return false;

        // check overflow
        if (res <= -max_value || res >= max_value)
            return false;

        return true;
    }

    template <OpCase op_case, OpCase target, class E>
    static auto unwrap(const E & elem, size_t i)
    {
        if constexpr (op_case == target)
            return elem.value;
        else
            return elem[i].value;
    }

    template <typename LeftNativeType, typename ResultNativeType>
    static ResultNativeType applyScaled(LeftNativeType l, ResultNativeType scale)
    {
        if (scale > 1)
            return static_cast<ResultNativeType>(common::mulIgnoreOverflow(l, scale));

        return static_cast<ResultNativeType>(l);
    }
};


template <class Operation, typename Name>
class SparkFunctionDecimalBinaryArithmetic final : public IFunction
{
    static constexpr bool is_plus_minus = SparkIsOperation<Operation>::plus || SparkIsOperation<Operation>::minus;
    static constexpr bool is_multiply = SparkIsOperation<Operation>::multiply;
    static constexpr bool is_division = SparkIsOperation<Operation>::division;


public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<SparkFunctionDecimalBinaryArithmetic>(context_); }

    explicit SparkFunctionDecimalBinaryArithmetic(ContextPtr context_) : context(context_) { }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function '{}' expects 2 arguments", getName());

        if (!isDecimal(arguments[0].type) || !isDecimal(arguments[1].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} {} of argument of function {}",
                arguments[0].type->getName(),
                arguments[1].type->getName(),
                getName());

        const auto left = removeNullable(arguments[0].type);
        const auto right = removeNullable(arguments[1].type);

        return getReturnType<is_plus_minus, is_multiply, is_division>(*left, *right);
    }

    // executeImpl2
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & left_argument = arguments[0];
        const auto & right_argument = arguments[1];
        const auto * left_generic = left_argument.type.get();
        const auto * right_generic = right_argument.type.get();

        ColumnPtr res;
        const bool valid = castBothTypes(
            left_generic,
            right_generic,
            getReturnType<is_plus_minus, is_multiply, is_division>(*left_generic, *right_generic).get(),
            [&](const auto & left, const auto & right, const auto & result)
            {
                // using LeftDataType = std::decay_t<decltype(left)>;
                // using RightDataType = std::decay_t<decltype(right)>;
                return (res = SparkDecimalBinaryOperation<Operation>::template executeDecimal(arguments, left, right, result)) != nullptr;
            });

        if (!valid)
        {
            // This is a logical error, because the types should have been checked
            // by getReturnTypeImpl().
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Arguments of '{}' have incorrect data types: '{}' of type '{}',"
                " '{}' of type '{}'",
                getName(),
                left_argument.name,
                left_argument.type->getName(),
                right_argument.name,
                right_argument.type->getName());
        }

        return res;
    }

private:
    template <typename F>
    static bool castBothTypes(const IDataType * left, const IDataType * right, const IDataType * result, F && f)
    {
        return castType(
            left,
            [&](const auto & left_)
            {
                return castType(
                    right,
                    [&](const auto & right_) { return castType(result, [&](const auto & result_) { return f(left_, right_, result_); }); });
            });
    }

    static bool castType(const IDataType * type, auto && f)
    {
        using Types = TypeList<DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128, DataTypeDecimal256>;
        return castTypeToEither(Types{}, type, std::forward<decltype(f)>(f));
    }

    ContextPtr context;
};

struct NameSparkDecimalPlus
{
    static constexpr auto name = "sparkDecimalPlus";
};
struct NameSparkDecimalMinus
{
    static constexpr auto name = "sparkDecimalMinus";
};
struct NameSparkDecimalMultiply
{
    static constexpr auto name = "sparkDecimalMultiply";
};
struct NameSparkDecimalDivide
{
    static constexpr auto name = "sparkDecimalDivide";
};

using SparkDecimalFunctionPlus = SparkFunctionDecimalBinaryArithmetic<DecimalPlusImpl, NameSparkDecimalPlus>;
using SparkDecimalFunctionMinus = SparkFunctionDecimalBinaryArithmetic<DecimalMinusImpl, NameSparkDecimalMinus>;
using SparkDecimalFunctionMultiply = SparkFunctionDecimalBinaryArithmetic<DecimalMultiplyImpl, NameSparkDecimalMultiply>;
using SparkDecimalFunctionDivide = SparkFunctionDecimalBinaryArithmetic<DecimalDivideImpl, NameSparkDecimalDivide>;

}

REGISTER_FUNCTION(SparkDecimalFunctionArithmetic)
{
    factory.registerFunction<SparkDecimalFunctionPlus>();
    factory.registerFunction<SparkDecimalFunctionMinus>();
    factory.registerFunction<SparkDecimalFunctionMultiply>();
    factory.registerFunction<SparkDecimalFunctionDivide>();
}
}

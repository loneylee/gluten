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

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionHelpers.h>
#include <Parser/FunctionParser.h>
#include <Parser/TypeParser.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace local_engine
{

class DecimalType
{
public:
    static constexpr Int32 spark_max_precision = 38;
    static constexpr Int32 spark_max_scale = 38;
    static constexpr Int32 minimum_adjusted_scale = 6;

    static constexpr Int32 clickhouse_max_precision = DB::DataTypeDecimal256::maxPrecision();
    static constexpr Int32 clickhouse_max_scale = DB::DataTypeDecimal128::maxPrecision();

    Int32 precision;
    Int32 scale;

    enum MODE
    {
        NORMAL,
        QUICK
    };

private:
    static DecimalType bounded_to_clickhouse(const Int32 precision, const Int32 scale, const MODE mode)
    {
        const Int32 max_precision = (mode == NORMAL) ? clickhouse_max_precision : spark_max_precision;
        const Int32 max_scale = (mode == NORMAL) ? clickhouse_max_scale : spark_max_scale;
        return DecimalType(std::min(precision, max_precision), std::min(scale, max_scale));
    }

public:
    static DecimalType evalAddSubtractDecimalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2, const MODE mode)
    {
        const Int32 scale = s1;
        const Int32 precision = scale + std::max(p1 - s1, p2 - s2) + 1;
        return bounded_to_clickhouse(precision, scale, mode);
    }

    static DecimalType evalDivideDecimalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2, const MODE mode)
    {
        const Int32 scale = std::max(minimum_adjusted_scale, s1 + p2 + 1);
        const Int32 precision = p1 - s1 + s2 + scale;
        return bounded_to_clickhouse(precision, scale, mode);
    }

    static DecimalType evalModuloDecimalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2, const MODE mode)
    {
        const Int32 scale = std::max(s1, s2);
        const Int32 precision = std::min(p1 - s1, p2 - s2) + scale;
        return bounded_to_clickhouse(precision, scale, mode);
    }

    static DecimalType evalMultiplyDecimalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 /*s2*/, const MODE mode)
    {
        const Int32 scale = s1;
        const Int32 precision = p1 + p2 + 1;
        return bounded_to_clickhouse(precision, scale, mode);
    }
};

class FunctionParserBinaryArithmetic : public FunctionParser
{
protected:
    ActionsDAG::NodeRawConstPtrs convertBinaryArithmeticFunDecimalArgs(
        ActionsDAG & actions_dag,
        const ActionsDAG::NodeRawConstPtrs & args,
        const DecimalType & eval_type,
        const substrait::Expression_ScalarFunction & arithmeticFun) const
    {
        const Int32 precision = eval_type.precision;
        const Int32 scale = eval_type.scale;

        ActionsDAG::NodeRawConstPtrs new_args;
        new_args.reserve(args.size());

        ActionsDAG::NodeRawConstPtrs cast_args;
        cast_args.reserve(2);
        cast_args.emplace_back(args[0]);
        DataTypePtr ch_type = createDecimal<DataTypeDecimal>(precision, scale);
        ch_type = wrapNullableType(arithmeticFun.output_type().decimal().nullability(), ch_type);
        const String type_name = ch_type->getName();
        const DataTypePtr str_type = std::make_shared<DataTypeString>();
        const ActionsDAG::Node * type_node
            = &actions_dag.addColumn(ColumnWithTypeAndName(str_type->createColumnConst(1, type_name), str_type, getUniqueName(type_name)));
        cast_args.emplace_back(type_node);
        const ActionsDAG::Node * cast_node = toFunctionNode(actions_dag, "CAST", cast_args);
        actions_dag.addOrReplaceInOutputs(*cast_node);
        new_args.emplace_back(cast_node);
        new_args.emplace_back(args[1]);
        return new_args;
    }

    DecimalType getDecimalType(const DataTypePtr & left, const DataTypePtr & right, const DecimalType::MODE mode) const
    {
        assert(isDecimal(left) && isDecimal(right));
        const Int32 p1 = getDecimalPrecision(*left);
        const Int32 s1 = getDecimalScale(*left);
        const Int32 p2 = getDecimalPrecision(*right);
        const Int32 s2 = getDecimalScale(*right);
        return internalEvalType(p1, s1, p2, s2, mode);
    }

    virtual DecimalType internalEvalType(Int32 p1, Int32 s1, Int32 p2, Int32 s2, const DecimalType::MODE mode) const = 0;

    const ActionsDAG::Node *
    checkDecimalOverflow(ActionsDAG & actions_dag, const ActionsDAG::Node * func_node, Int32 precision, Int32 scale) const
    {
        //TODO: checkDecimalOverflowSpark throw exception per configuration
        const DB::ActionsDAG::NodeRawConstPtrs overflow_args
            = {func_node,
               plan_parser->addColumn(actions_dag, std::make_shared<DataTypeInt32>(), precision),
               plan_parser->addColumn(actions_dag, std::make_shared<DataTypeInt32>(), scale)};
        return toFunctionNode(actions_dag, "checkDecimalOverflowSparkOrNull", overflow_args);
    }

    virtual const DB::ActionsDAG::Node *
    createFunctionNode(DB::ActionsDAG & actions_dag, const String & func_name, const DB::ActionsDAG::NodeRawConstPtrs & args) const
    {
        return toFunctionNode(actions_dag, func_name, args);
    }

public:
    explicit FunctionParserBinaryArithmetic(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    const ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAG & actions_dag) const override
    {
        const auto ch_func_name = getCHFunctionName(substrait_func);
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);

        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto left_type = DB::removeNullable(parsed_args[0]->result_type);
        const auto right_type = DB::removeNullable(parsed_args[1]->result_type);
        const bool converted = isDecimal(left_type) && isDecimal(right_type);
        DataTypePtr parsed_output_type [[maybe_unused]];

        if (converted)
        {
            parsed_output_type = removeNullable(TypeParser::parseType(substrait_func.output_type()));
            chassert(isDecimal(parsed_output_type));

            const auto & settings = plan_parser->getContext()->getSettingsRef();
            DecimalType::MODE mode = DecimalType::NORMAL;
            if (settings.has("arithmetic.decimal.mode") && Poco::toUpper(settings.getString("arithmetic.decimal.mode")) == "'QUICK'")
                mode = DecimalType::QUICK;

            const DecimalType evalType = getDecimalType(left_type, right_type, mode);
            parsed_args = convertBinaryArithmeticFunDecimalArgs(actions_dag, parsed_args, evalType, substrait_func);
        }

        const auto * func_node = createFunctionNode(actions_dag, ch_func_name, parsed_args);

        if (converted)
        {
            const Int32 parsed_precision = getDecimalPrecision(*parsed_output_type);
            const Int32 parsed_scale = getDecimalScale(*parsed_output_type);

            auto output_type = removeNullable(func_node->result_type);
            Int32 output_precision = getDecimalPrecision(*output_type);
            Int32 output_scale = getDecimalScale(*output_type);

             if (parsed_precision == output_precision && parsed_scale == output_scale && parsed_precision <= DecimalType::spark_max_precision )
                 return func_node;

            func_node = checkDecimalOverflow(actions_dag, func_node, parsed_precision, parsed_scale);
#ifndef NDEBUG
            output_type = removeNullable(func_node->result_type);
            output_precision = getDecimalPrecision(*output_type);
            output_scale = getDecimalScale(*output_type);
            if (output_precision != parsed_precision || output_scale != parsed_scale)
                throw Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS,
                    "Function {} has wrong output type, need Decimal({}, {}), actual Decimal({}, {})",
                    getName(),
                    parsed_precision,
                    parsed_scale,
                    output_precision,
                    output_scale);
#endif

            return func_node;
        }
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};

class FunctionParserPlus final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserPlus(SerializedPlanParser * plan_parser_) : FunctionParserBinaryArithmetic(plan_parser_) { }

    static constexpr auto name = "add";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const override { return "plus"; }

protected:
    DecimalType internalEvalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2, const DecimalType::MODE mode) const override
    {
        return DecimalType::evalAddSubtractDecimalType(p1, s1, p2, s2, mode);
    }
};

class FunctionParserMinus final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserMinus(SerializedPlanParser * plan_parser_) : FunctionParserBinaryArithmetic(plan_parser_) { }

    static constexpr auto name = "subtract";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const override { return "minus"; }

protected:
    DecimalType internalEvalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2, const DecimalType::MODE mode) const override
    {
        return DecimalType::evalAddSubtractDecimalType(p1, s1, p2, s2, mode);
    }
};

class FunctionParserMultiply final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserMultiply(SerializedPlanParser * plan_parser_) : FunctionParserBinaryArithmetic(plan_parser_) { }
    static constexpr auto name = "multiply";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const override { return "multiply"; }

protected:
    DecimalType internalEvalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2, const DecimalType::MODE mode) const override
    {
        return DecimalType::evalMultiplyDecimalType(p1, s1, p2, s2, mode);
    }
};

class FunctionParserModulo final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserModulo(SerializedPlanParser * plan_parser_) : FunctionParserBinaryArithmetic(plan_parser_) { }
    static constexpr auto name = "modulus";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const override { return "modulo"; }

protected:
    DecimalType internalEvalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2, const DecimalType::MODE mode) const override
    {
        return DecimalType::evalModuloDecimalType(p1, s1, p2, s2, mode);
    }
};

class FunctionParserDivide final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserDivide(SerializedPlanParser * plan_parser_) : FunctionParserBinaryArithmetic(plan_parser_) { }
    static constexpr auto name = "divide";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const override { return "divide"; }

protected:
    DecimalType internalEvalType(
        const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2, const DecimalType::MODE mode) const override
    {
        return DecimalType::evalDivideDecimalType(p1, s1, p2, s2, mode);
    }

    const DB::ActionsDAG::Node * createFunctionNode(
        DB::ActionsDAG & actions_dag, const String & func_name, const DB::ActionsDAG::NodeRawConstPtrs & new_args) const override
    {
        assert(func_name == name);
        const auto * left_arg = new_args[0];
        const auto * right_arg = new_args[1];

        if (isDecimal(removeNullable(left_arg->result_type)) || isDecimal(removeNullable(right_arg->result_type)))
            return toFunctionNode(actions_dag, "sparkDivideDecimal", {left_arg, right_arg});

        return toFunctionNode(actions_dag, "sparkDivide", {left_arg, right_arg});
    }
};

static FunctionParserRegister<FunctionParserPlus> register_plus;
static FunctionParserRegister<FunctionParserMinus> register_minus;
static FunctionParserRegister<FunctionParserMultiply> register_multiply;
static FunctionParserRegister<FunctionParserDivide> register_divide;
static FunctionParserRegister<FunctionParserModulo> register_modulo;

}

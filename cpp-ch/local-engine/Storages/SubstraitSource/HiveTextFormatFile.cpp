#include "HiveTextFormatFile.h"

#include <memory>
#include <string>
#include <utility>

#include <Columns/ColumnNullable.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/SerializationDate32.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/FormatSettings.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/Serializations/ExcelSerialization.h>


namespace local_engine
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

HiveTextFormatFile::HiveTextFormatFile(DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    :FormatFile(context_, file_info_, read_buffer_builder_) {}

FormatFile::InputFormatPtr HiveTextFormatFile::createInputFormat(const DB::Block & header)
{
    auto res = std::make_shared<FormatFile::InputFormat>();
    res->read_buffer = std::move(read_buffer_builder->build(file_info, true));

    DB::FormatSettings format_settings = createFormatSettings();
    size_t max_block_size = file_info.text().max_block_size();
    DB::RowInputFormatParams in_params = {max_block_size};

    std::shared_ptr<GlutenHiveTextRowInputFormat> hive_txt_input_format = std::make_shared<GlutenHiveTextRowInputFormat>(
        header, *(res->read_buffer), in_params, format_settings, file_info.text().schema());
    res->input = hive_txt_input_format;
    return res;
}

DB::FormatSettings HiveTextFormatFile::createFormatSettings()
{
    DB::FormatSettings format_settings = DB::getFormatSettings(context);
    format_settings.with_names_use_header = true;
    format_settings.skip_unknown_fields = true;
    std::string delimiter = file_info.text().field_delimiter();
    format_settings.csv.delimiter = *delimiter.data();
    format_settings.csv.skip_first_lines = file_info.text().header();

    char quote = *file_info.text().quote().data();
    if (quote == '\'')
    {
        format_settings.csv.allow_single_quotes = true;
        format_settings.csv.allow_double_quotes = false;
    }
    else if (quote == '"')
    {
        format_settings.csv.allow_single_quotes = false;
        format_settings.csv.allow_double_quotes = true;
    }

    return format_settings;
}

GlutenHiveTextRowInputFormat::GlutenHiveTextRowInputFormat(
    const DB::Block & header_, 
    DB::ReadBuffer & in_, 
    const DB::RowInputFormatParams & params_, 
    const DB::FormatSettings & format_settings_,
    const substrait::NamedStruct & input_schema_)
    : GlutenHiveTextRowInputFormat(header_, std::make_unique<DB::PeekableReadBuffer>(in_), params_, format_settings_)
{
    input_schema = input_schema_;
}

GlutenHiveTextRowInputFormat::GlutenHiveTextRowInputFormat(
    const DB::Block & header_, std::shared_ptr<DB::PeekableReadBuffer> buf_, const DB::RowInputFormatParams & params_, const DB::FormatSettings & format_settings_)
    : DB::CSVRowInputFormat(
        header_, buf_, params_, true, false, format_settings_, std::make_unique<GlutenHiveTextFormatReader>(*buf_, header_, format_settings_))
{
    DB::Serializations gluten_serializations ;
    for (const auto & item : data_types)
    {
        const auto & nest_type = item->isNullable() ? *static_cast<const DataTypeNullable &>(*item).getNestedType() : *item;
        if (item->isNullable())
        {
            gluten_serializations.insert(
                gluten_serializations.end(),
                std::make_shared<SerializationNullable>(std::make_shared<ExcelSerialization>(nest_type.getDefaultSerialization())));
        }
        else
        {
            gluten_serializations.insert(
                gluten_serializations.end(), std::make_shared<ExcelSerialization>(nest_type.getDefaultSerialization()));
        }
    }

    serializations = gluten_serializations;
}

void GlutenHiveTextRowInputFormat::readPrefix()
{
    CSVRowInputFormat::readPrefix();
    std::vector<std::string> column_names = column_mapping->names_of_columns;
    for (size_t i = 0; i < column_names.size(); ++i)
    {
        for (int j = 0; j < input_schema.names_size(); ++j) 
        {
            const char * file_field_name = input_schema.names(j).data();
            if (strcasecmp(file_field_name, column_names[i].data()) == 0) 
            {
                auto column_index = column_mapping->column_indexes_for_input_fields[i];
                if (column_index && static_cast<int>(*column_index) != j)
                {
                    column_mapping->column_indexes_for_input_fields[j] = std::optional<size_t>(i);
                    column_mapping->column_indexes_for_input_fields[i] = std::optional<size_t>();
                }
                break;
            }
        }
    }
}

GlutenHiveTextFormatReader::GlutenHiveTextFormatReader(
    DB::PeekableReadBuffer & buf_, const DB::Block & header, const DB::FormatSettings & format_settings_)
    : DB::CSVFormatReader(buf_, format_settings_), input_field_names(header.getNames())
{
}

std::vector<String> GlutenHiveTextFormatReader::readNames()
{
    DB::PeekableReadBufferCheckpoint checkpoint{*buf, true};
    auto values = readHeaderRow();
    input_field_names.resize(values.size());
    return input_field_names;
}

void registerInputFormatHiveText(DB::FormatFactory & factory)
{
    factory.registerInputFormat(
        "GlutenHiveText", [](
            DB::ReadBuffer & buf, 
            const DB::Block & sample, 
            const DB::RowInputFormatParams & params, 
            const DB::FormatSettings & settings)
        {
            substrait::NamedStruct input_schema;
            return std::make_shared<GlutenHiveTextRowInputFormat>(sample, buf, params, settings, input_schema);
        });
}

void registerFileSegmentationEngineHiveText(DB::FormatFactory & factory)
{
    factory.registerFileSegmentationEngine(
        "GlutenHiveText",
        [](DB::ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows) -> std::pair<bool, size_t> {
            return fileSegmentationEngineCSVImpl(in, memory, min_bytes, 0, max_rows);
        });
}

}

#pragma once

#include "config.h"
#include <memory>
#include <Columns/IColumn.h>
#include <IO/ReadBuffer.h>
#include <IO/PeekableReadBuffer.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>

namespace local_engine
{
class HiveTextFormatFile : public FormatFile
{
public:
    explicit HiveTextFormatFile(DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_);
    ~HiveTextFormatFile() override = default;
    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header) override;
    std::optional<size_t> getTotalRows() override  { return 1; }
    bool supportSplit() override { return true; }
};

class GlutenHiveTextFormatReader final : public DB::CSVFormatReader
{
public:
    explicit GlutenHiveTextFormatReader(DB::PeekableReadBuffer & buf_, const DB::FormatSettings & format_settings_);
private:
    std::vector<String> readNames() override;
    std::vector<String> input_field_names;
    
};

/// A stream for input data in Hive Text format.
class GlutenHiveTextRowInputFormat final : public DB::CSVRowInputFormat
{
public:
    GlutenHiveTextRowInputFormat(
        const DB::Block & header_, 
        DB::ReadBuffer & in_, 
        const DB::RowInputFormatParams & params_, 
        const DB::FormatSettings & format_settings_,
        const substrait::NamedStruct & input_schema
    );

    String getName() const override { return "GlutenHiveTextRowInputFormat"; }

protected:
    void readPrefix() override;

private:

    substrait::NamedStruct input_schema;

    GlutenHiveTextRowInputFormat(
        const DB::Block & header_, std::shared_ptr<DB::PeekableReadBuffer> buf_, const DB::RowInputFormatParams & params_, const DB::FormatSettings & format_settings_);
};


}

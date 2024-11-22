#pragma once

#include <optional>

#include <Core/Block.h>
#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <IO/PeekableReadBuffer.h>

namespace DB
{

class CSVFormatReader;

/** A stream for inputting data in csv format with dynamic schema adjustment.
  * Extends the base CSVRowInputFormat to handle missing columns dynamically during import.
  */
class CSVLiveRowInputFormat : public RowInputFormatWithNamesAndTypes<CSVFormatReader>
{
public:
    /** with_names - in the first line the header with column names
      * with_types - on the next line header with type names
      */
    CSVLiveRowInputFormat(const Block & header_, ReadBuffer & in_, const Params & params_,
                          bool with_names_, bool with_types_, const FormatSettings & format_settings_);

    String getName() const override { return "CSVLiveRowInputFormat"; }

    void setReadBuffer(ReadBuffer & in_) override;
    void resetReadBuffer() override;

    void readPrefix() override;

protected:
    CSVLiveRowInputFormat(const Block & header_, std::shared_ptr<PeekableReadBuffer> in_, const Params & params_,
                          bool with_names_, bool with_types_, const FormatSettings & format_settings_);

private:
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    bool supportsCountRows() const override { return true; }
    bool read(MutableColumns & columns, RowReadExtension & row_read_extension) override;

    /// Validates and dynamically adds missing columns to the table
    void validateAndAddMissingColumns();

    /// Adds missing columns to the table schema dynamically
    void addMissingColumns(const std::vector<String> & csv_columns);

    /// Fetches the updated table structure after adding missing columns
    Block getUpdatedTableStructure(const String & table_name, const ContextPtr & context);

protected:
    std::shared_ptr<PeekableReadBuffer> buf;
    std::vector<String> header_columns; /// Parsed column names from the CSV header
    String table_name; /// Name of the target table for schema validation
};

void registerInputFormatCSVLive(FormatFactory & factory);

}
    
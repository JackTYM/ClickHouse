#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/Operators.h>

#include <Formats/CSVFormatReader.h>
#include <Formats/verbosePrintString.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Formats/EscapingRuleUtils.h>
#include <Processors/Formats/Impl/CSVLiveRowInputFormat.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/logger_useful.h>
#include <Interpreters/executeQuery.h>
#include <Storages/StorageFactory.h>
#include <Storages/IStorage.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{
    void checkBadDelimiter(char delimiter, bool allow_whitespace_or_tab_as_delimiter)
    {
        if ((delimiter == ' ' || delimiter == '\t') && allow_whitespace_or_tab_as_delimiter)
        {
            return;
        }
        constexpr std::string_view bad_delimiters = " \t\"'.UL";
        if (bad_delimiters.find(delimiter) != std::string_view::npos)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "CSV format may not work correctly with delimiter '{}'. Try using CustomSeparated format instead",
                delimiter);
    }
}

CSVLiveRowInputFormat::CSVLiveRowInputFormat(
    const Block & header_,
    ReadBuffer & in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    const FormatSettings & format_settings_)
    : CSVLiveRowInputFormat(
        header_, std::make_shared<PeekableReadBuffer>(in_), params_, with_names_, with_types_, format_settings_)
{
}

CSVLiveRowInputFormat::CSVLiveRowInputFormat(
    const Block & header_,
    std::shared_ptr<PeekableReadBuffer> in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(
        header_,
        *in_,
        params_,
        false,
        with_names_,
        with_types_,
        format_settings_,
        std::make_unique<CSVFormatReader>(*in_, format_settings_),
        format_settings_.csv.try_detect_header),
    buf(std::move(in_))
{
    checkBadDelimiter(format_settings_.csv.delimiter, format_settings_.csv.allow_whitespace_or_tab_as_delimiter);

    if (with_names_)
    {
        header_columns = format_reader->readRowImpl<true>(); // Parse the header row
    }
}

void CSVLiveRowInputFormat::syncAfterError()
{
    skipToNextLineOrEOF(*buf);
}

void CSVLiveRowInputFormat::validateAndAddMissingColumns()
{
    if (header_columns.empty())
        return;

    auto existing_columns = header_.getNames();
    for (const auto & column : header_columns)
    {
        if (std::find(existing_columns.begin(), existing_columns.end(), column) == existing_columns.end())
        {
            // Assume default data type as String for new columns
            String query = fmt::format("ALTER TABLE {} ADD COLUMN {} String DEFAULT ''", table_name, column);
            executeQuery(query, getContext());
        }
    }

    // Update header after adding missing columns
    header_ = getUpdatedTableStructure(table_name, getContext());
}

Block CSVLiveRowInputFormat::getUpdatedTableStructure(const String & table_name, const ContextPtr & context)
{
    auto storage = DatabaseCatalog::instance().getTable(StorageID("", table_name), context);
    return storage->getSampleBlock();
}

void CSVLiveRowInputFormat::readPrefix()
{
    if (with_names_)
    {
        validateAndAddMissingColumns();
    }
}

bool CSVLiveRowInputFormat::read(MutableColumns & columns, RowReadExtension & row_read_extension)
{
    auto csv_row = format_reader->readRowImpl<false>();

    // Handle missing columns
    if (csv_row.size() < header_.columns())
    {
        for (size_t i = csv_row.size(); i < header_.columns(); ++i)
        {
            csv_row.push_back(""); // Default empty string for missing columns
        }
    }
    else if (csv_row.size() > header_.columns())
    {
        LOG_WARNING(log, "CSV row has more columns than the table schema. Ignoring extra columns.");
        csv_row.resize(header_.columns());
    }

    // Populate columns from the CSV row
    for (size_t i = 0; i < csv_row.size(); ++i)
    {
        insertValueIntoColumn(columns[i], csv_row[i]);
    }

    return true;
}

void CSVLiveRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    buf = std::make_unique<PeekableReadBuffer>(in_);
    RowInputFormatWithNamesAndTypes::setReadBuffer(*buf);
}

void CSVLiveRowInputFormat::resetReadBuffer()
{
    buf.reset();
    RowInputFormatWithNamesAndTypes::resetReadBuffer();
}

void registerInputFormatCSVLive(FormatFactory & factory)
{
    factory.registerInputFormat("CSVLive", [](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings)
    {
        return std::make_shared<CSVLiveRowInputFormat>(sample, buf, std::move(params), true, false, settings);
    });
}

} // namespace DB

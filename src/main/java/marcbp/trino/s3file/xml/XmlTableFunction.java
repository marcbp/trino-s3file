package marcbp.trino.s3file.xml;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.file.AbstractTextFilePageSource;
import marcbp.trino.s3file.file.BaseTextFileHandle;
import marcbp.trino.s3file.file.FileSelection;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.PageSettings;
import marcbp.trino.s3file.file.ScanSettings;
import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.SizeOf.sizeOfCharArray;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;
import static marcbp.trino.s3file.util.TableFunctionArguments.encodingArgumentSpecification;
import static marcbp.trino.s3file.util.TableFunctionArguments.fileArguments;
import static marcbp.trino.s3file.util.TableFunctionArguments.optionalBoolean;
import static marcbp.trino.s3file.util.TableFunctionArguments.optionalString;
import static marcbp.trino.s3file.util.TableFunctionArguments.requirePath;
import static marcbp.trino.s3file.util.TableFunctionArguments.resolveEncoding;

/**
 * Table function that streams XML records stored in S3-compatible storage.
 */
public final class XmlTableFunction extends AbstractConnectorTableFunction {
    private static final String ROW_ELEMENT_ARGUMENT = "ROW_ELEMENT";
    private static final String INCLUDE_TEXT_ARGUMENT = "INCLUDE_TEXT";
    private static final String EMPTY_AS_NULL_ARGUMENT = "EMPTY_AS_NULL";
    private static final String INVALID_ROW_COLUMN_ARGUMENT = "INVALID_ROW_COLUMN";

    private static final Logger logger = Logger.get(XmlTableFunction.class);
    private final S3ClientBuilder s3ClientBuilder;
    private final PageSettings pageSettings;

    public XmlTableFunction(S3ClientBuilder s3ClientBuilder) {
        this(s3ClientBuilder, PageSettings.DEFAULT);
    }

    public XmlTableFunction(S3ClientBuilder s3ClientBuilder, PageSettings pageSettings) {
        super(
                "xml",
                "load",
                fileArguments(
                        encodingArgumentSpecification(),
                        ScalarArgumentSpecification.builder()
                                .name(ROW_ELEMENT_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice("row"))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(INCLUDE_TEXT_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice("false"))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(EMPTY_AS_NULL_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice("false"))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(INVALID_ROW_COLUMN_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice("_errors"))
                                .build()
                ),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.s3ClientBuilder = requireNonNull(s3ClientBuilder, "s3ClientBuilder is null");
        this.pageSettings = requireNonNull(pageSettings, "pageSettings is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session,
                                         ConnectorTransactionHandle transactionHandle,
                                         Map<String, Argument> arguments,
                                         ConnectorAccessControl accessControl) {
        String s3Path = requirePath(arguments);
        Charset charset = resolveEncoding(arguments);
        String rowElement = resolveRowElement(arguments);
        boolean includeText = resolveIncludeText(arguments);
        boolean emptyAsNull = resolveEmptyAsNull(arguments);
        String invalidRowColumn = resolveInvalidRowColumn(arguments);

        XmlFormatSupport.Schema schema;
        FileSelection selection;

        try (S3ClientBuilder.SessionClient s3 = s3ClientBuilder.forSession(session)) {
            selection = FileSelection.resolve(s3, s3Path, arguments);
            schema = selection.infer(s3, charset, (reader, object) ->
                    XmlFormatSupport.inferSchema(reader, object.path(), rowElement));
            if (!includeText) {
                schema = filterTextColumn(schema);
            }
            if (!invalidRowColumn.isEmpty()) {
                schema = XmlFormatSupport.appendRawColumn(schema, invalidRowColumn);
            }
        }
        catch (IOException e) {
            logger.error(e, "Failed to infer XML schema for %s", s3Path);
            throw new UncheckedIOException("Failed to infer XML schema", e);
        }

        List<String> columnNames = schema.columnNames();

        logger.info("Detected %s XML field(s) for %s", columnNames.size(), s3Path);
        Handle handle = new Handle(
                selection,
                new ScanSettings(Integer.MAX_VALUE, pageSettings, charset.name()),
                schema,
                new XmlOptions(rowElement, emptyAsNull, invalidRowColumn.isEmpty() ? null : invalidRowColumn));
        return TableFunctionAnalysis.builder()
                .returnedType(Descriptor.descriptor(handle.columnNames(), handle.resolveColumnTypes()))
                .handle(handle)
                .build();
    }

    private static String resolveRowElement(Map<String, Argument> arguments) {
        String rowElement = optionalString(arguments, ROW_ELEMENT_ARGUMENT, "row").trim();
        if (rowElement.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "ROW_ELEMENT cannot be empty");
        }
        return rowElement;
    }

    private static boolean resolveIncludeText(Map<String, Argument> arguments) {
        return optionalBoolean(arguments, INCLUDE_TEXT_ARGUMENT, false);
    }

    private static boolean resolveEmptyAsNull(Map<String, Argument> arguments) {
        return optionalBoolean(arguments, EMPTY_AS_NULL_ARGUMENT, false);
    }

    private static String resolveInvalidRowColumn(Map<String, Argument> arguments) {
        return optionalString(arguments, INVALID_ROW_COLUMN_ARGUMENT, "").trim();
    }

    private static XmlFormatSupport.Schema filterTextColumn(XmlFormatSupport.Schema schema) {
        List<XmlFormatSupport.Column> filtered = new ArrayList<>();
        for (XmlFormatSupport.Column column : schema.columns()) {
            if (column.source() != XmlFormatSupport.ColumnSource.TEXT) {
                filtered.add(column);
            }
        }
        return new XmlFormatSupport.Schema(filtered);
    }

    public ConnectorPageSource createPageSource(ConnectorSession session, Handle handle, FileSplit split, List<S3FileColumnHandle> columns) {
        return new PageSource(session, s3ClientBuilder, handle, split, columns);
    }

    public record XmlOptions(
            @JsonProperty("rowElement") String rowElement,
            @JsonProperty("emptyAsNull") boolean emptyAsNull,
            @JsonProperty("invalidRowColumn") String invalidRowColumn) {
        @JsonCreator
        public XmlOptions {
            rowElement = requireNonNull(rowElement, "rowElement is null");
            invalidRowColumn = invalidRowColumn == null ? "" : invalidRowColumn;
        }
    }

    public static final class Handle extends BaseTextFileHandle {
        private final XmlFormatSupport.Schema schema;
        private final XmlOptions options;

        public Handle(
                FileSelection selection,
                ScanSettings scan,
                XmlFormatSupport.Schema schema,
                XmlOptions options) {
            super(selection, scan);
            this.schema = requireNonNull(schema, "schema is null");
            this.options = requireNonNull(options, "options is null");
        }

        @JsonCreator
        public Handle(
                @JsonProperty("pathColumn") String pathColumn,
                @JsonProperty("scan") ScanSettings scan,
                @JsonProperty("schema") XmlFormatSupport.Schema schema,
                @JsonProperty("options") XmlOptions options) {
            super(pathColumn, scan);
            this.schema = requireNonNull(schema, "schema is null");
            this.options = requireNonNull(options, "options is null");
        }

        @Override
        protected boolean splittable() {
            return false;
        }

        @Override
        public String format() {
            return "xml";
        }

        @JsonProperty("schema")
        public XmlFormatSupport.Schema schema() {
            return schema;
        }

        @JsonProperty("options")
        public XmlOptions options() {
            return options;
        }

        @Override
        public List<String> dataColumnNames() {
            return schema.columnNames();
        }

        @Override
        public List<Type> dataColumnTypes() {
            return Collections.nCopies(schema.columns().size(), VarcharType.VARCHAR);
        }
    }

    private static final class PageSource extends AbstractTextFilePageSource<Handle, String[]> {
        private static final int READER_BUFFER_SIZE = 8192;
        // Known shallow reader sizes and fixed arrays; StAX/charset decoder internals are excluded.
        private static final long READER_MEMORY = instanceSize(BufferedReader.class) + instanceSize(InputStreamReader.class)
                + sizeOfCharArray(READER_BUFFER_SIZE) + sizeOfByteArray(READER_BUFFER_SIZE);
        private BufferedReader reader;
        private XMLStreamReader xmlReader;

        private PageSource(
                ConnectorSession session,
                S3ClientBuilder s3ClientBuilder,
                Handle handle,
                FileSplit split,
                List<S3FileColumnHandle> projectedColumns) {
            super(session, s3ClientBuilder, handle, split, projectedColumns);
        }

        @Override
        protected void openSource() throws IOException {
            reader = new BufferedReader(new InputStreamReader(openStream(), charset), READER_BUFFER_SIZE);
            try {
                this.xmlReader = XmlFormatSupport.newXmlReader(reader);
            }
            catch (XMLStreamException e) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Failed to initialise XML reader for " + split.object().path(), e);
            }
        }

        @Override
        protected RecordReadResult<String[]> readNextRecord() throws IOException {
            if (xmlReader == null) {
                return RecordReadResult.finished();
            }
            try {
                XmlFormatSupport.RowExtraction row = XmlFormatSupport.readNextRecord(xmlReader, handle.schema(), handle.options().rowElement(), handle.options().emptyAsNull());
                if (row.done()) {
                    return RecordReadResult.finished();
                }
                return RecordReadResult.produce(row.values(), 0);
            }
            catch (XMLStreamException e) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Failed to read XML record for " + split.object().path(), e);
            }
        }

        @Override
        protected void appendRecord(PageBuilder pageBuilder, String[] values) {
            for (int outputIndex = 0; outputIndex < projectedColumns.size(); outputIndex++) {
                S3FileColumnHandle columnHandle = projectedColumns.get(outputIndex);
                int sourceIndex = columnHandle.getOrdinalPosition();
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputIndex);
                String value = values[sourceIndex];
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    ((VarcharType) projectedTypes().get(outputIndex)).writeSlice(blockBuilder, Slices.utf8Slice(value));
                }
            }
            pageBuilder.declarePosition();
        }

        @Override
        public long getMemoryUsage() {
            return super.getMemoryUsage() + (reader == null ? 0 : READER_MEMORY);
        }

        @Override
        protected void closeReader() throws IOException {
            try {
                if (xmlReader != null) {
                    xmlReader.close();
                }
            }
            catch (XMLStreamException e) {
                throw new IOException("Failed to close XML reader", e);
            }
            finally {
                xmlReader = null;
                if (reader != null) {
                    try {
                        reader.close();
                    }
                    finally {
                        reader = null;
                    }
                }
            }
        }
    }
}

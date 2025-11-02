package marcbp.trino.s3file.xml;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.file.AbstractFileProcessor;
import marcbp.trino.s3file.file.BaseFileHandle;
import marcbp.trino.s3file.file.BaseFileProcessorProvider;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.FileSplitProcessor;
import marcbp.trino.s3file.s3.S3ClientBuilder;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;
import static marcbp.trino.s3file.util.TableFunctionArguments.encodingArgumentSpecification;
import static marcbp.trino.s3file.util.TableFunctionArguments.pathArgumentSpecification;
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

    public XmlTableFunction(S3ClientBuilder s3ClientBuilder) {
        super(
                "xml",
                "load",
                List.of(
                        pathArgumentSpecification(),
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
        S3ClientBuilder.ObjectMetadata metadata;

        try (S3ClientBuilder.SessionClient s3 = s3ClientBuilder.forSession(session);
             BufferedReader reader = s3.openReader(s3Path, charset)) {
            metadata = s3.getObjectMetadata(s3Path);
            schema = XmlFormatSupport.inferSchema(reader, s3Path, rowElement);
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
        List<Type> columnTypes = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            columnTypes.add(VarcharType.createUnboundedVarcharType());
        }
        Descriptor descriptor = Descriptor.descriptor(columnNames, columnTypes);

        logger.info("Detected %s XML field(s) for %s", columnNames.size(), s3Path);
        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .handle(new Handle(
                        s3Path,
                        rowElement,
                        schema.columns(),
                        emptyAsNull,
                        invalidRowColumn.isEmpty() ? null : invalidRowColumn,
                        null,
                        metadata.size(),
                        charset.name(),
                        metadata.eTag().orElse(null),
                        metadata.versionId().orElse(null)))
                .build();
    }

    private static String resolveRowElement(Map<String, Argument> arguments) {
        ScalarArgument argument = (ScalarArgument) arguments.get(ROW_ELEMENT_ARGUMENT);
        if (argument == null) {
            return "row";
        }
        Object value = argument.getValue();
        if (!(value instanceof Slice slice)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "ROW_ELEMENT must be a string");
        }
        String rowElement = slice.toStringUtf8().trim();
        if (rowElement.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "ROW_ELEMENT cannot be empty");
        }
        return rowElement;
    }

    private static boolean resolveIncludeText(Map<String, Argument> arguments) {
        ScalarArgument argument = (ScalarArgument) arguments.get(INCLUDE_TEXT_ARGUMENT);
        if (argument == null) {
            return false;
        }
        Object value = argument.getValue();
        if (!(value instanceof Slice slice)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "INCLUDE_TEXT must be a string boolean");
        }
        return Boolean.parseBoolean(slice.toStringUtf8().trim());
    }

    private static boolean resolveEmptyAsNull(Map<String, Argument> arguments) {
        ScalarArgument argument = (ScalarArgument) arguments.get(EMPTY_AS_NULL_ARGUMENT);
        if (argument == null) {
            return false;
        }
        Object value = argument.getValue();
        if (!(value instanceof Slice slice)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "EMPTY_AS_NULL must be a string boolean");
        }
        return Boolean.parseBoolean(slice.toStringUtf8().trim());
    }

    private static String resolveInvalidRowColumn(Map<String, Argument> arguments) {
        ScalarArgument argument = (ScalarArgument) arguments.get(INVALID_ROW_COLUMN_ARGUMENT);
        if (argument == null) {
            return "";
        }
        Object value = argument.getValue();
        if (!(value instanceof Slice slice)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "INVALID_ROW_COLUMN must be a string");
        }
        return slice.toStringUtf8().trim();
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

    public TableFunctionProcessorProvider createProcessorProvider() {
        return new ProcessorProvider();
    }

    public List<FileSplit> createSplits(Handle handle) {
        return List.of(handle.toWholeFileSplit());
    }

    public TableFunctionSplitProcessor createSplitProcessor(ConnectorSession session, Handle handle, FileSplit split) {
        return new FileSplitProcessor(new Processor(session, s3ClientBuilder, handle, split));
    }

    public static final class Handle extends BaseFileHandle {
        private final String rowElement;
        private final List<XmlFormatSupport.Column> columns;
        private final boolean emptyAsNull;
        private final String invalidRowColumn;

        @JsonCreator
        public Handle(@JsonProperty("s3Path") String s3Path,
                      @JsonProperty("rowElement") String rowElement,
                      @JsonProperty("columns") List<XmlFormatSupport.Column> columns,
                      @JsonProperty("emptyAsNull") boolean emptyAsNull,
                      @JsonProperty("invalidRowColumn") String invalidRowColumn,
                      @JsonProperty("batchSize") Integer batchSize,
                      @JsonProperty("fileSize") long fileSize,
                      @JsonProperty("charset") String charsetName,
                      @JsonProperty("etag") String eTag,
                      @JsonProperty("versionId") String versionId) {
            super(
                    s3Path,
                    fileSize,
                    Integer.MAX_VALUE,
                    charsetName,
                    batchSize == null ? BaseFileHandle.DEFAULT_BATCH_SIZE : batchSize,
                    Optional.ofNullable(eTag),
                    Optional.ofNullable(versionId));
            this.rowElement = requireNonNull(rowElement, "rowElement is null");
            this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
            this.emptyAsNull = emptyAsNull;
            this.invalidRowColumn = invalidRowColumn == null ? "" : invalidRowColumn;
        }

        @JsonProperty
        public String getRowElement() {
            return rowElement;
        }

        @JsonProperty
        public List<XmlFormatSupport.Column> getColumns() {
            return columns;
        }

        @JsonProperty("emptyAsNull")
        public boolean isEmptyAsNull() {
            return emptyAsNull;
        }

        @JsonProperty("invalidRowColumn")
        public String getInvalidRowColumn() {
            return invalidRowColumn;
        }

        public boolean hasInvalidRowColumn() {
            return !invalidRowColumn.isEmpty();
        }

        public List<String> columnNames() {
            List<String> names = new ArrayList<>(columns.size());
            for (XmlFormatSupport.Column column : columns) {
                names.add(column.name());
            }
            return names;
        }

        public List<Type> resolveColumnTypes() {
            List<Type> types = new ArrayList<>(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                types.add(VarcharType.createUnboundedVarcharType());
            }
            return List.copyOf(types);
        }
    }

    private final class ProcessorProvider extends BaseFileProcessorProvider<Handle> {
        private ProcessorProvider() {
            super(Handle.class);
        }

        @Override
        protected TableFunctionDataProcessor createDataProcessor(ConnectorSession session, Handle handle) {
            logger.info("Creating XML data processor for %s", handle.getS3Path());
            return new Processor(session, s3ClientBuilder, handle, null);
        }

        @Override
        protected TableFunctionSplitProcessor createSplitProcessor(ConnectorSession session, Handle handle, FileSplit split) {
            return XmlTableFunction.this.createSplitProcessor(session, handle, split);
        }
    }

    private static final class Processor extends AbstractFileProcessor<Handle> {
        private final List<Type> columnTypes;
        private final XmlFormatSupport.Schema schema;
        private final boolean emptyAsNull;
        private XMLStreamReader xmlReader;

        private Processor(ConnectorSession session, S3ClientBuilder s3ClientBuilder, Handle handle, FileSplit split) {
            super(session, s3ClientBuilder, handle, split);
            this.columnTypes = handle.resolveColumnTypes();
            this.schema = new XmlFormatSupport.Schema(handle.getColumns());
            this.emptyAsNull = handle.isEmptyAsNull();
        }

        @Override
        protected void afterReaderOpened(BufferedReader reader) throws IOException {
            try {
                this.xmlReader = XmlFormatSupport.newXmlReader(reader);
            }
            catch (XMLStreamException e) {
                throw new IOException("Failed to initialise XML reader for " + handle.getS3Path(), e);
            }
        }

        @Override
        protected List<Type> columnTypes() {
            return columnTypes;
        }

        @Override
        protected RecordReadResult<?> readNextRecord() throws IOException {
            if (xmlReader == null) {
                return RecordReadResult.finished();
            }
            try {
                XmlFormatSupport.RowExtraction row = XmlFormatSupport.readNextRecord(xmlReader, schema, handle.getRowElement(), emptyAsNull);
                if (row.done()) {
                    return RecordReadResult.finished();
                }
                return RecordReadResult.produce(row.values(), 0, false);
            }
            catch (XMLStreamException e) {
                throw new IOException("Failed to read XML record for " + handle.getS3Path(), e);
            }
        }

        @Override
        protected void appendRecord(PageBuilder pageBuilder, Object payload) {
            String[] values = (String[]) payload;
            for (int columnIndex = 0; columnIndex < values.length; columnIndex++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(columnIndex);
                String value = values[columnIndex];
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    ((VarcharType) columnTypes.get(columnIndex)).writeSlice(blockBuilder, Slices.utf8Slice(value));
                }
            }
            pageBuilder.declarePosition();
        }

        @Override
        protected void closeReader() {
            if (xmlReader != null) {
                try {
                    xmlReader.close();
                }
                catch (XMLStreamException e) {
                    logger.warn(e, "Failed to close XML reader for %s", handle.getS3Path());
                }
                finally {
                    xmlReader = null;
                }
            }
            super.closeReader();
        }
    }
}

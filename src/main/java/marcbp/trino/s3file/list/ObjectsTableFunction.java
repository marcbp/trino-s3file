package marcbp.trino.s3file.list;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.file.AnalysisStats;
import marcbp.trino.s3file.file.RuntimeTableHandle;
import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

/**
 * Table function that lists objects and common prefixes from an S3 bucket or prefix.
 */
public final class ObjectsTableFunction extends AbstractConnectorTableFunction {
    public static final String CANONICAL_SCHEMA = "list";
    public static final String CANONICAL_NAME = "objects";
    public static final String LEGACY_SCHEMA = "objects";
    public static final String LEGACY_NAME = "list";
    private static final String BUCKET_ARGUMENT = "BUCKET";
    private static final String PREFIX_ARGUMENT = "PREFIX";
    private static final String RECURSIVE_ARGUMENT = "RECURSIVE_LISTING";
    private static final String INCLUDE_PREFIXES_ARGUMENT = "INCLUDE_PREFIXES";
    private static final List<String> COLUMN_NAMES = List.of(
            "path",
            "bucket",
            "key",
            "name",
            "parent",
            "size",
            "last_modified",
            "etag",
            "type");
    private static final List<Type> COLUMN_TYPES = List.of(
            VarcharType.createUnboundedVarcharType(),
            VarcharType.createUnboundedVarcharType(),
            VarcharType.createUnboundedVarcharType(),
            VarcharType.createUnboundedVarcharType(),
            VarcharType.createUnboundedVarcharType(),
            BigintType.BIGINT,
            TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS,
            VarcharType.createUnboundedVarcharType(),
            VarcharType.createUnboundedVarcharType());

    private final S3ClientBuilder s3ClientBuilder;
    private final Logger logger = Logger.get(ObjectsTableFunction.class);

    public ObjectsTableFunction(S3ClientBuilder s3ClientBuilder) {
        this(s3ClientBuilder, CANONICAL_SCHEMA, CANONICAL_NAME);
    }

    public ObjectsTableFunction(S3ClientBuilder s3ClientBuilder, String schema, String name) {
        super(
                schema,
                name,
                List.of(
                        ScalarArgumentSpecification.builder()
                                .name(BUCKET_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(PREFIX_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice(""))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(RECURSIVE_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice("true"))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(INCLUDE_PREFIXES_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice("false"))
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
        String bucket = requireString(arguments, BUCKET_ARGUMENT);
        String prefix = optionalString(arguments, PREFIX_ARGUMENT, "");
        boolean recursive = parseBoolean(arguments, RECURSIVE_ARGUMENT, true);
        boolean includePrefixes = parseBoolean(arguments, INCLUDE_PREFIXES_ARGUMENT, false);

        logger.info("Analyzing list.objects for bucket %s and prefix %s (recursive=%s, includePrefixes=%s)", bucket, prefix, recursive, includePrefixes);

        Descriptor descriptor = Descriptor.descriptor(COLUMN_NAMES, COLUMN_TYPES);
        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .handle(new Handle(
                        bucket,
                        prefix,
                        recursive,
                        includePrefixes,
                        new AnalysisStats(0L, COLUMN_NAMES.size(), 0L)))
                .build();
    }

    public ConnectorPageSource createPageSource(ConnectorSession session, Handle handle, ListingSplit split, List<S3FileColumnHandle> columns) {
        return new ListingPageSource(
                session,
                s3ClientBuilder,
                (sessionClient, continuationToken) -> {
                    S3ClientBuilder.ListObjectsPage page = sessionClient.listObjects(handle.bucket(), handle.prefix(), handle.recursive(), continuationToken);
                    List<ListingRow> rows = new ArrayList<>(page.objects().size() + (handle.includePrefixes() ? page.prefixes().size() : 0));
                    for (S3ClientBuilder.ListedObject object : page.objects()) {
                        rows.add(Row.object(object));
                    }
                    if (handle.includePrefixes()) {
                        for (S3ClientBuilder.ListedPrefix prefix : page.prefixes()) {
                            rows.add(Row.prefix(prefix));
                        }
                    }
                    return new ListingPage(rows, page.nextContinuationToken());
                },
                columns,
                handle.resolveColumnTypes());
    }

    private static String requireString(Map<String, Argument> arguments, String name) {
        ScalarArgument arg = (ScalarArgument) arguments.get(name);
        if (arg == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Argument " + name + " is required");
        }
        String value = argumentStringValue(arg, name);
        if (value.isBlank()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, name + " cannot be blank");
        }
        if (value.startsWith("s3://")) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, name + " must not include an s3:// URI");
        }
        return value;
    }

    private static String optionalString(Map<String, Argument> arguments, String name, String defaultValue) {
        ScalarArgument arg = (ScalarArgument) arguments.get(name);
        if (arg == null) {
            return defaultValue;
        }
        String value = argumentStringValue(arg, name);
        if (value.startsWith("s3://")) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, name + " must not include an s3:// URI");
        }
        if (value.startsWith("/")) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, name + " must not start with /");
        }
        return value;
    }

    private static String argumentStringValue(ScalarArgument arg, String name) {
        if (!(arg.getValue() instanceof io.airlift.slice.Slice slice)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, name + " must be a string");
        }
        return slice.toStringUtf8();
    }

    private static boolean parseBoolean(Map<String, Argument> arguments, String name, boolean defaultValue) {
        ScalarArgument arg = (ScalarArgument) arguments.get(name);
        if (arg == null || !(arg.getValue() instanceof io.airlift.slice.Slice slice)) {
            return defaultValue;
        }
        String text = slice.toStringUtf8().trim();
        if (text.isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(text);
    }

    public static final class Handle implements RuntimeTableHandle {
        private final String bucket;
        private final String prefix;
        private final boolean recursive;
        private final boolean includePrefixes;
        private final AnalysisStats analysis;

        @JsonCreator
        public Handle(
                @JsonProperty("bucket") String bucket,
                @JsonProperty("prefix") String prefix,
                @JsonProperty("recursive") boolean recursive,
                @JsonProperty("includePrefixes") boolean includePrefixes,
                @JsonProperty("analysis") AnalysisStats analysis) {
            this.bucket = requireNonNull(bucket, "bucket is null");
            this.prefix = prefix == null ? "" : prefix;
            this.recursive = recursive;
            this.includePrefixes = includePrefixes;
            this.analysis = requireNonNull(analysis, "analysis is null");
        }

        @JsonProperty
        public String bucket() {
            return bucket;
        }

        @JsonProperty
        public String prefix() {
            return prefix;
        }

        @JsonProperty
        public boolean recursive() {
            return recursive;
        }

        @JsonProperty
        public boolean includePrefixes() {
            return includePrefixes;
        }

        @JsonProperty
        public AnalysisStats analysis() {
            return analysis;
        }

        @JsonIgnore
        @Override
        public String runtimeTableName() {
            return "objects_list";
        }

        @JsonIgnore
        @Override
        public List<String> columnNames() {
            return COLUMN_NAMES;
        }

        @JsonIgnore
        @Override
        public List<Type> resolveColumnTypes() {
            return COLUMN_TYPES;
        }
    }

    private record Row(
            String path,
            String bucket,
            String key,
            String name,
            String parent,
            Long size,
            Long lastModified,
            String etag,
            String type)
            implements ListingRow {
        static Row object(S3ClientBuilder.ListedObject object) {
            return new Row(
                    object.path(),
                    object.bucket(),
                    object.key(),
                    object.name(),
                    object.parent(),
                    object.size(),
                    object.lastModified().map(instant -> instant.toEpochMilli()).orElse(null),
                    object.eTag().orElse(null),
                    "object");
        }

        static Row prefix(S3ClientBuilder.ListedPrefix prefix) {
            return new Row(
                    prefix.path(),
                    prefix.bucket(),
                    prefix.prefix(),
                    prefix.name(),
                    prefix.parent(),
                    null,
                    null,
                    null,
                    "prefix");
        }

        @Override
        public Object valueFor(String columnName) {
            return switch (columnName) {
                case "path" -> path;
                case "bucket" -> bucket;
                case "key" -> key;
                case "name" -> name;
                case "parent" -> parent;
                case "size" -> size;
                case "last_modified" -> lastModified == null ? null : DateTimeEncoding.packDateTimeWithZone(lastModified, TimeZoneKey.UTC_KEY);
                case "etag" -> etag;
                case "type" -> type;
                default -> throw new IllegalArgumentException("Unexpected column: " + columnName);
            };
        }
    }
}

package marcbp.trino.s3file.objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
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
import marcbp.trino.s3file.s3.S3UriUtils;
import marcbp.trino.s3file.s3.S3UriUtils.S3Location;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;

import static java.util.Objects.requireNonNull;

/**
 * Table function that lists objects and common prefixes from an S3 bucket or prefix.
 */
public final class ObjectsTableFunction extends AbstractConnectorTableFunction {
    private static final String RECURSIVE_ARGUMENT = "RECURSIVE_LISTING";
    private static final String INCLUDE_PREFIXES_ARGUMENT = "INCLUDE_PREFIXES";
    private static final int PAGE_BUILDER_BATCH_SIZE = 1024;
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
        super(
                "objects",
                "list",
                List.of(
                        marcbp.trino.s3file.util.TableFunctionArguments.pathArgumentSpecification(),
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
        String s3Path = marcbp.trino.s3file.util.TableFunctionArguments.requirePath(arguments);
        boolean recursive = parseBoolean(arguments, RECURSIVE_ARGUMENT, true);
        boolean includePrefixes = parseBoolean(arguments, INCLUDE_PREFIXES_ARGUMENT, false);
        S3Location location = S3UriUtils.parsePrefix(s3Path);

        logger.info("Analyzing objects.list for path %s (recursive=%s, includePrefixes=%s)", s3Path, recursive, includePrefixes);

        Descriptor descriptor = Descriptor.descriptor(COLUMN_NAMES, COLUMN_TYPES);
        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .handle(new Handle(
                        location.bucket(),
                        location.key(),
                        recursive,
                        includePrefixes,
                        new AnalysisStats(0L, COLUMN_NAMES.size(), 0L)))
                .build();
    }

    public ConnectorPageSource createPageSource(ConnectorSession session, Handle handle, ObjectListSplit split, List<S3FileColumnHandle> columns) {
        return new PageSource(session, s3ClientBuilder, handle, columns);
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

    private static final class PageSource implements ConnectorPageSource {
        private final S3ClientBuilder.SessionClient sessionClient;
        private final Handle handle;
        private final List<S3FileColumnHandle> projectedColumns;
        private final List<Type> projectedTypes;
        private final Queue<Row> buffer = new ArrayDeque<>();
        private String continuationToken;
        private boolean exhausted;
        private boolean finished;
        private boolean closed;
        private boolean sessionClosed;
        private long completedPositions;
        private long readTimeNanos;
        private long pagesProduced;

        private PageSource(ConnectorSession session, S3ClientBuilder s3ClientBuilder, Handle handle, List<S3FileColumnHandle> projectedColumns) {
            this.sessionClient = requireNonNull(s3ClientBuilder, "s3ClientBuilder is null").forSession(session);
            this.handle = requireNonNull(handle, "handle is null");
            this.projectedColumns = List.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
            List<Type> allTypes = handle.resolveColumnTypes();
            this.projectedTypes = this.projectedColumns.stream()
                    .map(column -> allTypes.get(column.getOrdinalPosition()))
                    .toList();
        }

        @Override
        public long getCompletedBytes() {
            return 0;
        }

        @Override
        public OptionalLong getCompletedPositions() {
            return OptionalLong.of(completedPositions);
        }

        @Override
        public long getReadTimeNanos() {
            return readTimeNanos;
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public io.trino.spi.connector.SourcePage getNextSourcePage() {
            long startNanos = System.nanoTime();
            try {
                if (finished) {
                    closeSession();
                    return null;
                }

                PageBuilder pageBuilder = new PageBuilder(PAGE_BUILDER_BATCH_SIZE, projectedTypes);
                while (!pageBuilder.isFull()) {
                if (buffer.isEmpty()) {
                        if (!fetchNextBatch()) {
                            break;
                        }
                        if (buffer.isEmpty()) {
                            continue;
                        }
                    }
                    Row row = buffer.poll();
                    if (row == null) {
                        continue;
                    }
                    appendRow(pageBuilder, row);
                    completedPositions++;
                }

                if (pageBuilder.isEmpty()) {
                    markFinished();
                    return null;
                }

                pagesProduced++;
                return io.trino.spi.connector.SourcePage.create(pageBuilder.build());
            }
            catch (RuntimeException e) {
                closeSession();
                throw e;
            }
            finally {
                readTimeNanos += System.nanoTime() - startNanos;
            }
        }

        @Override
        public long getMemoryUsage() {
            return 0;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            finished = true;
            closeSession();
        }

        private boolean fetchNextBatch() {
            if (finished || exhausted) {
                return false;
            }

            S3ClientBuilder.ListObjectsPage page = sessionClient.listObjects(handle.bucket(), handle.prefix(), handle.recursive(), Optional.ofNullable(continuationToken));
            for (S3ClientBuilder.ListedObject object : page.objects()) {
                buffer.add(Row.object(object));
            }
            if (handle.includePrefixes()) {
                for (S3ClientBuilder.ListedPrefix prefix : page.prefixes()) {
                    buffer.add(Row.prefix(prefix));
                }
            }
            continuationToken = page.nextContinuationToken().orElse(null);
            if (continuationToken == null) {
                exhausted = true;
            }
            if (buffer.isEmpty() && exhausted) {
                finished = true;
                closeSession();
                return false;
            }
            return true;
        }

        private void appendRow(PageBuilder pageBuilder, Row row) {
            for (int outputIndex = 0; outputIndex < projectedColumns.size(); outputIndex++) {
                S3FileColumnHandle columnHandle = projectedColumns.get(outputIndex);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputIndex);
                Object value = row.valueFor(columnHandle.getName());
                if (value == null) {
                    blockBuilder.appendNull();
                    continue;
                }
                Type type = projectedTypes.get(outputIndex);
                if (type == BigintType.BIGINT) {
                    BigintType.BIGINT.writeLong(blockBuilder, (Long) value);
                }
                else if (type == TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS) {
                    TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, (Long) value);
                }
                else {
                    VarcharType.createUnboundedVarcharType().writeSlice(blockBuilder, Slices.utf8Slice(value.toString()));
                }
            }
            pageBuilder.declarePosition();
        }

        private void markFinished() {
            finished = true;
            closeSession();
        }

        private void closeSession() {
            if (sessionClosed) {
                return;
            }
            sessionClosed = true;
            sessionClient.close();
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
            String type) {
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

        Object valueFor(String columnName) {
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

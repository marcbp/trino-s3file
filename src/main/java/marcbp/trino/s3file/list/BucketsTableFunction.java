package marcbp.trino.s3file.list;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.file.AnalysisStats;
import marcbp.trino.s3file.file.RuntimeTableHandle;
import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Table function that lists buckets visible to the configured S3 credentials.
 */
public final class BucketsTableFunction extends AbstractConnectorTableFunction {
    private static final List<String> COLUMN_NAMES = List.of(
            "path",
            "bucket",
            "creation_date");
    private static final List<Type> COLUMN_TYPES = List.of(
            VarcharType.createUnboundedVarcharType(),
            VarcharType.createUnboundedVarcharType(),
            TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS);

    private final S3ClientBuilder s3ClientBuilder;
    private final Logger logger = Logger.get(BucketsTableFunction.class);

    public BucketsTableFunction(S3ClientBuilder s3ClientBuilder) {
        super(
                "list",
                "buckets",
                List.of(),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.s3ClientBuilder = requireNonNull(s3ClientBuilder, "s3ClientBuilder is null");
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transactionHandle,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl) {
        logger.info("Analyzing list.buckets");

        return TableFunctionAnalysis.builder()
                .returnedType(Descriptor.descriptor(COLUMN_NAMES, COLUMN_TYPES))
                .handle(new Handle(new AnalysisStats(0L, COLUMN_NAMES.size(), 0L)))
                .build();
    }

    public ConnectorPageSource createPageSource(ConnectorSession session, Handle handle, ListingSplit split, List<S3FileColumnHandle> columns) {
        return new ListingPageSource(
                session,
                s3ClientBuilder,
                (sessionClient, continuationToken) -> new ListingPage(
                        sessionClient.listBuckets().stream()
                                .map(Row::bucket)
                                .map(ListingRow.class::cast)
                                .toList(),
                        Optional.empty()),
                columns,
                handle.resolveColumnTypes());
    }

    public static final class Handle implements RuntimeTableHandle {
        private final AnalysisStats analysis;

        @JsonCreator
        public Handle(@JsonProperty("analysis") AnalysisStats analysis) {
            this.analysis = requireNonNull(analysis, "analysis is null");
        }

        @JsonProperty
        public AnalysisStats analysis() {
            return analysis;
        }

        @JsonIgnore
        @Override
        public String runtimeTableName() {
            return "buckets_list";
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
            Long creationDate)
            implements ListingRow {
        static Row bucket(S3ClientBuilder.ListedBucket bucket) {
            return new Row(
                    bucket.path(),
                    bucket.name(),
                    bucket.creationDate().map(instant -> instant.toEpochMilli()).orElse(null));
        }

        @Override
        public Object valueFor(String columnName) {
            return switch (columnName) {
                case "path" -> path;
                case "bucket" -> bucket;
                case "creation_date" -> creationDate == null ? null : DateTimeEncoding.packDateTimeWithZone(creationDate, TimeZoneKey.UTC_KEY);
                default -> throw new IllegalArgumentException("Unexpected column: " + columnName);
            };
        }
    }
}

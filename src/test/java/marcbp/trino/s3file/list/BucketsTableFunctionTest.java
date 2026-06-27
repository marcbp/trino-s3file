package marcbp.trino.s3file.list;

import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.s3.S3ClientBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BucketsTableFunctionTest {
    private final S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
    private final S3ClientBuilder.SessionClient sessionClient = mock(S3ClientBuilder.SessionClient.class);
    private final BucketsTableFunction function = new BucketsTableFunction(s3ClientBuilder);

    @BeforeEach
    void setUp() {
        reset(s3ClientBuilder, sessionClient);
        when(s3ClientBuilder.forSession(any(ConnectorSession.class))).thenReturn(sessionClient);
        doNothing().when(sessionClient).close();
    }

    @Test
    void analyzeBuildsBucketListingDescriptor() {
        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                Map.of(),
                null);

        Descriptor expectedDescriptor = Descriptor.descriptor(
                List.of("path", "bucket", "creation_date"),
                List.of(
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType(),
                        TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS));
        assertEquals(expectedDescriptor, analysis.getReturnedType().orElseThrow());

        BucketsTableFunction.Handle handle = (BucketsTableFunction.Handle) analysis.getHandle();
        assertEquals("buckets_list", handle.runtimeTableName());
        assertEquals(List.of("path", "bucket", "creation_date"), handle.columnNames());
        assertEquals(3, handle.resolveColumnTypes().size());
        assertEquals(0L, handle.analysis().rowsSampled());
    }

    @Test
    void pageSourceStreamsBuckets() {
        when(sessionClient.listBuckets()).thenReturn(List.of(
                new S3ClientBuilder.ListedBucket("alpha", Optional.of(Instant.parse("2026-01-02T03:04:05Z"))),
                new S3ClientBuilder.ListedBucket("beta", Optional.empty())));

        BucketsTableFunction.Handle handle = (BucketsTableFunction.Handle) function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                Map.of(),
                null).getHandle();

        List<S3FileColumnHandle> columns = List.of(
                new S3FileColumnHandle("path", 0),
                new S3FileColumnHandle("bucket", 1),
                new S3FileColumnHandle("creation_date", 2));

        ConnectorPageSource pageSource = function.createPageSource(mock(ConnectorSession.class), handle, ListingSplit.INSTANCE, columns);
        SourcePage page = nextPage(pageSource);

        assertEquals(2, page.getPositionCount());
        assertEquals("s3://alpha", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(0), 0));
        assertEquals("alpha", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(1), 0));
        assertFalse(page.getBlock(2).isNull(0));
        assertEquals("s3://beta", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(0), 1));
        assertEquals("beta", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(1), 1));
        assertTrue(page.getBlock(2).isNull(1));
        assertEquals(null, pageSource.getNextSourcePage());

        verify(sessionClient).listBuckets();
        verify(sessionClient).close();
    }

    private static SourcePage nextPage(ConnectorPageSource pageSource) {
        SourcePage page = pageSource.getNextSourcePage();
        assertInstanceOf(SourcePage.class, page);
        return page;
    }
}

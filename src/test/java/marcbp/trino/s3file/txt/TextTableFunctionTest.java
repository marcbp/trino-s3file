package marcbp.trino.s3file.txt;

import io.airlift.slice.Slices;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.file.Compression;
import marcbp.trino.s3file.file.FileSelection;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.PageSettings;
import marcbp.trino.s3file.file.S3ObjectRef;
import marcbp.trino.s3file.file.ScanSettings;
import marcbp.trino.s3file.s3.S3ClientBuilder.ObjectMetadata;
import marcbp.trino.s3file.s3.S3ClientBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests covering analysis and split creation for the text table function.
 */
class TextTableFunctionTest {
    private static final String PATH = "s3://bucket/messages.txt";
    private static final int DEFAULT_SPLIT_SIZE_BYTES = 32 * 1024 * 1024;

    private final S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
    private final S3ClientBuilder.SessionClient sessionClient = mock(S3ClientBuilder.SessionClient.class);
    private final TextTableFunction function = new TextTableFunction(s3ClientBuilder, DEFAULT_SPLIT_SIZE_BYTES);

    @BeforeEach
    void setUp() throws IOException {
        reset(s3ClientBuilder, sessionClient);
        when(s3ClientBuilder.forSession(any(ConnectorSession.class))).thenReturn(sessionClient);
        doNothing().when(sessionClient).close();
    }

    @Test
    void analyzeBuildsHandleWithDecodedLineBreak() {
        when(sessionClient.getObjectMetadata(eq(PATH))).thenReturn(new ObjectMetadata(2048L, Optional.of("etag-text"), Optional.of("v1")));
        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "LINE_BREAK", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("\\r\\n"))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        assertHandle(analysis, "\r\n", 2048L, Optional.of("etag-text"), Optional.of("v1"));
        assertEquals(Descriptor.descriptor(List.of("line"), List.of(VarcharType.createUnboundedVarcharType())),
                analysis.getReturnedType().orElseThrow());
        verify(sessionClient).getObjectMetadata(eq(PATH));
        verify(sessionClient).close();
    }

    @Test
    void analyzeAllowsSplitSizeOverridePerRequest() {
        when(sessionClient.getObjectMetadata(eq(PATH))).thenReturn(new ObjectMetadata(2048L, Optional.empty(), Optional.empty()));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "SPLIT_SIZE_MB", new ScalarArgument(BigintType.BIGINT, 1L)
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        TextTableFunction.Handle handle = (TextTableFunction.Handle) analysis.getHandle();
        assertEquals(1024 * 1024, handle.scan().splitSizeBytes());
    }

    @Test
    void pageSourceSkipsPartialFirstRecordOnNonInitialSplit() throws IOException {
        when(sessionClient.readBytes(eq(PATH), eq(4L), eq(5L), any(), any())).thenReturn(new byte[] {'x'});
        when(sessionClient.openStream(eq(PATH), eq(5L), eq(40L), any(), any())).thenAnswer(invocation ->
                stream("a;bravo;charlie;", StandardCharsets.UTF_8));

        TextTableFunction.Handle handle = handle(";", 128, 32, StandardCharsets.UTF_8, null, null);
        FileSplit split = new FileSplit(handle.selection().objects().getFirst(), "split-1", 5, 20, 40, false, false);

        ConnectorPageSource pageSource = function.createPageSource(mock(ConnectorSession.class), handle, split, allColumns(handle));
        SourcePage page = nextPage(pageSource);

        assertEquals(2, page.getPositionCount());
        assertEquals("bravo", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(0), 0));
        assertEquals("charlie", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(0), 1));
        assertEquals(null, pageSource.getNextSourcePage());

        verify(sessionClient).openStream(eq(PATH), eq(5L), eq(40L), any(), any());
    }

    @Test
    void pageSourceKeepsFirstRecordWhenSplitStartsAfterDelimiter() throws IOException {
        when(sessionClient.readBytes(eq(PATH), eq(5L), eq(6L), any(), any())).thenReturn(new byte[] {';'});
        when(sessionClient.openStream(eq(PATH), eq(6L), eq(40L), any(), any())).thenAnswer(invocation ->
                stream("bravo;charlie;", StandardCharsets.UTF_8));

        TextTableFunction.Handle handle = handle(";", 128, 32, StandardCharsets.UTF_8, null, null);
        FileSplit split = new FileSplit(handle.selection().objects().getFirst(), "split-1", 6, 20, 40, false, false);

        ConnectorPageSource pageSource = function.createPageSource(mock(ConnectorSession.class), handle, split, allColumns(handle));
        SourcePage page = nextPage(pageSource);

        assertEquals(2, page.getPositionCount());
        assertEquals("bravo", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(0), 0));
        assertEquals("charlie", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(0), 1));
        assertEquals(null, pageSource.getNextSourcePage());
    }

    @Test
    void adjacentSplitsKeepRecordLargerThanFormerLookaheadExactlyOnce() throws IOException {
        String largeRecord = "x".repeat(300 * 1024);
        String content = "first\n" + largeRecord + "\nlast\n";
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        long boundary = "first\n".getBytes(StandardCharsets.UTF_8).length + 10L;

        when(sessionClient.openStream(eq(PATH), eq(0L), eq((long) contentBytes.length), any(), any())).thenAnswer(invocation ->
                new ByteArrayInputStream(contentBytes));
        when(sessionClient.readBytes(eq(PATH), eq(boundary - 1), eq(boundary), any(), any())).thenReturn(new byte[] {'x'});
        when(sessionClient.openStream(eq(PATH), eq(boundary), eq((long) contentBytes.length), any(), any())).thenAnswer(invocation ->
                new ByteArrayInputStream(Arrays.copyOfRange(contentBytes, (int) boundary, contentBytes.length)));

        TextTableFunction.Handle handle = handle("\n", contentBytes.length, (int) boundary, StandardCharsets.UTF_8, null, null);
        FileSplit first = new FileSplit(handle.selection().objects().getFirst(), "split-0", 0, boundary, contentBytes.length, true, false);
        FileSplit second = new FileSplit(handle.selection().objects().getFirst(), "split-1", boundary, contentBytes.length, contentBytes.length, false, true);

        ConnectorPageSource firstSource = function.createPageSource(mock(ConnectorSession.class), handle, first, allColumns(handle));
        ConnectorPageSource secondSource = function.createPageSource(mock(ConnectorSession.class), handle, second, allColumns(handle));

        SourcePage firstPage = nextPage(firstSource);
        SourcePage secondPage = nextPage(secondSource);
        assertEquals(2, firstPage.getPositionCount());
        assertEquals("first", VarcharType.createUnboundedVarcharType().getObjectValue(firstPage.getBlock(0), 0));
        assertEquals(largeRecord, VarcharType.createUnboundedVarcharType().getObjectValue(firstPage.getBlock(0), 1));
        assertEquals(1, secondPage.getPositionCount());
        assertEquals("last", VarcharType.createUnboundedVarcharType().getObjectValue(secondPage.getBlock(0), 0));
        assertEquals(null, firstSource.getNextSourcePage());
        assertEquals(null, secondSource.getNextSourcePage());
    }

    @ParameterizedTest(name = "delimiter={0}, boundary={1}, encoding={2}")
    @MethodSource("splitBoundaries")
    void everySplitBoundaryPreservesWholeFileRows(String delimiter, int boundary, Charset charset) throws IOException {
        List<String> expected = List.of("é", "", "bravo", "", "終");
        byte[] data = (String.join(delimiter, expected) + delimiter).getBytes(charset);
        when(sessionClient.openStream(eq(PATH), anyLong(), any(), any(), any())).thenAnswer(invocation -> {
            int start = Math.toIntExact(invocation.getArgument(1, Long.class));
            Long end = invocation.getArgument(2);
            return new ByteArrayInputStream(Arrays.copyOfRange(data, start, end == null ? data.length : Math.toIntExact(end)));
        });
        when(sessionClient.readBytes(eq(PATH), anyLong(), any(), any(), any())).thenAnswer(invocation ->
                Arrays.copyOfRange(data, Math.toIntExact(invocation.getArgument(1, Long.class)),
                        Math.toIntExact(invocation.getArgument(2, Long.class))));
        var handle = handle(delimiter, data.length, data.length, charset, null, null);
        var object = handle.selection().objects().getFirst();

        assertEquals(expected, readRows(handle, List.of(FileSplit.forWholeFile(object))), "whole-file reference");
        assertEquals(expected, readRows(handle, List.of(
                new FileSplit(object, "first", 0, boundary, data.length, true, false),
                new FileSplit(object, "last", boundary, data.length, data.length, false, true))));
    }

    private static Stream<Arguments> splitBoundaries() {
        return Stream.of(StandardCharsets.UTF_8, StandardCharsets.UTF_16LE, StandardCharsets.UTF_16BE)
                .flatMap(charset -> Stream.of("\n", "\r\n", "||", "aba").flatMap(delimiter -> {
                    int size = (String.join(delimiter, "é", "", "bravo", "", "終") + delimiter).getBytes(charset).length;
                    return IntStream.range(1, size).mapToObj(boundary -> Arguments.of(delimiter, boundary, charset));
                }));
    }

    @Test
    void bomDependentEncodingKeepsOneSplitPerFile() throws IOException {
        byte[] data = "é\n終\n".getBytes(StandardCharsets.UTF_16);
        when(sessionClient.openStream(any(), eq(0L), any(), any(), any()))
                .thenAnswer(invocation -> new ByteArrayInputStream(data));
        var handle = new TextTableFunction.Handle(
                new FileSelection(List.of(
                        new S3ObjectRef(PATH, data.length, null, null, Compression.NONE),
                        new S3ObjectRef("s3://bucket/second.txt", data.length, null, null, Compression.NONE)), ""),
                new ScanSettings(1, PageSettings.DEFAULT, "UTF-16"), new TextTableFunction.TextOptions("\n"));
        var splits = handle.createSplits();
        assertEquals(2, splits.size());
        assertTrue(splits.stream().allMatch(FileSplit::isWholeFile));
        assertEquals(List.of("é", "終", "é", "終"), readRows(handle, splits));
    }

    private List<String> readRows(TextTableFunction.Handle handle, List<FileSplit> splits) throws IOException {
        List<String> rows = new ArrayList<>();
        for (FileSplit split : splits) {
            try (var source = function.createPageSource(mock(ConnectorSession.class), handle, split, allColumns(handle))) {
                SourcePage page;
                while ((page = source.getNextSourcePage()) != null) {
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        rows.add((String) VarcharType.VARCHAR.getObjectValue(page.getBlock(0), position));
                    }
                }
                assertTrue(source.isFinished());
                assertEquals(split.primaryLength(), source.getCompletedBytes());
            }
        }
        return rows;
    }

    @Test
    void pageSourceDecodesIso88591BytesIntoJavaStrings() throws IOException {
        byte[] data = "Andr\u00e9\nPr\u00e9jean\n".getBytes(StandardCharsets.ISO_8859_1);
        when(sessionClient.openStream(eq(PATH), eq(0L), any(), any(), any())).thenAnswer(invocation ->
                new ByteArrayInputStream(data));

        TextTableFunction.Handle handle = handle("\n", data.length, 32, StandardCharsets.ISO_8859_1, null, null);

        ConnectorPageSource pageSource = function.createPageSource(
                mock(ConnectorSession.class),
                handle,
                FileSplit.forWholeFile(handle.selection().objects().getFirst()),
                allColumns(handle));

        SourcePage page = nextPage(pageSource);

        assertEquals(2, page.getPositionCount());
        assertEquals("André", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(0), 0));
        assertEquals("Préjean", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(0), 1));
        verify(sessionClient).openStream(eq(PATH), eq(0L), any(), any(), any());
    }

    @Test
    void pageSourceFlushesProgressBeforeBatchIsFullForLargeRecords() throws IOException {
        String largeLine = "x".repeat(8 * 1024 * 1024 + 1);
        String data = largeLine + "\nsmall\n";
        when(sessionClient.openStream(eq(PATH), eq(0L), any(), any(), any())).thenAnswer(invocation ->
                stream(data, StandardCharsets.UTF_8));

        TextTableFunction.Handle handle = handle("\n", data.getBytes(StandardCharsets.UTF_8).length, 32, StandardCharsets.UTF_8, null, null);

        ConnectorPageSource pageSource = function.createPageSource(
                mock(ConnectorSession.class),
                handle,
                FileSplit.forWholeFile(handle.selection().objects().getFirst()),
                allColumns(handle));

        long initialMemory = pageSource.getMemoryUsage();
        assertTrue(initialMemory > 0);
        SourcePage firstPage = nextPage(pageSource);
        assertTrue(pageSource.getMemoryUsage() >= initialMemory + largeLine.length());
        SourcePage secondPage = nextPage(pageSource);
        assertEquals(largeLine, VarcharType.VARCHAR.getObjectValue(firstPage.getBlock(0), 0));
        assertEquals("small", VarcharType.VARCHAR.getObjectValue(secondPage.getBlock(0), 0));

        assertEquals(1, firstPage.getPositionCount());
        assertEquals(1, secondPage.getPositionCount());
        assertEquals(null, pageSource.getNextSourcePage());
        assertTrue(pageSource.getMemoryUsage() <= initialMemory);
    }

    @Test
    void closingBeforeFirstReadDoesNotOpenObject() throws IOException {
        TextTableFunction.Handle handle = handle("\n", 64, 32, StandardCharsets.UTF_8, null, null);
        ConnectorPageSource source = function.createPageSource(
                mock(ConnectorSession.class), handle, FileSplit.forWholeFile(handle.selection().objects().getFirst()), allColumns(handle));

        source.close();
        source.close();

        assertTrue(source.isFinished());
        assertEquals(null, source.getNextSourcePage());
        verify(sessionClient, times(1)).close();
        verifyNoMoreInteractions(sessionClient);
    }

    @Test
    void emptyObjectFinishesAndClosesResources() throws IOException {
        InputStream stream = mock(InputStream.class);
        when(stream.read()).thenReturn(-1);
        when(sessionClient.openStream(eq(PATH), eq(0L), any(), any(), any())).thenReturn(stream);
        TextTableFunction.Handle handle = handle("\n", 0, 32, StandardCharsets.UTF_8, null, null);
        ConnectorPageSource source = function.createPageSource(
                mock(ConnectorSession.class), handle, FileSplit.forWholeFile(handle.selection().objects().getFirst()), List.of());

        assertEquals(null, source.getNextSourcePage());
        assertTrue(source.isFinished());
        assertEquals(0, source.getCompletedBytes());
        source.close();
        verify(sessionClient, times(1)).close();
        verify(stream, times(1)).close();
    }

    @Test
    void readFailureClosesStreamAndSession() throws IOException {
        InputStream stream = mock(InputStream.class);
        IOException failure = new IOException("read failed");
        when(stream.read()).thenThrow(failure);
        when(sessionClient.openStream(eq(PATH), eq(0L), any(), any(), any())).thenReturn(stream);
        TextTableFunction.Handle handle = handle("\n", 64, 32, StandardCharsets.UTF_8, null, null);
        ConnectorPageSource source = function.createPageSource(
                mock(ConnectorSession.class), handle, FileSplit.forWholeFile(handle.selection().objects().getFirst()), allColumns(handle));

        assertEquals(failure, assertThrows(UncheckedIOException.class, source::getNextSourcePage).getCause());
        assertTrue(source.isFinished());
        source.close();
        verify(stream, times(1)).close();
        verify(sessionClient, times(1)).close();
    }

    @Test
    void closingPartialScanReleasesStreamAndSession() throws IOException {
        ByteArrayInputStream stream = spy(stream("first\nsecond\n", StandardCharsets.UTF_8));
        when(sessionClient.openStream(eq(PATH), eq(0L), any(), any(), any())).thenReturn(stream);
        TextTableFunction.Handle handle = new TextTableFunction.Handle(
                new FileSelection(List.of(new S3ObjectRef(PATH, 13, null, null, Compression.NONE)), ""),
                new ScanSettings(32, new PageSettings(1024, 1), "UTF-8"),
                new TextTableFunction.TextOptions("\n"));
        ConnectorPageSource source = function.createPageSource(
                mock(ConnectorSession.class), handle, FileSplit.forWholeFile(handle.selection().objects().getFirst()), allColumns(handle));

        assertEquals(1, nextPage(source).getPositionCount());
        assertFalse(source.isFinished());
        source.close();
        source.close();

        assertTrue(source.isFinished());
        assertEquals(null, source.getNextSourcePage());
        verify(stream, times(1)).close();
        verify(sessionClient, times(1)).close();
    }

    private static List<S3FileColumnHandle> allColumns(TextTableFunction.Handle handle) {
        return List.of(new S3FileColumnHandle("line", 0));
    }

    private static SourcePage nextPage(ConnectorPageSource pageSource) {
        SourcePage page = pageSource.getNextSourcePage();
        assertInstanceOf(SourcePage.class, page);
        return page;
    }

    private static TextTableFunction.Handle assertHandle(TableFunctionAnalysis analysis,
                                                         String expectedDelimiter,
                                                         long expectedSize,
                                                         Optional<String> expectedEtag,
                                                         Optional<String> expectedVersion) {
        assertInstanceOf(TextTableFunction.Handle.class, analysis.getHandle());
        TextTableFunction.Handle handle = (TextTableFunction.Handle) analysis.getHandle();
        assertEquals(PATH, handle.selection().objects().getFirst().path());
        assertEquals(expectedDelimiter, handle.options().lineBreak());
        assertEquals(expectedSize, handle.selection().objects().getFirst().size());
        assertEquals(DEFAULT_SPLIT_SIZE_BYTES, handle.scan().splitSizeBytes());
        assertEquals(1024, handle.scan().page().batchSize());
        assertEquals(StandardCharsets.UTF_8.name(), handle.scan().charsetName());
        assertEquals(expectedEtag, handle.selection().objects().getFirst().eTagRef());
        assertEquals(expectedVersion, handle.selection().objects().getFirst().versionIdRef());
        List<Type> columnTypes = handle.resolveColumnTypes();
        assertEquals(1, columnTypes.size());
        assertEquals(VarcharType.createUnboundedVarcharType(), columnTypes.get(0));
        return handle;
    }

    private static TextTableFunction.Handle handle(
            String lineBreak,
            long fileSize,
            int splitSizeBytes,
            Charset charset,
            String eTag,
            String versionId) {
        return new TextTableFunction.Handle(
                new FileSelection(List.of(new S3ObjectRef(PATH, fileSize, eTag, versionId, Compression.NONE)), ""),
                new ScanSettings(splitSizeBytes, new PageSettings(PageSettings.DEFAULT.batchSize(), 8L * 1024L * 1024L), charset.name()),
                new TextTableFunction.TextOptions(lineBreak));
    }

    private static ByteArrayInputStream stream(String value, Charset charset) {
        return new ByteArrayInputStream(value.getBytes(charset));
    }
}

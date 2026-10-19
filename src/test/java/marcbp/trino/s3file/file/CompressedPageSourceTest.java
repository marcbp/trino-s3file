package marcbp.trino.s3file.file;

import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import marcbp.trino.s3file.csv.CsvTableFunction;
import marcbp.trino.s3file.json.JsonFormatSupport;
import marcbp.trino.s3file.json.JsonTableFunction;
import marcbp.trino.s3file.s3.S3ClientBuilder;
import marcbp.trino.s3file.txt.TextTableFunction;
import marcbp.trino.s3file.xml.XmlFormatSupport;
import marcbp.trino.s3file.xml.XmlTableFunction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CompressedPageSourceTest {
    static Stream<Arguments> codecsAndFormats() {
        return Arrays.stream(Compression.values()).filter(codec -> codec != Compression.NONE)
                .flatMap(codec -> Stream.of("txt", "json", "csv", "xml").map(format -> Arguments.of(codec, format)));
    }

    @ParameterizedTest
    @MethodSource("codecsAndFormats")
    void pagesStreamBeforeCompressedEofAndUseDecodedSizeLimits(Compression codec, String format) throws Exception {
        byte[] compressed = CompressedTestData.compressed(codec, format);
        var object = new S3ObjectRef("s3://bucket/file", compressed.length, "etag", null, codec);
        var selection = new FileSelection(List.of(object), "$path");
        var scan = new ScanSettings(1, new PageSettings(10_000, 1024), "UTF-8");
        var session = mock(ConnectorSession.class);
        for (boolean cancel : new boolean[] {true, false}) {
            var client = mock(S3ClientBuilder.SessionClient.class);
            var builder = mock(S3ClientBuilder.class);
            when(builder.forSession(session)).thenReturn(client);
            var raw = spy(new ByteArrayInputStream(compressed));
            when(client.openStream(object.path(), 0, null, Optional.empty(), Optional.of("etag"))).thenReturn(raw);
            BaseTextFileHandle handle = switch (format) {
                case "txt" -> new TextTableFunction.Handle(selection, scan, new TextTableFunction.TextOptions("\n"));
                case "csv" -> new CsvTableFunction.Handle(selection, scan, new CsvTableFunction.CsvSchema(List.of("value")),
                        new CsvTableFunction.CsvOptions(';', true, false));
                case "json" -> new JsonTableFunction.Handle(selection, scan,
                        new JsonTableFunction.JsonSchema(List.of("value"), List.of(JsonFormatSupport.ColumnType.VARCHAR)));
                case "xml" -> new XmlTableFunction.Handle(selection, scan, new XmlFormatSupport.Schema(List.of(
                        new XmlFormatSupport.Column("value", XmlFormatSupport.ColumnSource.ELEMENT, "value"))),
                        new XmlTableFunction.XmlOptions("row", false, ""));
                default -> throw new AssertionError(format);
            };
            var splits = handle.createSplits(true);
            assertEquals(1, splits.size(), "Compression must prevent byte-range splitting");
            ConnectorPageSource source = switch (handle) {
                case TextTableFunction.Handle h -> new TextTableFunction(builder).createPageSource(session, h, splits.getFirst(), List.of());
                case CsvTableFunction.Handle h -> new CsvTableFunction(builder).createPageSource(session, h, splits.getFirst(), List.of());
                case JsonTableFunction.Handle h -> new JsonTableFunction(builder).createPageSource(session, h, splits.getFirst(), List.of());
                case XmlTableFunction.Handle h -> new XmlTableFunction(builder).createPageSource(session, h, splits.getFirst(), List.of());
                default -> throw new AssertionError(format);
            };
            long initialMemory = source.getMemoryUsage();
            var page = source.getNextSourcePage();
            assertNotNull(page);
            assertTrue(page.getPositionCount() > 0 && page.getPositionCount() < 10_000,
                    "The decoded-byte target must flush a page before the row limit");
            assertTrue(raw.available() > 0, "A first page must not require downloading the whole compressed file");
            assertTrue(source.getCompletedBytes() > 0 && source.getCompletedBytes() < compressed.length);
            assertTrue(source.getMemoryUsage() > initialMemory);
            if (cancel) {
                source.close();
                assertTrue(raw.available() > 0, "Cancellation must not drain the source");
            }
            else {
                long rows = page.getPositionCount();
                while ((page = source.getNextSourcePage()) != null) {
                    rows += page.getPositionCount();
                }
                assertEquals(CompressedTestData.ROWS, rows);
                assertEquals(compressed.length, source.getCompletedBytes());
                assertEquals(rows, source.getCompletedPositions().orElseThrow());
            }
            assertTrue(source.isFinished());
            assertTrue(source.getMemoryUsage() <= initialMemory);
            verify(raw, times(1)).close();
            verify(client).close();
            verify(client, never()).readBytes(any(), anyLong(), any(), any(), any());
        }
    }
}

package marcbp.trino.s3file.xml;

import io.airlift.slice.Slices;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.s3.S3ClientBuilder;
import marcbp.trino.s3file.s3.S3ClientBuilder.ObjectMetadata;
import marcbp.trino.s3file.xml.XmlFormatSupport.Column;
import marcbp.trino.s3file.xml.XmlFormatSupport.ColumnSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class XmlTableFunctionTest {
    private static final String PATH = "s3://bucket/books.xml";

    private final S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
    private final S3ClientBuilder.SessionClient sessionClient = mock(S3ClientBuilder.SessionClient.class);
    private final XmlTableFunction function = new XmlTableFunction(s3ClientBuilder);

    @BeforeEach
    void setUp() {
        reset(s3ClientBuilder, sessionClient);
        when(s3ClientBuilder.forSession(any(ConnectorSession.class))).thenReturn(sessionClient);
        doNothing().when(sessionClient).close();
    }

    @Test
    void analyzeInfersAttributesAndElements() {
        when(sessionClient.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        <catalog>
                          <book id="bk101">
                            <author>Gambardella, Matthew</author>
                            <title>XML Developer's Guide</title>
                            <genre>Computer</genre>
                          </book>
                          <book id="bk102">
                            <author>Ralls, Kim</author>
                            <title>Midnight Rain</title>
                            <genre>Fantasy</genre>
                          </book>
                        </catalog>
                        """)));
        when(sessionClient.getObjectMetadata(eq(PATH))).thenReturn(new ObjectMetadata(512L, Optional.of("etag-xml"), Optional.empty()));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "ROW_ELEMENT", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("book"))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        Descriptor descriptor = analysis.getReturnedType().orElseThrow();
        Descriptor expectedDescriptor = Descriptor.descriptor(
                List.of("@id", "author", "title", "genre"),
                List.of(
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType()));
        assertEquals(expectedDescriptor, descriptor);

        XmlTableFunction.Handle handle = (XmlTableFunction.Handle) analysis.getHandle();
        assertEquals(PATH, handle.getS3Path());
        assertEquals("book", handle.getRowElement());
        assertEquals(List.of("@id", "author", "title", "genre"), handle.columnNames());
        assertEquals(StandardCharsets.UTF_8.name(), handle.getCharsetName());
        assertEquals(512L, handle.getFileSize());
        assertEquals(Optional.of("etag-xml"), handle.getETag());
        assertEquals(Optional.empty(), handle.getVersionId());
        assertFalse(handle.isEmptyAsNull());
        assertFalse(handle.hasInvalidRowColumn());

        List<FileSplit> splits = function.createSplits(handle);
        assertEquals(1, splits.size());
        assertTrue(splits.get(0).isWholeFile());

        verify(sessionClient).openReader(eq(PATH), any(Charset.class));
        verify(sessionClient).getObjectMetadata(eq(PATH));
        verify(sessionClient).close();
    }

    @Test
    void analyzeKeepsTextColumnWhenRequested() {
        when(sessionClient.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        <feed>
                          <entry status="new">Note<message>Hello</message></entry>
                          <entry status="done"><message>Bye</message></entry>
                        </feed>
                        """)));
        when(sessionClient.getObjectMetadata(eq(PATH))).thenReturn(new ObjectMetadata(1024L, Optional.empty(), Optional.of("version-text")));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "ROW_ELEMENT", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("entry")),
                "INCLUDE_TEXT", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("true")),
                "ENCODING", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("ISO-8859-1"))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        Descriptor descriptor = analysis.getReturnedType().orElseThrow();
        Descriptor expectedDescriptor = Descriptor.descriptor(
                List.of("@status", "message", "text"),
                List.of(
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType()));
        assertEquals(expectedDescriptor, descriptor);

        XmlTableFunction.Handle handle = (XmlTableFunction.Handle) analysis.getHandle();
        assertEquals("entry", handle.getRowElement());
        assertEquals("ISO-8859-1", handle.getCharsetName());
        assertEquals(List.of("@status", "message", "text"), handle.columnNames());
        assertEquals(1024L, handle.getFileSize());
        assertEquals(Optional.empty(), handle.getETag());
        assertEquals(Optional.of("version-text"), handle.getVersionId());
        assertFalse(handle.isEmptyAsNull());
        assertFalse(handle.hasInvalidRowColumn());

        verify(sessionClient).openReader(eq(PATH), any(Charset.class));
        verify(sessionClient).getObjectMetadata(eq(PATH));
        verify(sessionClient).close();
    }

    @Test
    void analyzeHonoursEmptyAsNullFlag() {
        when(sessionClient.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        <items>
                          <item code="">
                            <name></name>
                          </item>
                        </items>
                        """)));
        when(sessionClient.getObjectMetadata(eq(PATH))).thenReturn(new ObjectMetadata(128L, Optional.empty(), Optional.empty()));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "ROW_ELEMENT", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("item")),
                "EMPTY_AS_NULL", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("true"))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        XmlTableFunction.Handle handle = (XmlTableFunction.Handle) analysis.getHandle();
        assertTrue(handle.isEmptyAsNull());
        assertEquals(List.of("@code", "name"), handle.columnNames());
        assertFalse(handle.hasInvalidRowColumn());

        verify(sessionClient).openReader(eq(PATH), any(Charset.class));
        verify(sessionClient).getObjectMetadata(eq(PATH));
        verify(sessionClient).close();
    }

    @Test
    void analyzeAddsInvalidRowColumnWhenRequested() {
        when(sessionClient.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        <items>
                          <item code="1"><name>Valid</name></item>
                        </items>
                        """)));
        when(sessionClient.getObjectMetadata(eq(PATH))).thenReturn(new ObjectMetadata(256L, Optional.empty(), Optional.empty()));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "ROW_ELEMENT", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("item")),
                "INVALID_ROW_COLUMN", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("raw_row"))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        Descriptor descriptor = analysis.getReturnedType().orElseThrow();
        Descriptor expected = Descriptor.descriptor(
                List.of("@code", "name", "raw_row"),
                List.of(
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType()));
        assertEquals(expected, descriptor);

        XmlTableFunction.Handle handle = (XmlTableFunction.Handle) analysis.getHandle();
        assertTrue(handle.hasInvalidRowColumn());
        assertEquals("raw_row", handle.getInvalidRowColumn());
        assertEquals(List.of("@code", "name", "raw_row"), handle.columnNames());
    }

    @Test
    void readNextRecordConvertsEmptyStringsWhenRequested() throws Exception {
        String xml = """
                <items>
                  <item code="">
                    <name></name>
                    <note> </note>
                  </item>
                </items>
                """;

        XmlFormatSupport.Schema schema = new XmlFormatSupport.Schema(List.of(
                new Column("@code", ColumnSource.ATTRIBUTE, "code"),
                new Column("name", ColumnSource.ELEMENT, "name"),
                new Column("text", ColumnSource.TEXT, "")));

        XmlFormatSupport.RowExtraction withoutNull = XmlFormatSupport.readNextRecord(
                XmlFormatSupport.newXmlReader(new BufferedReader(new StringReader(xml))),
                schema,
                "item",
                false);
        assertFalse(withoutNull.done());
        assertTrue(withoutNull.valid());
        assertArrayEquals(new String[] {"", "", null}, withoutNull.values());

        XmlFormatSupport.RowExtraction withNull = XmlFormatSupport.readNextRecord(
                XmlFormatSupport.newXmlReader(new BufferedReader(new StringReader(xml))),
                schema,
                "item",
                true);
        assertFalse(withNull.done());
        assertTrue(withNull.valid());
        assertArrayEquals(new String[] {null, null, null}, withNull.values());

        XmlFormatSupport.RowExtraction finished = XmlFormatSupport.readNextRecord(
                XmlFormatSupport.newXmlReader(new BufferedReader(new StringReader("<items></items>"))),
                schema,
                "item",
                true);
        assertTrue(finished.done());
    }

    @Test
    void readNextRecordCapturesInvalidRowWhenRawColumnProvided() throws Exception {
        String xml = """
                <items>
                  <item code="1">
                    <name><first>Nested</first></name>
                  </item>
                </items>
                """;

        XmlFormatSupport.Schema baseSchema = new XmlFormatSupport.Schema(List.of(
                new Column("@code", ColumnSource.ATTRIBUTE, "code"),
                new Column("name", ColumnSource.ELEMENT, "name")));
        XmlFormatSupport.Schema schema = XmlFormatSupport.appendRawColumn(baseSchema, "raw_row");

        XmlFormatSupport.RowExtraction extraction = XmlFormatSupport.readNextRecord(
                XmlFormatSupport.newXmlReader(new BufferedReader(new StringReader(xml))),
                schema,
                "item",
                false);

        assertFalse(extraction.done());
        assertFalse(extraction.valid());
        String[] values = extraction.values();
        assertNull(values[0]);
        assertNull(values[1]);
        String raw = values[2];
        assertTrue(raw.contains("<item"));
        assertTrue(raw.contains("<first>Nested</first>"));
    }
}

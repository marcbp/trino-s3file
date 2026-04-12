package marcbp.trino.s3file.txt;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class TextFormatSupportTest {
    @Test
    void decodeEscapesHandlesCommonSequences() {
        assertEquals("a\nb\tc\\", TextFormatSupport.decodeEscapes("a\\nb\\tc\\\\"));
    }

    @Test
    void formatForLogReplacesControlCharacters() {
        assertEquals("\\r\\n", TextFormatSupport.formatForLog("\r\n"));
    }

    @Test
    void readNextLineProducesRecordWithSplitBoundary() throws Exception {
        BufferedReader reader = new BufferedReader(new StringReader("first\nsecond\n"));
        Optional<TextFormatSupport.TextRecord> record = TextFormatSupport.readNextLine(
                reader,
                StandardCharsets.UTF_8,
                "\n",
                "\n".getBytes(StandardCharsets.UTF_8),
                5,
                0,
                false);

        assertTrue(record.isPresent());
        TextFormatSupport.TextRecord textRecord = record.orElseThrow();
        assertEquals("first", textRecord.value());
        assertTrue(textRecord.finishesSplit());
        assertEquals(6, textRecord.bytes());
    }

    @Test
    void readNextLineUsesConfiguredSeparator() throws Exception {
        BufferedReader reader = new BufferedReader(new StringReader("alpha;bravo;charlie"));

        Optional<TextFormatSupport.TextRecord> first = TextFormatSupport.readNextLine(
                reader,
                StandardCharsets.UTF_8,
                ";",
                ";".getBytes(StandardCharsets.UTF_8),
                1024,
                0,
                true);
        Optional<TextFormatSupport.TextRecord> second = TextFormatSupport.readNextLine(
                reader,
                StandardCharsets.UTF_8,
                ";",
                ";".getBytes(StandardCharsets.UTF_8),
                1024,
                first.orElseThrow().bytes(),
                true);
        Optional<TextFormatSupport.TextRecord> third = TextFormatSupport.readNextLine(
                reader,
                StandardCharsets.UTF_8,
                ";",
                ";".getBytes(StandardCharsets.UTF_8),
                1024,
                first.orElseThrow().bytes() + second.orElseThrow().bytes(),
                true);

        assertEquals("alpha", first.orElseThrow().value());
        assertEquals(6, first.orElseThrow().bytes());
        assertEquals("bravo", second.orElseThrow().value());
        assertEquals(6, second.orElseThrow().bytes());
        assertEquals("charlie", third.orElseThrow().value());
        assertEquals(7, third.orElseThrow().bytes());
    }

    @Test
    void calculateLineBytesHonoursCharsetEncoding() {
        byte[] utf8LineBreak = "\n".getBytes(StandardCharsets.UTF_8);
        byte[] latin1LineBreak = "\n".getBytes(StandardCharsets.ISO_8859_1);

        assertEquals(3, TextFormatSupport.calculateLineBytes("é", StandardCharsets.UTF_8, utf8LineBreak));
        assertEquals(2, TextFormatSupport.calculateLineBytes("é", StandardCharsets.ISO_8859_1, latin1LineBreak));
    }

    @Test
    void writeLineAppendsUtf8Slice() {
        PageBuilderStatus pageBuilderStatus = new PageBuilderStatus(PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        BlockBuilderStatus blockBuilderStatus = pageBuilderStatus.createBlockBuilderStatus();
        BlockBuilder builder = VarcharType.createUnboundedVarcharType().createBlockBuilder(blockBuilderStatus, 1);
        TextFormatSupport.writeLine(builder, VarcharType.createUnboundedVarcharType(), "hello");
        assertEquals(1, builder.getPositionCount());
    }
}

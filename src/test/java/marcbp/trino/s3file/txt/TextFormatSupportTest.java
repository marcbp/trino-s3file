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
    void writeLineAppendsUtf8Slice() {
        PageBuilderStatus pageBuilderStatus = new PageBuilderStatus();
        BlockBuilderStatus blockBuilderStatus = pageBuilderStatus.createBlockBuilderStatus();
        BlockBuilder builder = VarcharType.createUnboundedVarcharType().createBlockBuilder(blockBuilderStatus, 1);
        TextFormatSupport.writeLine(builder, VarcharType.createUnboundedVarcharType(), "hello");
        assertEquals(1, builder.getPositionCount());
    }
}

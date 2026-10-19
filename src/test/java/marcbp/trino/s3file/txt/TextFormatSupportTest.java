package marcbp.trino.s3file.txt;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TextFormatSupportTest {
    @Test
    void decodeEscapesHandlesCommonSequences() {
        assertEquals("a\nb\tc\\", TextFormatSupport.decodeEscapes("a\\nb\\tc\\\\"));
    }

}

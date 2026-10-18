package marcbp.trino.s3file.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class S3ClientConfigTest {
    @Test
    void parsesPageSettings() {
        S3ClientConfig config = S3ClientConfig.from(Map.of(
                "s3.page-batch-size", "2048",
                "s3.page-target-size-mb", "16"));

        assertEquals(2048, config.pageSettings().batchSize());
        assertEquals(16L * 1024L * 1024L, config.pageSettings().targetSizeBytes());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "s3.page-batch-size", "s3.page-target-size-mb", "s3.default-split-size-mb",
            "s3.max-connections-per-worker", "s3.connection-acquisition-timeout-s"
    })
    void rejectsInvalidNumericSettings(String property) {
        for (String value : new String[] {"0", "-1", "invalid", "9223372036854775808"}) {
            assertThrows(IllegalArgumentException.class, () -> S3ClientConfig.from(Map.of(property, value)),
                    property + "=" + value);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"s3.default-split-size-mb", "s3.page-target-size-mb"})
    void rejectsOverflowWhenConvertingMegabytes(String property) {
        assertThrows(IllegalArgumentException.class, () -> S3ClientConfig.from(
                Map.of(property, "9223372036854775807")));
    }
}

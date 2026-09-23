package marcbp.trino.s3file.s3;

import org.junit.jupiter.api.Test;

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

    @Test
    void rejectsInvalidPageSettings() {
        assertThrows(IllegalArgumentException.class, () -> S3ClientConfig.from(Map.of("s3.page-batch-size", "0")));
        assertThrows(IllegalArgumentException.class, () -> S3ClientConfig.from(Map.of("s3.page-target-size-mb", "0")));
    }
}

package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Settings that control the size of pages emitted by connector page sources. */
public record PageSettings(
        @JsonProperty("batchSize") int batchSize,
        @JsonProperty("targetSizeBytes") long targetSizeBytes) {
    public static final PageSettings DEFAULT = new PageSettings(1024, 8L * 1024 * 1024);

    @JsonCreator
    public PageSettings {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (targetSizeBytes <= 0) {
            throw new IllegalArgumentException("targetSizeBytes must be positive");
        }
    }
}

package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Settings that control the size of pages emitted by connector page sources. */
public record PageSettings(
        @JsonProperty("batchSize") int batchSize,
        @JsonProperty("targetSizeBytes") long targetSizeBytes) {
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

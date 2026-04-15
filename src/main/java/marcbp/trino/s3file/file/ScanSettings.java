package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public record ScanSettings(
        @JsonProperty("splitSizeBytes") int splitSizeBytes,
        @JsonProperty("batchSize") int batchSize,
        @JsonProperty("charset") String charsetName) {
    @JsonCreator
    public ScanSettings {
        charsetName = requireNonNull(charsetName, "charsetName is null");
    }
}

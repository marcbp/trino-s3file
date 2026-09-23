package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public record ScanSettings(
        @JsonProperty("splitSizeBytes") int splitSizeBytes,
        @JsonProperty("page") PageSettings page,
        @JsonProperty("charset") String charsetName) {
    @JsonCreator
    public ScanSettings {
        page = requireNonNull(page, "page is null");
        charsetName = requireNonNull(charsetName, "charsetName is null");
    }
}

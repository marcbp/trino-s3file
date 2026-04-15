package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record AnalysisStats(
        @JsonProperty("rowsSampled") long rowsSampled,
        @JsonProperty("columnsDetected") int columnsDetected,
        @JsonProperty("timeNanos") long timeNanos) {
    public static final AnalysisStats EMPTY = new AnalysisStats(0, 0, 0);

    @JsonCreator
    public AnalysisStats {}
}

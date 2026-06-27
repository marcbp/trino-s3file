package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;

import static java.util.Objects.requireNonNull;

/**
 * Generic file-based split shared by the S3 table functions.
 */
public record FileSplit(
        @JsonProperty("id") String id,
        @JsonProperty("startOffset") long startOffset,
        @JsonProperty("primaryEndOffset") long primaryEndOffset,
        @JsonProperty("rangeEndExclusive") long rangeEndExclusive,
        @JsonProperty("first") boolean first,
        @JsonProperty("last") boolean last)
        implements ConnectorSplit {
    @JsonCreator
    public FileSplit {
        id = requireNonNull(id, "id is null");
    }

    public static FileSplit forWholeFile(long size) {
        return new FileSplit("split-0", 0, size, size, true, true);
    }

    @JsonProperty
    public String getId() {
        return id;
    }

    @JsonProperty("startOffset")
    public long getStartOffset() {
        return startOffset;
    }

    @JsonProperty("primaryEndOffset")
    public long getPrimaryEndOffset() {
        return primaryEndOffset;
    }

    @JsonProperty("rangeEndExclusive")
    public long getRangeEndExclusive() {
        return rangeEndExclusive;
    }

    @JsonProperty("first")
    public boolean isFirst() {
        return first;
    }

    @JsonProperty("last")
    public boolean isLast() {
        return last;
    }

    public long getPrimaryLength() {
        return Math.max(0, primaryEndOffset - startOffset);
    }

    public boolean isWholeFile() {
        return first && last && startOffset == 0 && rangeEndExclusive == primaryEndOffset;
    }

    @Override
    public long getRetainedSizeInBytes() {
        return 64L + (long) id.length() * Character.BYTES;
    }

    @Override
    public String toString() {
        return "FileSplit{" +
                "id='" + id + '\'' +
                ", startOffset=" + startOffset +
                ", primaryEndOffset=" + primaryEndOffset +
                ", rangeEndExclusive=" + rangeEndExclusive +
                ", first=" + first +
                ", last=" + last +
                '}';
    }
}

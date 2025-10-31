package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Objects;

/**
 * Generic file-based split shared by the S3 table functions.
 */
public final class FileSplit implements ConnectorSplit {
    private final String id;
    private final long startOffset;
    private final long primaryEndOffset;
    private final long rangeEndExclusive;
    private final boolean first;
    private final boolean last;

    @JsonCreator
    public FileSplit(@JsonProperty("id") String id,
                     @JsonProperty("startOffset") long startOffset,
                     @JsonProperty("primaryEndOffset") long primaryEndOffset,
                     @JsonProperty("rangeEndExclusive") long rangeEndExclusive,
                     @JsonProperty("first") boolean first,
                     @JsonProperty("last") boolean last) {
        this.id = Objects.requireNonNull(id, "id is null");
        this.startOffset = startOffset;
        this.primaryEndOffset = primaryEndOffset;
        this.rangeEndExclusive = rangeEndExclusive;
        this.first = first;
        this.last = last;
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

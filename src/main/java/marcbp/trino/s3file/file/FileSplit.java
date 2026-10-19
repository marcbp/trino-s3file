package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * Generic file-based split shared by the S3 table functions.
 */
@JsonAutoDetect(getterVisibility = NONE, isGetterVisibility = NONE)
public record FileSplit(
        @JsonProperty("object") S3ObjectRef object,
        @JsonProperty("id") String id,
        @JsonProperty("startOffset") long startOffset,
        @JsonProperty("primaryEndOffset") long primaryEndOffset,
        @JsonProperty("rangeEndExclusive") long rangeEndExclusive,
        @JsonProperty("first") boolean first,
        @JsonProperty("last") boolean last)
        implements ConnectorSplit {
    @JsonCreator
    public FileSplit {
        object = requireNonNull(object, "object is null");
        id = requireNonNull(id, "id is null");
    }

    public static FileSplit forWholeFile(S3ObjectRef object) {
        return new FileSplit(object, "split-0", 0, object.size(), object.size(), true, true);
    }

    public long primaryLength() {
        return Math.max(0, primaryEndOffset - startOffset);
    }

    public boolean isWholeFile() {
        return first && last && startOffset == 0 && rangeEndExclusive == primaryEndOffset;
    }

    @Override
    public long getRetainedSizeInBytes() {
        // Shallow split/object storage only; strings and the shared compression enum are excluded.
        return instanceSize(FileSplit.class) + instanceSize(S3ObjectRef.class);
    }

}

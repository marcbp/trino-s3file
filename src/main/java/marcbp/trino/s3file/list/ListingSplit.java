package marcbp.trino.s3file.list;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;

/**
 * Single split used by S3 listing table functions.
 */
public final class ListingSplit implements ConnectorSplit {
    public static final ListingSplit INSTANCE = new ListingSplit();

    private final boolean singleton;

    @JsonCreator
    public ListingSplit(@JsonProperty("singleton") boolean singleton) {
        this.singleton = singleton;
    }

    public ListingSplit() {
        this(true);
    }

    @JsonProperty
    public boolean singleton() {
        return singleton;
    }

    @Override
    public long getRetainedSizeInBytes() {
        return 16L;
    }
}

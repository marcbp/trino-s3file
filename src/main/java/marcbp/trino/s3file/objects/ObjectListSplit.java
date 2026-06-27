package marcbp.trino.s3file.objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;

/**
 * Single split used by the S3 object listing table function.
 */
public final class ObjectListSplit implements ConnectorSplit {
    public static final ObjectListSplit INSTANCE = new ObjectListSplit();

    private final boolean singleton;

    @JsonCreator
    public ObjectListSplit(@JsonProperty("singleton") boolean singleton) {
        this.singleton = singleton;
    }

    public ObjectListSplit() {
        this(true);
    }

    @JsonProperty
    public boolean singleton() {
        return singleton;
    }
}

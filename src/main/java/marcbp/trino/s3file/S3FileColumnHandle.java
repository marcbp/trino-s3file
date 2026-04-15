package marcbp.trino.s3file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;

import static java.util.Objects.requireNonNull;

public final class S3FileColumnHandle implements ColumnHandle {
    private final String name;
    private final int ordinalPosition;

    @JsonCreator
    public S3FileColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("ordinalPosition") int ordinalPosition) {
        this.name = requireNonNull(name, "name is null");
        this.ordinalPosition = ordinalPosition;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public int getOrdinalPosition() {
        return ordinalPosition;
    }
}

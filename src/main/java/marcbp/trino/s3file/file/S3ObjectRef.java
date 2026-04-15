package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record S3ObjectRef(String path, long size, String eTag, String versionId) {
    @JsonCreator
    public S3ObjectRef(
            @JsonProperty("path") String path,
            @JsonProperty("size") long size,
            @JsonProperty("etag") String eTag,
            @JsonProperty("versionId") String versionId) {
        this.path = requireNonNull(path, "path is null");
        this.size = size;
        this.eTag = eTag;
        this.versionId = versionId;
    }

    @JsonIgnore
    public Optional<String> eTagRef() {
        return Optional.ofNullable(eTag);
    }

    @JsonIgnore
    public Optional<String> versionIdRef() {
        return Optional.ofNullable(versionId);
    }
}

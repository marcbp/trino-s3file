package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record S3ObjectRef(String path, long size, String eTag, String versionId, Compression compression) {
    public static S3ObjectRef from(String path, S3ClientBuilder.ObjectMetadata metadata, Compression compression) {
        return new S3ObjectRef(path, metadata.size(), metadata.eTag().orElse(null), metadata.versionId().orElse(null), compression);
    }

    @JsonCreator
    public S3ObjectRef(
            @JsonProperty("path") String path,
            @JsonProperty("size") long size,
            @JsonProperty("etag") String eTag,
            @JsonProperty("versionId") String versionId,
            @JsonProperty("compression") Compression compression) {
        this.path = requireNonNull(path, "path is null");
        this.size = size;
        this.eTag = eTag;
        this.versionId = versionId;
        this.compression = requireNonNull(compression, "compression is null");
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

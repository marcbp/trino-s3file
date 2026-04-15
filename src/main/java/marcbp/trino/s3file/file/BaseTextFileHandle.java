package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.type.Type;
import marcbp.trino.s3file.util.CharsetUtils;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Common connector table function handle metadata shared by text-backed functions.
 */
public abstract class BaseTextFileHandle implements ConnectorTableFunctionHandle, ConnectorTableHandle {
    public static final int DEFAULT_BATCH_SIZE = 1024;

    private final String s3Path;
    private final String charsetName;
    private final long fileSize;
    private final int splitSizeBytes;
    private final int batchSize;
    private final Optional<String> eTag;
    private final Optional<String> versionId;
    private final long analysisRowsSampled;
    private final int analysisColumnsDetected;
    private final long analysisTimeNanos;

    protected BaseTextFileHandle(String s3Path, long fileSize, int splitSizeBytes, String charsetName, int batchSize) {
        this(s3Path, fileSize, splitSizeBytes, charsetName, batchSize, Optional.empty(), Optional.empty(), 0, 0, 0);
    }

    protected BaseTextFileHandle(
            String s3Path,
            long fileSize,
            int splitSizeBytes,
            String charsetName,
            int batchSize,
            Optional<String> eTag,
            Optional<String> versionId) {
        this(s3Path, fileSize, splitSizeBytes, charsetName, batchSize, eTag, versionId, 0, 0, 0);
    }

    protected BaseTextFileHandle(
            String s3Path,
            long fileSize,
            int splitSizeBytes,
            String charsetName,
            int batchSize,
            Optional<String> eTag,
            Optional<String> versionId,
            long analysisRowsSampled,
            int analysisColumnsDetected,
            long analysisTimeNanos) {
        this.s3Path = requireNonNull(s3Path, "s3Path is null");
        this.charsetName = requireNonNull(charsetName, "charsetName is null");
        this.fileSize = fileSize;
        this.splitSizeBytes = splitSizeBytes;
        this.batchSize = batchSize;
        this.eTag = eTag;
        this.versionId = versionId;
        this.analysisRowsSampled = analysisRowsSampled;
        this.analysisColumnsDetected = analysisColumnsDetected;
        this.analysisTimeNanos = analysisTimeNanos;
    }

    @JsonProperty("s3Path")
    public String getS3Path() {
        return s3Path;
    }

    @JsonProperty("fileSize")
    public long getFileSize() {
        return fileSize;
    }

    @JsonProperty("splitSizeBytes")
    public int getSplitSizeBytes() {
        return splitSizeBytes;
    }

    @JsonProperty("charset")
    public String getCharsetName() {
        return charsetName;
    }

    @JsonProperty("batchSize")
    public int getBatchSize() {
        return batchSize;
    }

    @JsonIgnore
    public Optional<String> getETag() {
        return eTag;
    }

    @JsonIgnore
    public Optional<String> getVersionId() {
        return versionId;
    }

    @JsonProperty("etag")
    public String getETagValue() {
        return eTag.orElse(null);
    }

    @JsonProperty("versionId")
    public String getVersionIdValue() {
        return versionId.orElse(null);
    }

    @JsonProperty("analysisRowsSampled")
    public long getAnalysisRowsSampled() {
        return analysisRowsSampled;
    }

    @JsonProperty("analysisColumnsDetected")
    public int getAnalysisColumnsDetected() {
        return analysisColumnsDetected;
    }

    @JsonProperty("analysisTimeNanos")
    public long getAnalysisTimeNanos() {
        return analysisTimeNanos;
    }

    @JsonIgnore
    public Charset charset() {
        return CharsetUtils.resolve(charsetName);
    }

    @JsonIgnore
    public FileSplit toWholeFileSplit() {
        return FileSplit.forWholeFile(fileSize);
    }

    @JsonIgnore
    public abstract String format();

    @JsonIgnore
    public abstract List<String> columnNames();

    @JsonIgnore
    public abstract List<Type> resolveColumnTypes();
}

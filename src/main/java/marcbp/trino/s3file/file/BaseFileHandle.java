package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import marcbp.trino.s3file.util.CharsetUtils;

import java.nio.charset.Charset;

import static java.util.Objects.requireNonNull;

/**
 * Common connector table function handle metadata shared by file-backed functions.
 */
public abstract class BaseFileHandle implements ConnectorTableFunctionHandle {
    public static final int DEFAULT_BATCH_SIZE = 1024;

    private final String s3Path;
    private final String charsetName;
    private final long fileSize;
    private final int splitSizeBytes;
    private final int batchSize;

    protected BaseFileHandle(String s3Path, long fileSize, int splitSizeBytes, String charsetName, int batchSize) {
        this.s3Path = requireNonNull(s3Path, "s3Path is null");
        this.charsetName = requireNonNull(charsetName, "charsetName is null");
        this.fileSize = fileSize;
        this.splitSizeBytes = splitSizeBytes;
        this.batchSize = batchSize;
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
    public Charset charset() {
        return CharsetUtils.resolve(charsetName);
    }

    @JsonIgnore
    public FileSplit toWholeFileSplit() {
        return FileSplit.forWholeFile(fileSize);
    }
}

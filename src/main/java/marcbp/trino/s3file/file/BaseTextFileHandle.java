package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.type.Type;
import marcbp.trino.s3file.util.CharsetUtils;

import java.nio.charset.Charset;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Common connector table function handle metadata shared by text-backed functions.
 */
public abstract class BaseTextFileHandle implements ConnectorTableFunctionHandle, ConnectorTableHandle {
    public static final int DEFAULT_BATCH_SIZE = 1024;

    private final S3ObjectRef object;
    private final ScanSettings scan;
    private final AnalysisStats analysis;

    protected BaseTextFileHandle(S3ObjectRef object, ScanSettings scan) {
        this(object, scan, AnalysisStats.EMPTY);
    }

    protected BaseTextFileHandle(S3ObjectRef object, ScanSettings scan, AnalysisStats analysis) {
        this.object = requireNonNull(object, "object is null");
        this.scan = requireNonNull(scan, "scan is null");
        this.analysis = requireNonNull(analysis, "analysis is null");
    }

    @JsonProperty("object")
    public S3ObjectRef object() {
        return object;
    }

    @JsonProperty("scan")
    public ScanSettings scan() {
        return scan;
    }

    @JsonProperty("analysis")
    public AnalysisStats analysis() {
        return analysis;
    }

    @JsonIgnore
    public Charset charset() {
        return CharsetUtils.resolve(scan.charsetName());
    }

    @JsonIgnore
    public FileSplit toWholeFileSplit() {
        return FileSplit.forWholeFile(object.size());
    }

    @JsonIgnore
    public abstract String format();

    @JsonIgnore
    public abstract List<String> columnNames();

    @JsonIgnore
    public abstract List<Type> resolveColumnTypes();
}

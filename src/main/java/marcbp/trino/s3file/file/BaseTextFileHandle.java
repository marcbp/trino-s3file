package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.util.CharsetUtils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

/**
 * Common connector table function handle metadata shared by text-backed functions.
 */
public abstract class BaseTextFileHandle implements RuntimeTableHandle {
    public static final int DEFAULT_BATCH_SIZE = 1024;

    private final FileSelection selection;
    private final ScanSettings scan;

    protected BaseTextFileHandle(FileSelection selection, ScanSettings scan) {
        this.selection = requireNonNull(selection, "selection is null");
        this.scan = requireNonNull(scan, "scan is null");
    }

    @JsonProperty("selection")
    public FileSelection selection() {
        return selection;
    }

    @JsonProperty("scan")
    public ScanSettings scan() {
        return scan;
    }

    @JsonIgnore
    public Charset charset() {
        return CharsetUtils.resolve(scan.charsetName());
    }

    public List<FileSplit> createSplits(boolean splittable) {
        return selection.objects().stream()
                .flatMap(object -> (splittable && object.compression() == Compression.NONE
                        ? SplitPlanner.planSplits(object, scan.splitSizeBytes())
                        : List.of(FileSplit.forWholeFile(object))).stream())
                .toList();
    }

    @JsonIgnore
    public final List<String> columnNames() {
        var names = new ArrayList<>(dataColumnNames());
        if (!selection.pathColumn().isEmpty()) {
            if (names.stream().anyMatch(name -> name.equalsIgnoreCase(selection.pathColumn()))) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
                        "PATH_COLUMN conflicts with a data column: " + selection.pathColumn());
            }
            names.add(selection.pathColumn());
        }
        return List.copyOf(names);
    }

    @JsonIgnore
    public final List<Type> resolveColumnTypes() {
        var types = new ArrayList<>(dataColumnTypes());
        if (!selection.pathColumn().isEmpty()) {
            types.add(VarcharType.VARCHAR);
        }
        return List.copyOf(types);
    }

    @JsonIgnore
    public final String runtimeTableName() {
        return format() + "_load";
    }

    @JsonIgnore
    public abstract String format();

    @JsonIgnore
    public abstract List<String> dataColumnNames();

    @JsonIgnore
    public abstract List<Type> dataColumnTypes();
}

package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.s3.S3ClientBuilder;
import marcbp.trino.s3file.s3.S3Glob;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static marcbp.trino.s3file.util.TableFunctionArguments.optionalString;
import static marcbp.trino.s3file.util.TableFunctionArguments.scalarArgument;
import static marcbp.trino.s3file.util.TableFunctionArguments.stringValue;

/** The fixed object selection and provenance column for one query. */
public record FileSelection(
        @JsonProperty("objects") List<S3ObjectRef> objects,
        @JsonProperty("pathColumn") String pathColumn) {

    @JsonCreator
    public FileSelection {
        objects = List.copyOf(objects);
        if (objects.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "No matching S3 objects");
        }
        pathColumn = java.util.Objects.requireNonNull(pathColumn, "pathColumn is null");
    }

    public static ScalarArgumentSpecification globArgument() {
        return ScalarArgumentSpecification.builder()
                .name("GLOB")
                .type(BooleanType.BOOLEAN)
                .defaultValue(true)
                .build();
    }

    public static ScalarArgumentSpecification pathColumnArgument() {
        return ScalarArgumentSpecification.builder()
                .name("PATH_COLUMN")
                .type(VarcharType.VARCHAR)
                .defaultValue(null)
                .build();
    }

    public static FileSelection resolve(S3ClientBuilder.SessionClient client, String path, Map<String, Argument> arguments) {
        ScalarArgument globArgument = scalarArgument(arguments, "GLOB");
        if (globArgument != null && !(globArgument.getValue() instanceof Boolean)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "GLOB must be true or false");
        }
        boolean globEnabled = globArgument == null || (boolean) globArgument.getValue();
        S3Glob glob = S3Glob.parse(path, globEnabled);
        String compression = optionalString(arguments, "COMPRESSION", "auto");
        Compression.resolve(compression, path); // Validate before issuing S3 requests.
        String column = glob.isGlob() ? "$path" : "";
        ScalarArgument argument = (ScalarArgument) arguments.get("PATH_COLUMN");
        if (argument != null && argument.getValue() != null) {
            column = stringValue(argument, "PATH_COLUMN");
            if (!column.isEmpty() && column.isBlank()) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "PATH_COLUMN cannot be blank; use '' to disable it");
            }
        }
        if (!glob.isGlob()) {
            String literal = "s3://" + glob.bucket() + "/" + glob.prefix();
            return new FileSelection(List.of(S3ObjectRef.from(literal, client.getObjectMetadata(literal), Compression.resolve(compression, literal))), column);
        }
        Map<String, S3ObjectRef> objects = new TreeMap<>();
        Optional<String> token = Optional.empty();
        do {
            var page = client.listObjects(glob.bucket(), glob.prefix(), true, token);
            for (var object : page.objects()) {
                if (!object.key().endsWith("/") && glob.matches(object.key())) {
                    var ref = object.eTag().isPresent()
                            ? new S3ObjectRef(object.path(), object.size(), object.eTag().orElseThrow(), null, Compression.resolve(compression, object.path()))
                            : S3ObjectRef.from(object.path(), client.getObjectMetadata(object.path()), Compression.resolve(compression, object.path()));
                    objects.put(object.key(), ref);
                    if (objects.size() > client.maxGlobFiles()) {
                        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "S3 glob selects more than " + client.maxGlobFiles() + " objects: " + path);
                    }
                }
            }
            token = page.nextContinuationToken();
        }
        while (token.isPresent());
        if (objects.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "No S3 objects match " + path);
        }
        return new FileSelection(List.copyOf(objects.values()), column);
    }

    /** Infer once, from the first usable object. Readers never merge or re-infer schemas. */
    public <T> T infer(S3ClientBuilder.SessionClient client, Charset charset, SchemaReader<T> inference) throws IOException {
        for (S3ObjectRef object : objects) {
            if (object.size() == 0 && object.compression() == Compression.NONE) {
                continue;
            }
            try (var input = FileInput.open(client, object, 0, null);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(input, charset))) {
                T schema = inference.read(reader, object);
                if (schema != null) {
                    return schema;
                }
            }
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot infer a schema: no usable records in the selected objects");
    }

    @FunctionalInterface
    public interface SchemaReader<T> {
        T read(BufferedReader reader, S3ObjectRef object) throws IOException;
    }
}

package marcbp.trino.s3file.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.Argument;
import marcbp.trino.s3file.s3.S3ClientBuilder;
import marcbp.trino.s3file.s3.S3Glob;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static marcbp.trino.s3file.util.TableFunctionArguments.globEnabled;
import static marcbp.trino.s3file.util.TableFunctionArguments.optionalString;
import static marcbp.trino.s3file.util.TableFunctionArguments.resolvePathColumn;

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
        pathColumn = Objects.requireNonNull(pathColumn, "pathColumn is null");
    }

    public static FileSelection resolve(S3ClientBuilder.SessionClient client, String path, Map<String, Argument> arguments) {
        S3Glob glob = S3Glob.parse(path, globEnabled(arguments));
        String compression = optionalString(arguments, "COMPRESSION", "auto");
        Compression.resolve(compression, path); // Validate before issuing S3 requests.
        String column = resolvePathColumn(arguments, glob.isGlob());
        if (!glob.isGlob()) {
            String literal = "s3://" + glob.bucket() + "/" + glob.prefix();
            return new FileSelection(List.of(S3ObjectRef.from(literal, client.getObjectMetadata(literal), Compression.resolve(compression, literal))), column);
        }
        return new FileSelection(listMatches(client, glob, compression, path), column);
    }

    private static List<S3ObjectRef> listMatches(S3ClientBuilder.SessionClient client, S3Glob glob, String compression, String path) {
        Map<String, S3ObjectRef> objects = new TreeMap<>();
        Optional<String> token = Optional.empty();
        do {
            var page = client.listObjects(glob.bucket(), glob.prefix(), true, token);
            for (var object : page.objects()) {
                if (!object.key().endsWith("/") && glob.matches(object.key())) {
                    objects.put(object.key(), resolveObject(client, object, compression));
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
        return List.copyOf(objects.values());
    }

    private static S3ObjectRef resolveObject(S3ClientBuilder.SessionClient client,
            S3ClientBuilder.ListedObject object, String compression) {
        Compression codec = Compression.resolve(compression, object.path());
        return object.eTag().isPresent()
                ? new S3ObjectRef(object.path(), object.size(), object.eTag().orElseThrow(), null, codec)
                : S3ObjectRef.from(object.path(), client.getObjectMetadata(object.path()), codec);
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

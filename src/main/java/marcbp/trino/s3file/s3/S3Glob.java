package marcbp.trino.s3file.s3;

import io.trino.spi.TrinoException;

import java.lang.reflect.Proxy;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.regex.PatternSyntaxException;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/** JDK glob matching and literal prefixes for listing S3 keys. */
public record S3Glob(String bucket, String prefix, PathMatcher matcher) {
    public static S3Glob parse(String path, boolean enabled) {
        var location = S3UriUtils.parse(path);
        if (location.bucket().chars().anyMatch(c -> "*?[]{}\\".indexOf(c) >= 0)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "The bucket must be literal");
        }
        if (!enabled) {
            return new S3Glob(location.bucket(), location.key(), null);
        }
        String key = location.key();
        PathMatcher matcher;
        try {
            matcher = FileSystems.getDefault().getPathMatcher("glob:" + key);
        }
        catch (PatternSyntaxException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid S3 glob: " + e.getDescription(), e);
        }

        // Only extract the listing prefix here; the JDK handles the glob syntax.
        StringBuilder prefix = new StringBuilder();
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c == '\\') {
                c = key.charAt(++i);
            }
            else if ("*?[{".indexOf(c) >= 0) {
                return new S3Glob(location.bucket(), prefix.toString(), matcher);
            }
            prefix.append(c);
        }
        return new S3Glob(location.bucket(), prefix.toString(), null);
    }

    public boolean isGlob() {
        return matcher != null;
    }

    public boolean matches(String key) {
        if (!isGlob()) {
            return prefix.equals(key);
        }
        // The JDK glob matcher reads Path.toString(). A filesystem Path would collapse
        // consecutive slashes, although they distinguish valid S3 keys.
        Path rawKey = (Path) Proxy.newProxyInstance(Path.class.getClassLoader(), new Class<?>[] {Path.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("toString")) {
                        return key;
                    }
                    throw new UnsupportedOperationException("S3 keys only support string matching");
                });
        return matcher.matches(rawKey);
    }
}

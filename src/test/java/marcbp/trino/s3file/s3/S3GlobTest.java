package marcbp.trino.s3file.s3;

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class S3GlobTest {
    @Test
    void recursiveGlobIncludesDirectChildrenAndDotFilesWithoutCrossingSingleStar() {
        var recursive = S3Glob.parse("s3://bucket/events/{,**/}*.jsonl", true);
        assertTrue(recursive.matches("events/direct.jsonl"));
        assertTrue(recursive.matches("events/a/b/deep.jsonl"));
        assertTrue(recursive.matches("events/.hidden.jsonl"));
        assertFalse(recursive.matches("events/a.txt"));
        assertFalse(S3Glob.parse("s3://bucket/events/*.jsonl", true).matches("events/a/deep.jsonl"));
    }

    @Test
    void doubleStarCrossesSeparatorsButDoesNotRemoveThem() {
        var recursive = S3Glob.parse("s3://bucket/events/**/*.jsonl", true);
        assertFalse(recursive.matches("events/direct.jsonl"));
        assertTrue(recursive.matches("events/nested/file.jsonl"));
        assertTrue(recursive.matches("events/a/b/file.jsonl"));
        assertTrue(S3Glob.parse("s3://bucket/events/**.jsonl", true).matches("events/direct.jsonl"));
        assertTrue(S3Glob.parse("s3://bucket/events/**.jsonl", true).matches("events/a/b/file.jsonl"));
    }

    @Test
    void classesAlternativesAndEscapesMatchLiteralS3Keys() {
        var glob = S3Glob.parse("s3://bucket/data/file[0-9]-[!a]?.{csv,txt}", true);
        assertTrue(glob.matches("data/file2-bx.csv"));
        assertTrue(glob.matches("data/file9-z1.txt"));
        assertFalse(glob.matches("data/file2-ax.csv"));
        assertFalse(glob.matches("data/file2-b/.csv"));
        var literal = S3Glob.parse("s3://bucket/data/file\\*.txt", true);
        assertFalse(literal.isGlob());
        assertEquals("data/file*.txt", literal.prefix());
        assertTrue(literal.matches("data/file*.txt"));
    }

    @Test
    void invalidPatternsAndWildcardBucketsFailBeforeListing() {
        for (String path : new String[] {"s3://buck*/x", "s3://bucket/x/[", "s3://bucket/x/{a,b", "s3://bucket/x/{a,{b,c}}", "s3://bucket/x/escape\\"}) {
            assertThrows(TrinoException.class, () -> S3Glob.parse(path, true));
        }
    }
}

package marcbp.trino.s3file.s3;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class S3UriUtilsTest {

    @Test
    void parseExtractsBucketAndKey() {
        S3UriUtils.S3Location location = S3UriUtils.parse("s3://my-bucket/path/to/object.json");
        assertEquals("my-bucket", location.bucket());
        assertEquals("path/to/object.json", location.key());
    }

    @Test
    void parseThrowsOnInvalidUri() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> S3UriUtils.parse("http://not-an-s3-uri"));
        assertEquals("Invalid S3 URI: http://not-an-s3-uri", exception.getMessage());
    }
}

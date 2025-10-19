package marcbp.trino.s3file;

import io.trino.spi.connector.ConnectorFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Verifies the plugin exposes the expected connector factory.
 */
class S3FilePluginTest {
    @Test
    void exposesSingleConnectorFactory() {
        S3FilePlugin plugin = new S3FilePlugin();

        List<ConnectorFactory> factories = StreamSupport.stream(plugin.getConnectorFactories().spliterator(), false)
                .toList();

        assertEquals(1, factories.size());
        ConnectorFactory factory = factories.get(0);
        assertInstanceOf(S3FileConnectorFactory.class, factory);
        assertEquals("s3file", factory.getName());
    }
}

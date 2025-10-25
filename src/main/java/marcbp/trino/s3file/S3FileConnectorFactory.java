package marcbp.trino.s3file;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

public final class S3FileConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "s3file";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        return new S3FileConnector();
    }
}

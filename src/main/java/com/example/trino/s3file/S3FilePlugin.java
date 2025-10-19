package com.example.trino.s3file;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.List;

/**
 * Trino plugin entry point exposing the connector factory.
 */
public final class S3FilePlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return List.of(new S3FileConnectorFactory());
    }
}

package marcbp.trino.s3file;

import io.trino.spi.function.table.ConnectorTableFunction;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

class S3FileConnectorTest {
    @Test
    void exposesListingTableFunctions() {
        S3FileConnector connector = new S3FileConnector();

        Set<ConnectorTableFunction> functions = connector.getTableFunctions();
        assertTrue(functions.stream().anyMatch(function -> function instanceof marcbp.trino.s3file.list.ObjectsTableFunction));
        assertTrue(functions.stream().anyMatch(function -> function instanceof marcbp.trino.s3file.list.BucketsTableFunction));
        connector.shutdown();
    }
}

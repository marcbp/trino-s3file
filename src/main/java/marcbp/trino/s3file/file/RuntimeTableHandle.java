package marcbp.trino.s3file.file;

import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.type.Type;

import java.util.List;

/**
 * Shared contract for table-function-backed runtime handles.
 */
public interface RuntimeTableHandle extends ConnectorTableFunctionHandle, ConnectorTableHandle {
    String runtimeTableName();

    List<String> columnNames();

    List<Type> resolveColumnTypes();
}

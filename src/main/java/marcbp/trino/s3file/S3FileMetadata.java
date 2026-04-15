package marcbp.trino.s3file;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.statistics.TableStatistics;
import marcbp.trino.s3file.file.BaseTextFileHandle;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

final class S3FileMetadata implements ConnectorMetadata {
    private static final String RUNTIME_SCHEMA = "runtime";

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(
            ConnectorSession session,
            ConnectorTableFunctionHandle functionHandle) {
        if (!(functionHandle instanceof BaseTextFileHandle handle)) {
            return Optional.empty();
        }
        List<ColumnHandle> columnHandles = buildColumnHandles(handle).stream()
                .map(columnHandle -> (ColumnHandle) columnHandle)
                .toList();
        return Optional.of(new TableFunctionApplicationResult<>(handle, columnHandles));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        BaseTextFileHandle handle = requireHandle(tableHandle);
        Map<String, ColumnHandle> result = new LinkedHashMap<>();
        for (S3FileColumnHandle columnHandle : buildColumnHandles(handle)) {
            result.put(columnHandle.getName(), columnHandle);
        }
        return Map.copyOf(result);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        BaseTextFileHandle handle = requireHandle(tableHandle);
        S3FileColumnHandle s3Column = requireColumnHandle(columnHandle);
        return new ColumnMetadata(
                s3Column.getName(),
                handle.resolveColumnTypes().get(s3Column.getOrdinalPosition()));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        BaseTextFileHandle handle = requireHandle(tableHandle);
        List<ColumnMetadata> columns = buildColumnHandles(handle).stream()
                .map(columnHandle -> new ColumnMetadata(
                        columnHandle.getName(),
                        handle.resolveColumnTypes().get(columnHandle.getOrdinalPosition())))
                .toList();
        return new ConnectorTableMetadata(getTableName(session, tableHandle), columns);
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return getTableMetadata(session, tableHandle).getTableSchema();
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle tableHandle) {
        BaseTextFileHandle handle = requireHandle(tableHandle);
        return new SchemaTableName(RUNTIME_SCHEMA, handle.format() + "_load");
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return TableStatistics.empty();
    }

    private static BaseTextFileHandle requireHandle(ConnectorTableHandle tableHandle) {
        if (!(tableHandle instanceof BaseTextFileHandle handle)) {
            throw new IllegalArgumentException("Unexpected table handle type: " + tableHandle.getClass().getName());
        }
        return handle;
    }

    private static S3FileColumnHandle requireColumnHandle(ColumnHandle columnHandle) {
        if (!(columnHandle instanceof S3FileColumnHandle s3ColumnHandle)) {
            throw new IllegalArgumentException("Unexpected column handle type: " + columnHandle.getClass().getName());
        }
        return s3ColumnHandle;
    }

    static List<S3FileColumnHandle> buildColumnHandles(BaseTextFileHandle handle) {
        List<String> columnNames = handle.columnNames();
        return java.util.stream.IntStream.range(0, columnNames.size())
                .mapToObj(index -> new S3FileColumnHandle(columnNames.get(index), index))
                .toList();
    }
}

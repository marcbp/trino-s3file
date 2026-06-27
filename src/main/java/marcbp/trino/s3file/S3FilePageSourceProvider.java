package marcbp.trino.s3file;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.ConnectorTransactionHandle;
import marcbp.trino.s3file.csv.CsvTableFunction;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.json.JsonTableFunction;
import marcbp.trino.s3file.objects.ObjectListSplit;
import marcbp.trino.s3file.objects.ObjectsTableFunction;
import marcbp.trino.s3file.txt.TextTableFunction;
import marcbp.trino.s3file.xml.XmlTableFunction;

import java.util.List;
import java.util.Optional;

final class S3FilePageSourceProvider implements ConnectorPageSourceProvider {
    private final CsvTableFunction csvTableFunction;
    private final TextTableFunction textTableFunction;
    private final JsonTableFunction jsonTableFunction;
    private final ObjectsTableFunction objectsTableFunction;
    private final XmlTableFunction xmlTableFunction;

    S3FilePageSourceProvider(
            CsvTableFunction csvTableFunction,
            TextTableFunction textTableFunction,
            JsonTableFunction jsonTableFunction,
            ObjectsTableFunction objectsTableFunction,
            XmlTableFunction xmlTableFunction) {
        this.csvTableFunction = csvTableFunction;
        this.textTableFunction = textTableFunction;
        this.jsonTableFunction = jsonTableFunction;
        this.objectsTableFunction = objectsTableFunction;
        this.xmlTableFunction = xmlTableFunction;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> credentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        List<S3FileColumnHandle> projectedColumns = columns.stream()
                .map(S3FilePageSourceProvider::requireColumnHandle)
                .toList();

        if (table instanceof CsvTableFunction.Handle csvHandle) {
            if (!(split instanceof FileSplit fileSplit)) {
                throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
            }
            return csvTableFunction.createPageSource(session, csvHandle, fileSplit, projectedColumns);
        }
        if (table instanceof TextTableFunction.Handle textHandle) {
            if (!(split instanceof FileSplit fileSplit)) {
                throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
            }
            return textTableFunction.createPageSource(session, textHandle, fileSplit, projectedColumns);
        }
        if (table instanceof JsonTableFunction.Handle jsonHandle) {
            if (!(split instanceof FileSplit fileSplit)) {
                throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
            }
            return jsonTableFunction.createPageSource(session, jsonHandle, fileSplit, projectedColumns);
        }
        if (table instanceof ObjectsTableFunction.Handle objectsHandle) {
            if (!(split instanceof ObjectListSplit objectListSplit)) {
                throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
            }
            return objectsTableFunction.createPageSource(session, objectsHandle, objectListSplit, projectedColumns);
        }
        if (table instanceof XmlTableFunction.Handle xmlHandle) {
            if (!(split instanceof FileSplit fileSplit)) {
                throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
            }
            return xmlTableFunction.createPageSource(session, xmlHandle, fileSplit, projectedColumns);
        }
        throw new IllegalArgumentException("Unexpected table handle type: " + table.getClass().getName());
    }

    private static S3FileColumnHandle requireColumnHandle(ColumnHandle columnHandle) {
        if (!(columnHandle instanceof S3FileColumnHandle s3ColumnHandle)) {
            throw new IllegalArgumentException("Unexpected column handle type: " + columnHandle.getClass().getName());
        }
        return s3ColumnHandle;
    }
}

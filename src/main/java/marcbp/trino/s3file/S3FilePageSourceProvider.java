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
import marcbp.trino.s3file.list.BucketsTableFunction;
import marcbp.trino.s3file.list.ListingSplit;
import marcbp.trino.s3file.list.ObjectsTableFunction;
import marcbp.trino.s3file.txt.TextTableFunction;
import marcbp.trino.s3file.xml.XmlTableFunction;

import java.util.List;
import java.util.Optional;

final class S3FilePageSourceProvider implements ConnectorPageSourceProvider {
    private final CsvTableFunction csvTableFunction;
    private final TextTableFunction textTableFunction;
    private final JsonTableFunction jsonTableFunction;
    private final ObjectsTableFunction objectsTableFunction;
    private final BucketsTableFunction bucketsTableFunction;
    private final XmlTableFunction xmlTableFunction;

    S3FilePageSourceProvider(
            CsvTableFunction csvTableFunction,
            TextTableFunction textTableFunction,
            JsonTableFunction jsonTableFunction,
            ObjectsTableFunction objectsTableFunction,
            BucketsTableFunction bucketsTableFunction,
            XmlTableFunction xmlTableFunction) {
        this.csvTableFunction = csvTableFunction;
        this.textTableFunction = textTableFunction;
        this.jsonTableFunction = jsonTableFunction;
        this.objectsTableFunction = objectsTableFunction;
        this.bucketsTableFunction = bucketsTableFunction;
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

        return switch (table) {
            case CsvTableFunction.Handle csvHandle ->
                    csvTableFunction.createPageSource(session, csvHandle, requireFileSplit(split), projectedColumns);
            case TextTableFunction.Handle textHandle ->
                    textTableFunction.createPageSource(session, textHandle, requireFileSplit(split), projectedColumns);
            case JsonTableFunction.Handle jsonHandle ->
                    jsonTableFunction.createPageSource(session, jsonHandle, requireFileSplit(split), projectedColumns);
            case ObjectsTableFunction.Handle objectsHandle ->
                    objectsTableFunction.createPageSource(session, objectsHandle, requireListingSplit(split), projectedColumns);
            case BucketsTableFunction.Handle bucketsHandle ->
                    bucketsTableFunction.createPageSource(session, bucketsHandle, requireListingSplit(split), projectedColumns);
            case XmlTableFunction.Handle xmlHandle ->
                    xmlTableFunction.createPageSource(session, xmlHandle, requireFileSplit(split), projectedColumns);
            default -> throw new IllegalArgumentException("Unexpected table handle type: " + table.getClass().getName());
        };
    }

    private static FileSplit requireFileSplit(ConnectorSplit split) {
        if (split instanceof FileSplit fileSplit) {
            return fileSplit;
        }
        throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
    }

    private static ListingSplit requireListingSplit(ConnectorSplit split) {
        if (split instanceof ListingSplit listingSplit) {
            return listingSplit;
        }
        throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
    }

    private static S3FileColumnHandle requireColumnHandle(ColumnHandle columnHandle) {
        if (!(columnHandle instanceof S3FileColumnHandle s3ColumnHandle)) {
            throw new IllegalArgumentException("Unexpected column handle type: " + columnHandle.getClass().getName());
        }
        return s3ColumnHandle;
    }
}

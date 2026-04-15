package marcbp.trino.s3file;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.ConnectorTransactionHandle;
import marcbp.trino.s3file.csv.CsvTableFunction;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.json.JsonTableFunction;
import marcbp.trino.s3file.txt.TextTableFunction;
import marcbp.trino.s3file.xml.XmlTableFunction;

import java.util.List;

final class S3FilePageSourceProvider implements ConnectorPageSourceProvider {
    private final CsvTableFunction csvTableFunction;
    private final TextTableFunction textTableFunction;
    private final JsonTableFunction jsonTableFunction;
    private final XmlTableFunction xmlTableFunction;

    S3FilePageSourceProvider(
            CsvTableFunction csvTableFunction,
            TextTableFunction textTableFunction,
            JsonTableFunction jsonTableFunction,
            XmlTableFunction xmlTableFunction) {
        this.csvTableFunction = csvTableFunction;
        this.textTableFunction = textTableFunction;
        this.jsonTableFunction = jsonTableFunction;
        this.xmlTableFunction = xmlTableFunction;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        if (!(split instanceof FileSplit fileSplit)) {
            throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
        }
        List<S3FileColumnHandle> projectedColumns = columns.stream()
                .map(S3FilePageSourceProvider::requireColumnHandle)
                .toList();

        if (table instanceof CsvTableFunction.Handle csvHandle) {
            return csvTableFunction.createPageSource(session, csvHandle, fileSplit, projectedColumns);
        }
        if (table instanceof TextTableFunction.Handle textHandle) {
            return textTableFunction.createPageSource(session, textHandle, fileSplit, projectedColumns);
        }
        if (table instanceof JsonTableFunction.Handle jsonHandle) {
            return jsonTableFunction.createPageSource(session, jsonHandle, fileSplit, projectedColumns);
        }
        if (table instanceof XmlTableFunction.Handle xmlHandle) {
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

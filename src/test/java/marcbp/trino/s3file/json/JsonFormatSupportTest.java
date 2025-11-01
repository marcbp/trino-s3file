package marcbp.trino.s3file.json;

import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.json.JsonFormatSupport.ColumnDefinition;
import marcbp.trino.s3file.json.JsonFormatSupport.ColumnType;
import marcbp.trino.s3file.json.JsonFormatSupport.ColumnsMetadata;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonFormatSupportTest {
    @Test
    void inferColumnsDetectsNamesAndTypes() throws Exception {
        BufferedReader reader = new BufferedReader(new StringReader(
                "{\"id\":1,\"flag\":true,\"value\":12.5}\n"));

        ColumnsMetadata metadata = JsonFormatSupport.inferColumns(reader, "source");

        assertEquals(List.of("id", "flag", "value"), metadata.names());
        assertEquals(List.of(ColumnType.BIGINT, ColumnType.BOOLEAN, ColumnType.DOUBLE), metadata.types());
    }

    @Test
    void parseAdditionalColumnsParsesSpecification() {
        Map<String, Argument> arguments = Map.of(
                JsonTableFunction.ADDITIONAL_COLUMNS_ARGUMENT,
                new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("score:double, active:boolean")));

        List<ColumnDefinition> columns = JsonFormatSupport.parseAdditionalColumns(arguments);

        assertEquals(2, columns.size());
        assertEquals("score", columns.get(0).name());
        assertEquals(ColumnType.DOUBLE, columns.get(0).type());
    }

    @Test
    void mergeAdditionalColumnsAddsOrOverrides() {
        List<String> names = new ArrayList<>(List.of("id"));
        List<ColumnType> types = new ArrayList<>(List.of(ColumnType.BIGINT));
        List<ColumnDefinition> additions = List.of(
                new ColumnDefinition("id", ColumnType.DOUBLE),
                new ColumnDefinition("extra", ColumnType.VARCHAR));

        JsonFormatSupport.mergeAdditionalColumns(names, types, additions);

        assertEquals(List.of("id", "extra"), names);
        assertEquals(List.of(ColumnType.DOUBLE, ColumnType.VARCHAR), types);
    }

    @Test
    void parseObjectValidatesJsonContent() {
        assertThrows(TrinoException.class, () -> JsonFormatSupport.parseObject("[1,2,3]", "source"));
    }

    @Test
    void describeColumnsBuildsSummary() {
        List<String> summary = JsonFormatSupport.describeColumns(
                List.of("id", "active"),
                List.of(ColumnType.BIGINT, ColumnType.BOOLEAN));

        assertEquals(List.of("id:bigint", "active:boolean"), summary);
    }
}

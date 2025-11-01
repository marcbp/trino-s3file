package marcbp.trino.s3file.xml;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.TrinoException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

/**
 * XML-specific support utilities for inferring schemas and streaming rows.
 */
public final class XmlFormatSupport {
    private XmlFormatSupport() {}

    public static Schema inferSchema(BufferedReader reader, String sourceDescription, String rowElement) throws IOException {
        requireNonNull(reader, "reader is null");
        try {
            XMLStreamReader xmlReader = newXmlReader(reader);
            try {
                return collectSchema(xmlReader, sourceDescription, rowElement);
            }
            finally {
                xmlReader.close();
            }
        }
        catch (XMLStreamException e) {
            throw new IOException("Failed to parse XML for " + sourceDescription, e);
        }
    }

    public static XMLStreamReader newXmlReader(BufferedReader reader) throws XMLStreamException {
        XMLInputFactory factory = XMLInputFactory.newFactory();
        factory.setProperty(XMLInputFactory.IS_COALESCING, true);
        factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false);
        return factory.createXMLStreamReader(reader);
    }

    private static Schema collectSchema(XMLStreamReader reader, String sourceDescription, String rowElement) throws XMLStreamException {
        Set<String> attributeNames = new LinkedHashSet<>();
        Set<String> elementNames = new LinkedHashSet<>();
        boolean includeText = false;

        while (reader.hasNext()) {
            int event = reader.next();
            if (event != XMLStreamConstants.START_ELEMENT) {
                continue;
            }
            if (!rowElement.equals(reader.getLocalName())) {
                continue;
            }

            for (int i = 0; i < reader.getAttributeCount(); i++) {
                attributeNames.add(reader.getAttributeLocalName(i));
            }

            int depth = 1;
            while (reader.hasNext()) {
                int innerEvent = reader.next();
                if (innerEvent == XMLStreamConstants.START_ELEMENT) {
                    depth++;
                    if (depth == 2) {
                        elementNames.add(reader.getLocalName());
                    }
                }
                else if (innerEvent == XMLStreamConstants.END_ELEMENT) {
                    if (depth == 1) {
                        return buildSchema(attributeNames, elementNames, includeText);
                    }
                    depth--;
                }
                else if (innerEvent == XMLStreamConstants.CHARACTERS || innerEvent == XMLStreamConstants.CDATA) {
                    if (depth == 1 && !reader.isWhiteSpace()) {
                        includeText = true;
                    }
                }
            }
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Element <" + rowElement + "> not found in " + sourceDescription);
    }

    private static Schema buildSchema(Set<String> attributeNames, Set<String> elementNames, boolean includeText) {
        List<Column> columns = new ArrayList<>();
        Set<String> usedNames = new LinkedHashSet<>();

        for (String attribute : attributeNames) {
            String outputName = uniqueName("@" + attribute, usedNames);
            columns.add(new Column(outputName, ColumnSource.ATTRIBUTE, attribute));
            usedNames.add(outputName);
        }
        for (String element : elementNames) {
            String outputName = uniqueName(element, usedNames);
            columns.add(new Column(outputName, ColumnSource.ELEMENT, element));
            usedNames.add(outputName);
        }
        if (includeText) {
            String outputName = uniqueName("text", usedNames);
            columns.add(new Column(outputName, ColumnSource.TEXT, ""));
        }
        return new Schema(columns);
    }

    private static String uniqueName(String base, Set<String> usedNames) {
        if (!usedNames.contains(base)) {
            return base;
        }
        int suffix = 1;
        while (true) {
            String candidate = base + "_" + suffix;
            if (!usedNames.contains(candidate)) {
                return candidate;
            }
            suffix++;
        }
    }

    public static String[] readNextRecord(XMLStreamReader reader, Schema schema, String rowElement, boolean emptyAsNull) throws XMLStreamException {
        while (reader.hasNext()) {
            int event = reader.next();
            if (event != XMLStreamConstants.START_ELEMENT) {
                continue;
            }
            if (!rowElement.equals(reader.getLocalName())) {
                continue;
            }
            return extractRow(reader, schema, rowElement, emptyAsNull);
        }
        return null;
    }

    private static String[] extractRow(XMLStreamReader reader, Schema schema, String rowElement, boolean emptyAsNull) throws XMLStreamException {
        Map<String, String> attributeValues = new LinkedHashMap<>();
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            attributeValues.put(reader.getAttributeLocalName(i), reader.getAttributeValue(i));
        }

        Map<String, String> elementValues = new LinkedHashMap<>();
        StringBuilder textContent = new StringBuilder();

        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                String localName = reader.getLocalName();
                String text = reader.getElementText();
                if (text != null) {
                    elementValues.putIfAbsent(localName, text.trim());
                }
            }
            else if (event == XMLStreamConstants.CHARACTERS || event == XMLStreamConstants.CDATA) {
                if (!reader.isWhiteSpace()) {
                    textContent.append(reader.getText());
                }
            }
            else if (event == XMLStreamConstants.END_ELEMENT && rowElement.equals(reader.getLocalName())) {
                break;
            }
        }

        String[] values = new String[schema.columns.size()];
        String textValue = textContent.length() == 0 ? null : textContent.toString().trim();

        for (int i = 0; i < schema.columns.size(); i++) {
            Column column = schema.columns.get(i);
            switch (column.source) {
                case ATTRIBUTE -> values[i] = normalizeValue(attributeValues.get(column.sourceName), emptyAsNull);
                case ELEMENT -> values[i] = normalizeValue(elementValues.get(column.sourceName), emptyAsNull);
                case TEXT -> values[i] = normalizeValue(textValue, emptyAsNull);
            }
        }
        return values;
    }

    private static String normalizeValue(String value, boolean emptyAsNull) {
        if (!emptyAsNull || value == null) {
            return value;
        }
        return value.isEmpty() ? null : value;
    }

    public record Schema(List<Column> columns) {
        @JsonCreator
        public Schema(@JsonProperty("columns") List<Column> columns) {
            this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
        }

        public List<String> columnNames() {
            List<String> names = new ArrayList<>(columns.size());
            for (Column column : columns) {
                names.add(column.name());
            }
            return names;
        }
    }

    public record Column(String name, ColumnSource source, String sourceName) {
        @JsonCreator
        public Column(@JsonProperty("name") String name,
                      @JsonProperty("source") ColumnSource source,
                      @JsonProperty("sourceName") String sourceName) {
            this.name = requireNonNull(name, "name is null");
            this.source = requireNonNull(source, "source is null");
            this.sourceName = Objects.requireNonNullElse(sourceName, "");
        }
    }

    public enum ColumnSource {
        ATTRIBUTE,
        ELEMENT,
        TEXT
    }
}

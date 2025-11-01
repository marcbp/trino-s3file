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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
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

    public static Schema appendRawColumn(Schema schema, String columnName) {
        if (columnName == null || columnName.isBlank()) {
            throw new IllegalArgumentException("columnName must not be blank");
        }
        List<Column> columns = new ArrayList<>(schema.columns());
        columns.add(new Column(columnName, ColumnSource.RAW, ""));
        return new Schema(columns);
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

    public static RowExtraction readNextRecord(XMLStreamReader reader, Schema schema, String rowElement, boolean emptyAsNull) throws XMLStreamException {
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
        return RowExtraction.finished();
    }

    private static RowExtraction extractRow(XMLStreamReader reader, Schema schema, String rowElement, boolean emptyAsNull) throws XMLStreamException {
        int columnCount = schema.columns().size();
        String[] values = new String[columnCount];
        boolean[] seen = new boolean[columnCount];

        Map<String, Integer> attributeIndex = new LinkedHashMap<>();
        Map<String, Integer> elementIndex = new LinkedHashMap<>();
        int textColumnIndex = -1;
        int rawColumnIndex = -1;

        for (int columnIndex = 0; columnIndex < schema.columns().size(); columnIndex++) {
            Column column = schema.columns().get(columnIndex);
            switch (column.source()) {
                case ATTRIBUTE -> attributeIndex.put(column.sourceName(), columnIndex);
                case ELEMENT -> elementIndex.putIfAbsent(column.sourceName(), columnIndex);
                case TEXT -> textColumnIndex = columnIndex;
                case RAW -> rawColumnIndex = columnIndex;
            }
        }

        StringBuilder rawBuilder = rawColumnIndex >= 0 ? new StringBuilder() : null;
        appendStartElementForRaw(reader, rawBuilder);

        Map<String, String> attributes = new LinkedHashMap<>();
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            attributes.put(reader.getAttributeLocalName(i), reader.getAttributeValue(i));
        }

        for (Map.Entry<String, Integer> entry : attributeIndex.entrySet()) {
            String attributeValue = attributes.get(entry.getKey());
            int index = entry.getValue();
            values[index] = normalizeValue(attributeValue, emptyAsNull);
            seen[index] = true;
        }

        StringBuilder textBuilder = textColumnIndex >= 0 ? new StringBuilder() : null;
        Deque<ElementFrame> frames = new ArrayDeque<>();
        boolean invalid = false;

        while (reader.hasNext()) {
            int event = reader.next();
            switch (event) {
                case XMLStreamConstants.START_ELEMENT -> {
                    String localName = reader.getLocalName();
                    appendStartElementForRaw(reader, rawBuilder);
                    ElementFrame frame;
                    if (frames.isEmpty()) {
                        int columnIndex = elementIndex.getOrDefault(localName, -1);
                        frame = new ElementFrame(columnIndex);
                        frames.push(frame);
                        if (columnIndex >= 0 && seen[columnIndex]) {
                            invalid = true;
                        }
                    }
                    else {
                        ElementFrame parent = frames.peek();
                        parent.markNested();
                        frame = new ElementFrame(-1);
                        frames.push(frame);
                        invalid = true;
                    }
                }
                case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA -> {
                    String text = reader.getText();
                    appendTextForRaw(rawBuilder, text);
                    if (!frames.isEmpty()) {
                        frames.peek().append(text);
                    }
                    else if (!reader.isWhiteSpace()) {
                        if (textBuilder != null) {
                            textBuilder.append(text);
                        }
                        else {
                            invalid = true;
                        }
                    }
                    else if (textBuilder != null) {
                        textBuilder.append(text);
                    }
                }
                case XMLStreamConstants.END_ELEMENT -> {
                    String localName = reader.getLocalName();
                    appendEndElementForRaw(localName, rawBuilder);
                    if (frames.isEmpty()) {
                        if (!rowElement.equals(localName)) {
                            throw new XMLStreamException("Unexpected closing tag </" + localName + "> inside <" + rowElement + ">");
                        }
                        String textValue = textBuilder == null ? null : textBuilder.toString().trim();
                        if (textValue != null && textValue.isEmpty()) {
                            textValue = null;
                        }
                        if (textColumnIndex >= 0) {
                            values[textColumnIndex] = normalizeValue(textValue, emptyAsNull);
                            seen[textColumnIndex] = true;
                        }
                        if (invalid && rawColumnIndex < 0) {
                            throw new XMLStreamException("Unable to project XML row because it contains unsupported structure");
                        }
                        if (invalid) {
                            for (int i = 0; i < values.length; i++) {
                                if (i != rawColumnIndex) {
                                    values[i] = null;
                                }
                            }
                        }
                        if (rawColumnIndex >= 0) {
                            values[rawColumnIndex] = invalid ? rawBuilder.toString() : null;
                        }
                        return RowExtraction.row(values, !invalid);
                    }

                    ElementFrame frame = frames.pop();
                    if (frame.columnIndex >= 0) {
                        String text = frame.content.toString();
                        String trimmed = text.trim();
                        if (frame.hasNestedContent()) {
                            invalid = true;
                        }
                        String normalised = normalizeValue(trimmed, emptyAsNull);
                        int index = frame.columnIndex;
                        values[index] = normalised;
                        seen[index] = true;
                    }
                }
                default -> {
                    // Ignore
                }
            }
        }

        throw new XMLStreamException("Unexpected end of XML document while reading <" + rowElement + ">");
    }

    private static void appendStartElementForRaw(XMLStreamReader reader, StringBuilder rawBuilder) {
        if (rawBuilder == null) {
            return;
        }
        rawBuilder.append('<').append(reader.getLocalName());
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            rawBuilder.append(' ')
                    .append(reader.getAttributeLocalName(i))
                    .append("=\"")
                    .append(escapeAttribute(reader.getAttributeValue(i)))
                    .append('"');
        }
        rawBuilder.append('>');
    }

    private static void appendEndElementForRaw(String localName, StringBuilder rawBuilder) {
        if (rawBuilder == null) {
            return;
        }
        rawBuilder.append("</").append(localName).append('>');
    }

    private static void appendTextForRaw(StringBuilder rawBuilder, String text) {
        if (rawBuilder == null || text == null || text.isEmpty()) {
            return;
        }
        rawBuilder.append(escapeText(text));
    }

    private static String escapeAttribute(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("&", "&amp;")
                .replace("\"", "&quot;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }

    private static String escapeText(String value) {
        return value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }

    private static String normalizeValue(String value, boolean emptyAsNull) {
        if (value == null) {
            return null;
        }
        if (emptyAsNull && value.isEmpty()) {
            return null;
        }
        return value;
    }

    private static final class ElementFrame {
        private final int columnIndex;
        private final StringBuilder content = new StringBuilder();
        private boolean nestedContent;

        private ElementFrame(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        void append(String text) {
            content.append(text);
        }

        void markNested() {
            nestedContent = true;
        }

        boolean hasNestedContent() {
            return nestedContent;
        }
    }

    public record RowExtraction(boolean done, String[] values, boolean valid) {
        public static RowExtraction finished() {
            return new RowExtraction(true, null, true);
        }

        public static RowExtraction row(String[] values, boolean valid) {
            return new RowExtraction(false, values, valid);
        }
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
        TEXT,
        RAW
    }
}

package me.whitewood.simpledb.adapter.json;

import com.fasterxml.jackson.databind.JsonNode;
import me.whitewood.simpledb.engine.json.common.JsonDataType;
import me.whitewood.simpledb.engine.json.common.JsonTable;
import me.whitewood.simpledb.engine.json.embedded.EmbeddedJsonMaster;
import org.apache.calcite.linq4j.Enumerator;

import java.io.IOException;
import java.util.List;

/**
 * Enumerator that iterates over a json table. It's not thread safe.
 *
 * Since JsonMaster only supports batch query, JsonEnumerator loads the whole result into memory and
 * provides random access. It's inefficient and impractical for real world use cases, but convenient
 * for experimental uses.
 *
 * TODO: support stream-based IO
 **/
public class JsonEnumerator implements Enumerator<Object[]> {

    private final EmbeddedJsonMaster jsonMaster;

    private final JsonTable jsonTable;

    private final List<String> columnNames;

    private final List<JsonDataType> columnTypes;

    private List<JsonNode> resultSet;

    private int index = -1;

    public JsonEnumerator(EmbeddedJsonMaster jsonMaster, JsonTable jsonTable, List<String> columnNames, List<JsonDataType> columnTypes) {
        this.jsonMaster = jsonMaster;
        this.jsonTable = jsonTable;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        try {
            resultSet = jsonMaster.scanTable(jsonTable);
        } catch (IOException e) {
            throw new RuntimeException("Failed to query table " + jsonTable.getName(), e);
        }
    }

    @Override
    public Object[] current() {
        if (index < 0) {
            throw new IllegalStateException("moveNext() must be called before getting the first element.");
        }
        JsonNode currentNode = resultSet.get(index);
        Object[] row = new Object[columnNames.size()];
        for (int i=0;i<columnNames.size();i++) {
            JsonNode fieldNode = currentNode.get(columnNames.get(i));
            JsonDataType jsonDataType = columnTypes.get(i);
            switch (jsonDataType) {
                case STRING:
                    row[i] = fieldNode.asText();
                    break;
                case NUMBER:
                    row[i] = fieldNode.asDouble();
                    break;
                case INTEGER:
                    row[i] = fieldNode.asInt();
                    break;
                case BOOLEAN:
                    row[i] = fieldNode.asBoolean();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported JSON type: " + jsonDataType);

            }
        }
        return row;
    }

    @Override
    public boolean moveNext() {
        if (index < resultSet.size() - 1) {
            index++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void reset() {
        index = -1;
    }

    @Override
    public void close() {
        resultSet = null;
    }
}

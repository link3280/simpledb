package me.whitewood.simpledb.adapter.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import me.whitewood.simpledb.engine.json.JsonDataType;
import me.whitewood.simpledb.engine.json.JsonMaster;
import me.whitewood.simpledb.engine.json.JsonTable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


/**
 * Tests for {@link JsonEnumerator}.
 **/
@RunWith(MockitoJUnitRunner.class)
public class JsonEnumeratorTest {

    @Mock
    private JsonMaster jsonMaster;

    @Mock
    private JsonTable jsonTable;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testIterateTable() throws IOException {
        when(jsonMaster.scanTable(any(JsonTable.class))).thenReturn(
                Lists.newArrayList(
                        OBJECT_MAPPER.readTree("{\"author\":\"George Orwell\", \"title\":\"1984\", \"publish-year\":1949}"),
                        OBJECT_MAPPER.readTree("{\"author\":\"Bertrand Russell\", \"title\":\"Authority and the Individual\", \"publish-year\":1948}"),
                        OBJECT_MAPPER.readTree("{\"author\":\"Montesquieu\", \"title\":\"The Spirit of the Laws\", \"publish-year\":1748}")
                )
        );

        List<String> columnNames = Lists.newArrayList("title", "author", "publish-year");
        List<JsonDataType> columnTypes = Lists.newArrayList(JsonDataType.STRING, JsonDataType.STRING, JsonDataType.INTEGER);

        JsonEnumerator enumerator = new JsonEnumerator(jsonMaster, jsonTable, columnNames, columnTypes);
        assertTrue(enumerator.moveNext());
        Object[] first = enumerator.current();
        assertArrayEquals(new Object[]{"1984", "George Orwell", 1949}, first);

    }
}
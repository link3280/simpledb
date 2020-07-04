package me.whitewood.simpledb.engine.json.server;

import com.fasterxml.jackson.databind.JsonNode;
import me.whitewood.simpledb.engine.json.common.JsonTable;

import java.io.InputStream;
import java.util.List;

/**
 * JsonDatabaseServer is a per-database master, responsible for:
 *
 * 1. Manage metadata of the json database.
 * 2. Provide metadata of the json database.
 * 3. Serve reads(writes are not supported at the moment).
 **/
public interface JsonDatabaseServer {

    /**
     * Start the daemon thread or process.
     * @param isLocal Start as a thread if true, and as a process otherwise.
     */
    void start(boolean isLocal);

    /**
     * Stop the daemon.
     */
    void stop();

    /**
     * List the names of the available {@link JsonTable} in the current database that matches an optional pattern.
     * @param pattern Optional pattern that the table names must match. Null denotes no pattern.
     * @return List of matched table names.
     */
    List<String> listTableNames(String pattern);

    /**
     * List the available {@link JsonTable} in the current database that matches an optional pattern.
     * @param pattern Optional pattern that the table names must match. Null denotes no pattern.
     * @return List of matched table.
     */
    List<JsonTable> listTables(String pattern);

    /**
     * Scan table with a desired column name list.
     * @param tableName The table name.
     * @param columns Optional column names. Null denotes all columns are desired.
     * @return The records in the form of {@link JsonNode}.
     */
    List<JsonNode> scanTable(String tableName, List<String> columns);

    /**
     * Scan table as stream with a desired column name list.
     * @param tableName The table name.
     * @param columns Optional column names. Null denotes all columns are desired.
     * @return The InputStream of the data of the table.
     */
    InputStream scanTableAsStream(String tableName, List<String> columns);

}

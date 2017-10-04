/*
    Copyright 2017 Artem Stasiuk

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.github.terma.sqlonjson;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Convert JSON to in memory SQL database with tables created from JSON.
 * <p>
 * Create in memory DB which will be destroyed as soon as
 * connection will be closed.
 * <p>
 * Thread safe. Each call will create new independent SQL DB.
 * <p>
 * {@link JsonIterator} defines what part of JSON will be converted to tables.
 */
@SuppressWarnings("WeakerAccess")
public class SqlOnJson {

    public static final String INSTANCE_ID_PLACEHOLDER = "<INSTANCE_ID>";

    public static final String DEFAULT_DRIVER = "org.hsqldb.jdbc.JDBCDriver";
    public static final String DEFAULT_URL = "jdbc:hsqldb:mem:sql_on_json_" + INSTANCE_ID_PLACEHOLDER + ";shutdown=true";
    public static final String DEFAULT_USERNAME = "sa";
    public static final String DEFAULT_PASSWORD = "";

    private static final Logger LOGGER = Logger.getLogger(SqlOnJson.class.getName());

    private final AtomicInteger counter;
    private final String driver;
    private final String url;
    private final String username;
    private final String password;

    /**
     * @param driver DB driver class which implement JDBC interface
     * @param url    regular JDBC URL string with optional {@link SqlOnJson#INSTANCE_ID_PLACEHOLDER} placeholder which will be
     *               replaced on instance ID during conversion to make sure that all instances are unique
     */
    public SqlOnJson(String driver, String url, String username, String password) {
        this.counter = new AtomicInteger();
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    /**
     * With default DB H2 in in-memory private mode (DB life until connection will be closed)
     */
    public SqlOnJson() {
        this(DEFAULT_DRIVER, DEFAULT_URL, DEFAULT_USERNAME, DEFAULT_PASSWORD);
    }

    private static LinkedHashMap<String, ColumnType> getColumns(JsonTable jsonTable) {
        final LinkedHashMap<String, ColumnType> cls = new LinkedHashMap<>();

        for (int i = 0; i < jsonTable.data.size(); i++) {
            final JsonObject jsonObject = jsonTable.data.get(i).getAsJsonObject();
            for (Map.Entry<String, JsonElement> part : jsonObject.entrySet()) {
                ColumnType columnType = ColumnType.STRING;
                if (part.getValue().isJsonPrimitive()) {
                    if (part.getValue().getAsString().matches("[-0-9]+")) {
                        columnType = ColumnType.BIGINT;
                    } else if (part.getValue().getAsString().matches("[-0-9.]+")) {
                        columnType = ColumnType.DOUBLE;
                    }
                }

                ColumnType current = cls.get(part.getKey());
                if (current == null || current != ColumnType.STRING) cls.put(part.getKey(), columnType);
            }
        }
        return cls;
    }

    private static String nameToSqlName(final String columnName) {
        String first = columnName.substring(0, 1);
        if (first.matches("[^a-zA-Z]")) first = "i";

        return first + columnName.substring(1).replaceAll("[^a-zA-Z0-9_]+", "");
    }

    /**
     * Convert JSON to SQL and assume that root of JSON is Object properties
     * for which with array type could be converted to tables {@link Plain}
     *
     * @param json - json
     * @return connection to in mem db with tables
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public Connection convertPlain(String json) throws SQLException, ClassNotFoundException {
        return convert(new Plain(json));
    }

    public Connection convert(JsonIterator jsonIterator) throws SQLException, ClassNotFoundException {
        Class.forName(driver);

        if (counter.get() > Integer.MAX_VALUE - 10) counter.set(0); // to avoid possible overflow, who knows =)
        final int id = counter.incrementAndGet();

        final Connection c = DriverManager.getConnection(url.replaceAll(INSTANCE_ID_PLACEHOLDER, String.valueOf(id)), username, password);
        try {
            final long start = System.currentTimeMillis();

            int countOfTables = 0;

            while (jsonIterator.hasNext()) {
                final JsonTable jsonTable = jsonIterator.next();

                if (jsonTable.data.size() == 0) continue;

                countOfTables++;

                List<ColumnType> columnTypes = new ArrayList<>();
                List<String> columnWithTypes = new ArrayList<>();
                List<String> columns = new ArrayList<>();

                final LinkedHashMap<String, ColumnType> cls = getColumns(jsonTable);

                final String sqlTableName = nameToSqlName(jsonTable.name);
                for (final Map.Entry<String, ColumnType> tt : cls.entrySet()) {
                    columns.add(tt.getKey());
                    columnTypes.add(tt.getValue());
                    columnWithTypes.add(nameToSqlName(tt.getKey()) + " " + tt.getValue().getSqlName());
                }

                try (PreparedStatement ps = c.prepareStatement("create table " + sqlTableName + " (" + StringUtils.join(columnWithTypes, ", ") + ")")) {
                    ps.execute();
                }

                String parameterPlaceholders = StringUtils.repeat("?", ",", cls.size());
                try (PreparedStatement ps = c.prepareStatement("insert into " + sqlTableName + " values (" + parameterPlaceholders + ")")) {
                    for (int i = 0; i < jsonTable.data.size(); i++) {
                        JsonObject item = jsonTable.data.get(i).getAsJsonObject();
                        for (int cl = 1; cl <= cls.size(); cl++) {
                            JsonElement value = item.get(columns.get(cl - 1));
                            columnTypes.get(cl - 1).setToPs(ps, cl, value);
                        }
                        ps.execute();
                    }
                }
            }


            LOGGER.info("JSON " + jsonIterator.getJsonLength() + " chars to SQL DB with "
                    + countOfTables + " tables in " + (System.currentTimeMillis() - start) + " msec");
        } catch (Exception exception) {
            c.close();
            throw exception;
        }
        return c;
    }

}

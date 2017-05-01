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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@SuppressWarnings("WeakerAccess")
public class JsonToSql {

    private static final Logger LOGGER = Logger.getLogger(JsonToSql.class.getName());

    public static Connection convertPlain(String json) throws SQLException, ClassNotFoundException {
        return convert(new Plain(json));
    }

    public static Connection convert(JsonIterator jsonIterator) throws SQLException, ClassNotFoundException {
        Class.forName("org.hsqldb.jdbc.JDBCDriver");
        final Connection c = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb;shutdown=true", "SA", "");

        final long start = System.currentTimeMillis();

        int countOfTables = 0;

        while (jsonIterator.hasNext()) {
            final JsonTable jsonTable = jsonIterator.next();

            if (jsonTable.data.size() == 0) continue;

            countOfTables++;

            List<ColumnType> columnTypes = new ArrayList<>();
            List<String> columnWithTypes = new ArrayList<>();
            List<String> columns = new ArrayList<>();
            JsonObject first = jsonTable.data.get(0).getAsJsonObject();
            for (Map.Entry<String, JsonElement> part : first.entrySet()) {
                columns.add(part.getKey());

                ColumnType columnType = ColumnType.STRING;
                if (part.getValue().isJsonPrimitive()) {
                    if (part.getValue().getAsString().matches("[-0-9]+")) {
                        columnType = ColumnType.BIGINT;
                    } else if (part.getValue().getAsString().matches("[-0-9.]+")) {
                        columnType = ColumnType.DOUBLE;
                    }
                }

                columnTypes.add(columnType);
                columnWithTypes.add(part.getKey() + " " + columnType.getSqlName());
            }

            try (PreparedStatement ps = c.prepareStatement("create table " + jsonTable.name + " (" + StringUtils.join(columnWithTypes, ", ") + ")")) {
                ps.execute();
            }

            String parameterPlaceholders = StringUtils.repeat("?", ",", columns.size());
            try (PreparedStatement ps = c.prepareStatement("insert into " + jsonTable.name + " values (" + parameterPlaceholders + ")")) {
                for (int i = 0; i < jsonTable.data.size(); i++) {
                    JsonObject item = jsonTable.data.get(i).getAsJsonObject();
                    for (int cl = 1; cl <= columns.size(); cl++) {
                        JsonElement value = item.get(columns.get(cl - 1));
                        columnTypes.get(cl - 1).setToPs(ps, cl, value);
                    }
                    ps.execute();
                }
            }
        }


        LOGGER.info("JSON " + jsonIterator.getJsonLength() + " chars to SQL DB with "
                + countOfTables + " tables in " + (System.currentTimeMillis() - start) + " msec");
        return c;
    }

    private interface JsonIterator extends Iterator<JsonTable> {

        int getJsonLength();

    }

    private static class JsonTable {
        public final String name;
        public final JsonArray data;

        private JsonTable(String name, JsonArray data) {
            this.name = name;
            this.data = data;
        }
    }

    private static class Plain implements JsonIterator {

        private final int jsonLength;
        private final Iterator<JsonTable> iterator;

        public Plain(String json) {
            List<JsonTable> list = new ArrayList<>();
            if (StringUtils.isNoneEmpty(json)) {
                final JsonParser parser = new JsonParser();
                final JsonObject jsonObject = parser.parse(json).getAsJsonObject();
                for (Map.Entry<String, JsonElement> e : jsonObject.entrySet()) {
                    if (e.getValue().isJsonArray()) {
                        list.add(new JsonTable(e.getKey(), e.getValue().getAsJsonArray()));
                    }
                }
            }
            jsonLength = json.length();
            iterator = list.iterator();
        }

        @Override
        public int getJsonLength() {
            return jsonLength;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public JsonTable next() {
            return iterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}

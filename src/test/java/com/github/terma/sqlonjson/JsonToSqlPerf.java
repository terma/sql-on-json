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

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JsonToSqlPerf {

    private static StringBuilder generateJson() {
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{sources: [");
        for (int i = 0; i < 10000; i++) {
            if (i > 0) jsonBuilder.append(",");
            jsonBuilder.append("{id:").append(i)
                    .append(",name:\"name of object which is big\"")
                    .append(",creation_time:" + System.currentTimeMillis())
                    .append("}");
        }
        jsonBuilder.append("]}");
        return jsonBuilder;
    }

    @Test
    public void test() throws SQLException, ClassNotFoundException {
        String json = generateJson().toString();

        try (Connection c = JsonToSql.convertPlain(json)) {
            try (PreparedStatement ps = c.prepareStatement("select * as count_of_rows from sources order by id desc limit 5")) {
                try (ResultSet rs = ps.executeQuery()) {
                    printResultSet(rs);
                }
            }
        }
    }

    private void printResultSet(ResultSet rs) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            List<String> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getMetaData().getColumnName(i) + "='" + rs.getObject(i) + "'");
            }
            System.out.println(StringUtils.join(row, " "));
        }
    }

}

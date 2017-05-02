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

import java.sql.PreparedStatement;
import java.sql.SQLException;

enum ColumnType {

    STRING,

    BIGINT {
        @Override
        public String getSqlName() {
            return "bigint";
        }

        @Override
        public void setToPs(PreparedStatement ps, int index, JsonElement value) throws SQLException {
            if (value == null) ps.setObject(index, null);
            else ps.setLong(index, value.getAsLong());
        }
    },

    DOUBLE {
        @Override
        public String getSqlName() {
            return "double";
        }

        @Override
        public void setToPs(PreparedStatement ps, int index, JsonElement value) throws SQLException {
            if (value == null) ps.setObject(index, null);
            else ps.setDouble(index, value.getAsDouble());
        }
    };

    public String getSqlName() {
        return "varchar(8000)";
    }

    public void setToPs(PreparedStatement ps, int index, JsonElement value) throws SQLException {
        if (value == null) ps.setString(index, null);
        else if (value.isJsonPrimitive()) ps.setString(index, value.getAsString());
        else ps.setString(index, value.toString());
    }
}

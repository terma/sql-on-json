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

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;

public class JsonToSqlTest {

    @Test
    public void representEmptyJsonAsEmptyDb() throws Exception {
        try (Connection c = JsonToSql.convertPlain("")) {
            ResultSet rs = c.getMetaData().getTables(null, null, "a", null);
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void representObjectWithEmptyArrayPropertyAsEmptyDb() throws Exception {
        try (Connection c = JsonToSql.convertPlain("{a:[]}")) {
            ResultSet rs = c.getMetaData().getTables(null, null, "a", null);
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void representObjectWithArrayPropertyAsTableInDb() throws Exception {
        try (Connection c = JsonToSql.convertPlain("{a:[{id:12000,name:\"super\"},{id:90,name:\"remta\"}]}")) {
            ResultSet rs = c.prepareStatement("select * from a").executeQuery();
            rs.next();
            Assert.assertEquals(12000, rs.getLong("id"));
            Assert.assertEquals("super", rs.getString("name"));
            rs.next();
            Assert.assertEquals(90, rs.getLong("id"));
            Assert.assertEquals("remta", rs.getString("name"));
        }
    }

    @Test
    public void representNumberPropertyAsLong() throws Exception {
        try (Connection c = JsonToSql.convertPlain("{a:[{o:" + Long.MAX_VALUE + "},{o:" + Long.MIN_VALUE + "}]}")) {
            ResultSet rs = c.prepareStatement("select * from a").executeQuery();
            Assert.assertEquals("BIGINT", rs.getMetaData().getColumnTypeName(1));
            rs.next();
            Assert.assertEquals(Long.MAX_VALUE, rs.getLong("o"));
            rs.next();
            Assert.assertEquals(Long.MIN_VALUE, rs.getLong("o"));
        }
    }

    @Test
    public void representNumberWithPrecisionPropertyAsDouble() throws Exception {
        try (Connection c = JsonToSql.convertPlain("{a:[{o:0.009},{o:-12.45}]}")) {
            ResultSet rs = c.prepareStatement("select * from a").executeQuery();
            Assert.assertEquals("DOUBLE", rs.getMetaData().getColumnTypeName(1));
            rs.next();
            Assert.assertEquals(0.009, rs.getDouble("o"), 0.0001);
            rs.next();
            Assert.assertEquals(-12.45, rs.getDouble("o"), 0.1);
        }
    }

    @Test
    public void representObjectWithArrayPropertyWithMissedAttributesAsTableInDbWithNull() throws Exception {
        try (Connection c = JsonToSql.convertPlain("{a:[{id:12000,name:\"super\"},{id:90}]}")) {
            ResultSet rs = c.prepareStatement("select * from a").executeQuery();
            rs.next();
            Assert.assertEquals(12000, rs.getLong("id"));
            Assert.assertEquals("super", rs.getString("name"));
            rs.next();
            Assert.assertEquals(90, rs.getLong("id"));
            Assert.assertEquals(null, rs.getString("name"));
        }
    }

    @Test
    public void representObjectWithArrayPropertiesAsMultipleTables() throws Exception {
        try (Connection c = JsonToSql.convertPlain("{orders:[{id:12}],history:[{orderId:12}]}")) {
            ResultSet rs1 = c.prepareStatement("select * from orders").executeQuery();
            rs1.next();
            Assert.assertEquals(12, rs1.getLong("id"));
            Assert.assertFalse(rs1.next());

            ResultSet rs2 = c.prepareStatement("select * from history").executeQuery();
            Assert.assertTrue(rs2.next());
        }
    }

    @Test
    public void representEmbeddedObjectAsString() throws Exception {
        try (Connection c = JsonToSql.convertPlain("{orders:[{em:{a:12}}]}")) {
            ResultSet rs1 = c.prepareStatement("select * from orders").executeQuery();
            rs1.next();
            Assert.assertEquals("{\"a\":12}", rs1.getString("em"));
            Assert.assertFalse(rs1.next());
        }
    }

    @Test
    public void supportEmbeddedArrayToTable() throws Exception {
        try (Connection c = JsonToSql.convertPlain("{orders:[{em:[{a:-7}]}]}")) {
            ResultSet rs1 = c.prepareStatement("select * from orders").executeQuery();
            rs1.next();
            Assert.assertEquals("[{\"a\":-7}]", rs1.getString("em"));
            Assert.assertFalse(rs1.next());
        }
    }

    @Test
    public void supportEmbeddedObjectAsTable() throws Exception {
        try (Connection c = JsonToSql.convertPlain("{orders:{em:[{a:-7}]}}")) {
            ResultSet rs1 = c.getMetaData().getTables(null, null, "orders", null);
            Assert.assertFalse(rs1.next());
        }
    }

}

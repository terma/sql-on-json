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
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Assumes JSON contains root Object. All properties of root object with array type will be converted
 * to SQL tables.
 */
public class Plain implements JsonIterator {

    private final int jsonLength;
    private final Iterator<JsonTable> iterator;

    @SuppressWarnings("WeakerAccess")
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

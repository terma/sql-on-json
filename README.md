# sql-on-json

[![Build Status](https://travis-ci.org/terma/sql-on-json.svg?branch=master)](https://travis-ci.org/terma/sql-on-json) 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.terma/sql-on-json/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.terma/sql-on-json/)

## How to use

```java
try (Connection c = sqlOnJson.convertPlain("{a:[{id:12000,name:\"super\"},{id:90,name:\"remta\"}]}")) {
    ResultSet rs = c.prepareStatement("select * from a").executeQuery();
    while (rs.next()) {
        // my business logic
    }
}
```
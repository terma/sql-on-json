# sql-on-json

[![Build Status](https://travis-ci.org/terma/sql-on-json.svg?branch=master)](https://travis-ci.org/terma/sql-on-json) 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.terma/sql-on-json/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.terma/sql-on-json/)

## How to use

```java
try (Connection c = new SqlOnJson().convertPlain("{a:[{id:12000,name:\"super\"},{id:90,name:\"remta\"}]}")) {
    ResultSet rs = c.prepareStatement("select * from a").executeQuery();
    while (rs.next()) {
        // my business logic
    }
}
```

## Use non default DB

By default ```sql-on-json``` uses [HSQLDB](http://hsqldb.org/)

```java
final SqlOnJson sqlOnJson = new SqlOnJson("driver-class", "url", "user", "password");
try (Connection c = sqlOnJson.convertPlain("{a:[{id:12000,name:\"super\"},{id:90,name:\"remta\"}]}")) {
    ...
}
```

For example you can use [H2](http://www.h2database.com/)
```java
final SqlOnJson sqlOnJson = new SqlOnJson("org.h2.Driver", "jdbc:h2:mem:", "", "");
try (...)
```

To make DB URL unique per ```new SqlOnJson(...)``` you can use placeholder ```<INSTANCE_ID>``` in second parameter of constructor ```url```, for HSQLDB it will be ```jdbc:hsqldb:mem:sql_on_json_<INSTANCE_ID>;shutdown=true```

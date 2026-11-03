package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static com.statedriven.agent.SqlStore.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

final class SchemaValidator {
  static byte[] resource(String name) throws Exception {
    try (var in = SchemaValidator.class.getResourceAsStream("/schema/" + name)) {
      if (in == null) throw new IllegalStateException("Missing schema resource: " + name);
      return in.readAllBytes();
    }
  }

  static ArrayNode tuples(Connection c, String sql, String schema, String table)
      throws SQLException {
    try (var p = statement(c, sql, schema, table);
        var rs = p.executeQuery()) {
      ArrayNode values = arr();
      int columns = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        ArrayNode row = arr();
        for (int i = 1; i <= columns; i++) {
          Object v = rs.getObject(i);
          if (v instanceof String text)
            v = text.replace("\"" + schema + "\".", "").replace(schema + ".", "");
          row.add(tree(v));
        }
        values.add(row);
      }
      return values;
    }
  }

  static void validate(Connection c) throws Exception {
    if (!c.isReadOnly())
      throw new IllegalStateException("Schema validation requires a read-only transaction");
    JsonNode manifest =
        parse(new String(resource("layout-v6.json"), StandardCharsets.UTF_8)).path("postgres.sql");
    if (!manifest.path("source_sha256").asText().equals(sha(resource("postgres.sql"))))
      throw new IllegalStateException("Schema layout manifest differs from source");
    try (var sql = c.createStatement()) {
      sql.execute(new String(resource("validate.sql"), StandardCharsets.UTF_8));
    }
    String schema = one(c, "SELECT current_schema() AS name").path("name").asText();
    var fields = manifest.path("tables").fields();
    while (fields.hasNext()) {
      var table = fields.next();
      ArrayNode actual =
          arr(
              tuples(
                  c,
                  "SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a JOIN pg_class t ON t.oid=a.attrelid JOIN pg_namespace n ON n.oid=t.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=t.oid AND d.adnum=a.attnum WHERE n.nspname=? AND t.relname=? AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
                  schema,
                  table.getKey()),
              tuples(
                  c,
                  "SELECT c.conname,c.contype,c.condeferrable,c.condeferred,pg_get_constraintdef(c.oid,true) FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname=? AND t.relname=? AND c.contype<>'n' ORDER BY c.conname",
                  schema,
                  table.getKey()),
              tuples(
                  c,
                  "SELECT indexname,indexdef FROM pg_indexes WHERE schemaname=? AND tablename=? ORDER BY indexname",
                  schema,
                  table.getKey()));
      if (!equal(actual, table.getValue()))
        throw new IllegalStateException(
            "Schema 6 incompatible: "
                + table.getKey()
                + "; run the explicit initializer. Runtime performs no migrations.");
    }
  }
}

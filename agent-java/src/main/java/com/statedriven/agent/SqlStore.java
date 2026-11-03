package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.sql.*;
import java.util.*;
import org.springframework.core.env.Environment;

/** JDBC connections are confined to blocking workers; every aggregate has one transaction. */
class SqlStore {
  final Environment env;
  final String databaseSetting;

  SqlStore(Environment env) {
    this(env, "AGENT_SESSION_DATABASE_URL");
  }

  SqlStore(Environment env, String setting) {
    this.env = env;
    this.databaseSetting = setting;
  }

  Connection connect() throws SQLException {
    String url =
        env.getProperty(
            databaseSetting,
            env.getProperty(
                "AGENT_SESSION_DATABASE_URL",
                "postgresql://user:password@192.168.1.50:5432/universal_state"));
    String user = env.getProperty("AGENT_SESSION_DB_USER", "user"),
        pass = env.getProperty("AGENT_SESSION_DB_PASSWORD", "password");
    if (url.startsWith("postgresql://") || url.startsWith("postgres://")) {
      var uri = java.net.URI.create(url);
      if (uri.getUserInfo() != null) {
        String[] pair = uri.getUserInfo().split(":", 2);
        user = java.net.URLDecoder.decode(pair[0], java.nio.charset.StandardCharsets.UTF_8);
        pass =
            pair.length > 1
                ? java.net.URLDecoder.decode(pair[1], java.nio.charset.StandardCharsets.UTF_8)
                : "";
      }
      url =
          "jdbc:postgresql://"
              + uri.getHost()
              + ":"
              + (uri.getPort() < 0 ? 5432 : uri.getPort())
              + uri.getRawPath()
              + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery());
    }
    return DriverManager.getConnection(url, user, pass);
  }

  @FunctionalInterface
  interface Transaction<T> {
    T run(Connection c) throws Exception;
  }

  <T> T transaction(Transaction<T> work) {
    try (Connection c = connect()) {
      c.setAutoCommit(false);
      c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      try {
        T value = work.run(c);
        c.commit();
        return value;
      } catch (Exception e) {
        try {
          c.rollback();
        } catch (SQLException ignored) {
        }
        throw e;
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new StoreFailure(e);
    }
  }

  <T> T read(Transaction<T> work) {
    return transaction(
        c -> {
          c.setReadOnly(true);
          return work.run(c);
        });
  }

  static PreparedStatement statement(Connection c, String sql, Object... values)
      throws SQLException {
    PreparedStatement p = c.prepareStatement(sql);
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      if (value instanceof JsonNode n) p.setString(i + 1, n.toString());
      else if (value instanceof UUID u) p.setObject(i + 1, u);
      else p.setObject(i + 1, value);
    }
    return p;
  }

  static int update(Connection c, String sql, Object... values) throws SQLException {
    try (var p = statement(c, sql, values)) {
      return p.executeUpdate();
    }
  }

  static List<ObjectNode> rows(Connection c, String sql, Object... values) throws SQLException {
    try (var p = statement(c, sql, values);
        var rs = p.executeQuery()) {
      List<ObjectNode> rows = new ArrayList<>();
      var m = rs.getMetaData();
      while (rs.next()) {
        ObjectNode row = obj();
        for (int i = 1; i <= m.getColumnCount(); i++) {
          Object v = rs.getObject(i);
          String type = m.getColumnTypeName(i);
          if (v != null && type.equals("timestamptz"))
            v = timestamp(rs.getObject(i, java.time.OffsetDateTime.class));
          JsonNode n;
          if (v == null) n = NullNode.instance;
          else if (type.equals("jsonb") || type.equals("json")) n = parse(v.toString());
          else if (v instanceof Number || v instanceof Boolean) n = tree(v);
          else n = tree(v.toString());
          row.set(m.getColumnLabel(i), n);
        }
        rows.add(row);
      }
      return rows;
    }
  }

  static ObjectNode one(Connection c, String sql, Object... values) throws SQLException {
    var rows = rows(c, sql, values);
    return rows.isEmpty() ? null : rows.get(0);
  }

  static String timestamp(java.time.OffsetDateTime time) {
    time = time.withOffsetSameInstant(java.time.ZoneOffset.UTC);
    return time.format(
        java.time.format.DateTimeFormatter.ofPattern(
            time.getNano() == 0 ? "uuuu-MM-dd'T'HH:mm:ss'Z'" : "uuuu-MM-dd'T'HH:mm:ss.SSSSSS'Z'"));
  }

  static UUID id(JsonNode n) {
    return n == null || n.isNull() || n.isMissingNode() ? null : UUID.fromString(n.asText());
  }

  static UUID id(String n) {
    return n == null ? null : UUID.fromString(n);
  }

  static class StoreFailure extends RuntimeException {
    StoreFailure(Throwable e) {
      super(e);
    }
  }
}

package com.statedriven.agent;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.*;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

class UpgradeSafetyTest {
  @Test
  void transactionsRollbackAllFailedAggregates() throws Exception {
    Connection connection = mock(Connection.class);
    SqlStore store =
        new SqlStore(new MockEnvironment()) {
          @Override
          Connection connect() {
            return connection;
          }
        };
    assertThrows(
        IllegalStateException.class,
        () ->
            store.transaction(
                c -> {
                  throw new IllegalStateException("injected aggregate failure");
                }));
    verify(connection).setAutoCommit(false);
    verify(connection).rollback();
    verify(connection, never()).commit();
    verify(connection).close();
  }

  @Test
  void readsAreExplicitlyReadOnly() throws Exception {
    Connection connection = mock(Connection.class);
    SqlStore store =
        new SqlStore(new MockEnvironment()) {
          @Override
          Connection connect() {
            return connection;
          }
        };
    assertEquals("value", store.read(c -> "value"));
    verify(connection).setReadOnly(true);
    verify(connection).commit();
  }

  @Test
  void schemaValidationRefusesWritableTransaction() throws Exception {
    Connection connection = mock(Connection.class);
    when(connection.isReadOnly()).thenReturn(false);
    assertThrows(IllegalStateException.class, () -> SchemaValidator.validate(connection));
    verify(connection, never()).createStatement();
  }

  @Test
  void applicationMapperRejectsDuplicateKeys() {
    var mapper = new AgentApplication().objectMapper();
    assertThrows(Exception.class, () -> mapper.readTree("{\"goal\":1,\"goal\":2}"));
  }

  @Test
  void commandNamespaceHasNoUnstableFallback() {
    var store = new CommandStore(new MockEnvironment());
    assertThrows(IllegalStateException.class, store::namespace);
  }
}

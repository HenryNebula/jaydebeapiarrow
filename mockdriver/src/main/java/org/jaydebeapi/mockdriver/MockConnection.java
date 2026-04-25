package org.jaydebeapi.mockdriver;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.*;
import java.time.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import org.mockito.Mockito;

public abstract class MockConnection implements Connection {

  ResultSet mockResultSet;

  private static Throwable createException(String className, String exceptionMessage) {
    try {
      return (Throwable) Class.forName(className).getConstructor(String.class)
          .newInstance(exceptionMessage);
    } catch (Exception e) {
      throw new RuntimeException("Couldn't initialize class " + className + ".", e);
    }
  }

  private static int extractTypeCodeForName(String sqlTypesName) {
    try {
      Field field = Types.class.getField(sqlTypesName);
      return field.getInt(null);
    } catch (NoSuchFieldException e) {
      throw new IllegalArgumentException("Type " + sqlTypesName + " not found in Types class.", e);
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static void mockGeneralResultSetMetaData(ResultSetMetaData mockMetaData, int columnType) throws SQLException {
    int column = 1;
    Mockito.when(mockMetaData.getCatalogName(column)).thenReturn("DummyCatalog");
    Mockito.when(mockMetaData.getColumnClassName(1)).thenReturn("Object");
    Mockito.when(mockMetaData.getColumnCount()).thenReturn(1);
    Mockito.when(mockMetaData.getColumnDisplaySize(column)).thenReturn(1);
    Mockito.when(mockMetaData.getColumnName(column)).thenReturn("DummyColumn");
    Mockito.when(mockMetaData.getColumnLabel(column)).thenReturn("DummyColumn");
    Mockito.when(mockMetaData.getColumnType(column)).thenReturn(columnType);
    Mockito.when(mockMetaData.getColumnTypeName(column)).thenReturn(JDBCType.valueOf(columnType).getName());
    Mockito.when(mockMetaData.getSchemaName(column)).thenReturn("DummySchema");
    Mockito.when(mockMetaData.getTableName(column)).thenReturn("DummyTable");
    Mockito.when(mockMetaData.isAutoIncrement(column)).thenReturn(false);
    Mockito.when(mockMetaData.isCaseSensitive(column)).thenReturn(false);
    Mockito.when(mockMetaData.isCurrency(column)).thenReturn(false);
    Mockito.when(mockMetaData.isDefinitelyWritable(column)).thenReturn(false);
    Mockito.when(mockMetaData.isNullable(column)).thenReturn(mockMetaData.columnNullable);
    Mockito.when(mockMetaData.isReadOnly(column)).thenReturn(false);
    Mockito.when(mockMetaData.isSearchable(column)).thenReturn(true);
    Mockito.when(mockMetaData.isSigned(column)).thenReturn(true);
    Mockito.when(mockMetaData.isWritable(column)).thenReturn(true);
  }

  public final void mockStringResult(String value) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for string)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.VARCHAR);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
    Mockito.when(mockResultSet.getObject(1)).thenReturn(value);
    Mockito.when(mockResultSet.getString(1)).thenReturn(value);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final void mockExceptionOnCommit(String className, String exceptionMessage)
      throws SQLException {
    Throwable exception = createException(className, exceptionMessage);
    Mockito.doThrow(exception).when(this).commit();
  }

  public final void mockExceptionOnRollback(String className, String exceptionMessage)
      throws SQLException {
    Throwable exception = createException(className, exceptionMessage);
    Mockito.doThrow(exception).when(this).rollback();
  }

  public final void mockExceptionOnExecute(String className, String exceptionMessage)
      throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Throwable exception = createException(className, exceptionMessage);
    Mockito.when(mockPreparedStatement.execute()).thenThrow(exception);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final void mockBinaryResult(byte[] value) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for binary)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.VARBINARY);
    Mockito.when(mockResultSet.getBytes(1)).thenReturn(value);
    Mockito.when(mockResultSet.getObject(1)).thenReturn(value);
    Mockito.when(mockResultSet.wasNull()).thenReturn(false);
    Mockito.when(mockResultSet.getBinaryStream(1)).thenReturn(new java.io.ByteArrayInputStream(value));
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final void mockBigDecimalResult(long value, int scale) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for BigDecimal)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.DECIMAL);
    mockMetaData.getPrecision(10);
    mockMetaData.getScale(5);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);

    BigDecimal columnValue = BigDecimal.valueOf(value, scale);
    Mockito.when(mockResultSet.getObject(1)).thenReturn(columnValue);
    Mockito.when(mockResultSet.getBigDecimal(1)).thenReturn(columnValue);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final void mockNullDecimalResult(int precision, int scale) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for null Decimal)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.DECIMAL);
    Mockito.when(mockMetaData.getPrecision(1)).thenReturn(precision);
    Mockito.when(mockMetaData.getScale(1)).thenReturn(scale);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);

    Mockito.when(mockResultSet.getObject(1)).thenReturn(null);
    Mockito.when(mockResultSet.wasNull()).thenReturn(true);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final void mockHighPrecisionDecimalResult(BigDecimal value, int precision, int scale) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for high-precision Decimal)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.DECIMAL);
    Mockito.when(mockMetaData.getPrecision(1)).thenReturn(precision);
    Mockito.when(mockMetaData.getScale(1)).thenReturn(scale);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);

    Mockito.when(mockResultSet.getObject(1)).thenReturn(value);
    Mockito.when(mockResultSet.wasNull()).thenReturn(false);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final void mockIntegerDecimalResult(long value, int precision, int scale) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for Integer-as-Decimal)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.DECIMAL);
    Mockito.when(mockMetaData.getPrecision(1)).thenReturn(precision);
    Mockito.when(mockMetaData.getScale(1)).thenReturn(scale);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);

    // Real drivers return BigDecimal even for integer-like values (e.g., Oracle NUMBER(10)).
    // The value has scale 0; OverriddenConsumer may inflate the vector scale,
    // causing precision overflow when setScale pads trailing zeros.
    BigDecimal bdValue = BigDecimal.valueOf(value);
    Mockito.when(mockResultSet.getObject(1)).thenReturn(bdValue);
    Mockito.when(mockResultSet.wasNull()).thenReturn(false);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final void mockNullNumericResult(int precision, int scale) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for null Numeric)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.NUMERIC);
    Mockito.when(mockMetaData.getPrecision(1)).thenReturn(precision);
    Mockito.when(mockMetaData.getScale(1)).thenReturn(scale);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);

    Mockito.when(mockResultSet.getObject(1)).thenReturn(null);
    Mockito.when(mockResultSet.wasNull()).thenReturn(true);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final void mockNumericTypeResult(BigDecimal value, int precision, int scale) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for NUMERIC type)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.NUMERIC);
    Mockito.when(mockMetaData.getPrecision(1)).thenReturn(precision);
    Mockito.when(mockMetaData.getScale(1)).thenReturn(scale);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);

    Mockito.when(mockResultSet.getObject(1)).thenReturn(value);
    Mockito.when(mockResultSet.wasNull()).thenReturn(false);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final void mockDoubleDecimalResult(double value) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for other)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.DECIMAL);
    mockMetaData.getPrecision(10);
    mockMetaData.getScale(5);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);

    Double columnValue = Double.valueOf(value);
    Mockito.when(mockResultSet.getObject(1)).thenReturn(value);
    Mockito.when(mockResultSet.getBigDecimal(1)).thenReturn(BigDecimal.valueOf(value));
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final void mockBigIntResult(long value) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for bigint)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.BIGINT);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);

    Mockito.when(mockResultSet.getObject(1)).thenReturn(Long.valueOf(value));
    Mockito.when(mockResultSet.getLong(1)).thenReturn(value);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final void mockDoubleResult(double value) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for double)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.DOUBLE);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);

    Mockito.when(mockResultSet.getObject(1)).thenReturn(Double.valueOf(value));
    Mockito.when(mockResultSet.getDouble(1)).thenReturn(value);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final void mockDateResult(int year, int month, int day) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for date)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.DATE);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
    Calendar cal = Calendar.getInstance();
    cal.clear();
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month - 1);
    cal.set(Calendar.DAY_OF_MONTH, day);
    Date ancientDate = new Date(cal.getTime().getTime());
    LocalDate ancientLocalDate = LocalDate.of(year, month, day);
    Mockito.when(mockResultSet.getDate(1)).thenReturn(ancientDate);
    Mockito.when(mockResultSet.getObject(1, LocalDate.class)).thenReturn(ancientLocalDate);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final void mockType(String sqlTypesName) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for type " + sqlTypesName + ")");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    int sqlTypeCode = extractTypeCodeForName(sqlTypesName);
    mockGeneralResultSetMetaData(mockMetaData, sqlTypeCode);
    Object object;
    switch (sqlTypeCode) {
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.CLOB:
      case Types.LONGVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.SQLXML:
        object = "DummyString";
        Mockito.when(mockResultSet.getString(1)).thenReturn((String) object);
        break;
      case Types.BINARY:
      case Types.BLOB:
      case Types.LONGVARBINARY:
      case Types.VARBINARY:
        object = true;
        Mockito.when(mockResultSet.getBoolean(1)).thenReturn((Boolean) object);
        break;
      case Types.BOOLEAN:
      case Types.BIGINT:
      case Types.BIT:
      case Types.INTEGER:
      case Types.SMALLINT:
      case Types.TINYINT:
        object = 1;
        Mockito.when(mockResultSet.getInt(1)).thenReturn((Integer) object);
        break;
      case Types.DOUBLE:
      case Types.FLOAT:
      case Types.REAL:
        object = 0.0;
        Mockito.when(mockResultSet.getDouble(1)).thenReturn((Double) object);
        break;
      case Types.DECIMAL:
      case Types.NUMERIC:
        object = BigDecimal.valueOf(0.0);
        Mockito.when(mockResultSet.getBigDecimal(1)).thenReturn((BigDecimal) object);
        break;
      case Types.DATE:
        LocalDate localDate = LocalDate.parse("2000-01-01");
        Date date = Date.valueOf(localDate);
        object = localDate;
        Mockito.when(mockResultSet.getDate(1)).thenReturn(date);
        Mockito.when(mockResultSet.getObject(1, LocalDate.class)).thenReturn(localDate);
        break;
      case Types.TIME:
        LocalTime localTime = LocalTime.parse("08:20:45.60000");
        Time time = Time.valueOf(localTime);
        object = localTime;
        Mockito.when(mockResultSet.getObject(1, LocalTime.class)).thenReturn(localTime);
        Mockito.when(mockResultSet.getTime(1)).thenReturn(time);
        break;
      case Types.TIMESTAMP:
        LocalDateTime localDateTime = LocalDateTime.parse("2009-12-01T08:20:45");
        Timestamp timestamp = Timestamp.valueOf(localDateTime);
        object = localDateTime;
        Mockito.when(mockResultSet.getObject(1, LocalDateTime.class)).thenReturn(localDateTime);
        Mockito.when(mockResultSet.getTimestamp(1)).thenReturn(timestamp);
        break;
      default:
        object = "DummyObject";
        break;
    }
    Mockito.when(mockResultSet.getObject(1)).thenReturn(object);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  private List<Object[]> capturedSetObjectArgs;
  private List<Object[]> capturedSetNullArgs;

  /** Set up a PreparedStatement that captures all setObject() and setNull() calls.
   *  Rejects Arrow-stream binding to force the _set_stmt_parms_fallback path. */
  public final void mockSetObjectCapture() throws SQLException {
    capturedSetObjectArgs = new ArrayList<>();
    capturedSetNullArgs = new ArrayList<>();
    // Throw by default so Arrow primary path fails and fallback is triggered,
    // but allow setNull() through (needed for NULL parameter binding tests).
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class,
        invocation -> {
          if ("setNull".equals(invocation.getMethod().getName())) {
            return null;
          }
          throw new UnsupportedOperationException("mockSetObjectCapture");
        });
    Mockito.doReturn(true).when(mockPreparedStatement).execute();
    Mockito.doNothing().when(mockPreparedStatement).close();
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for setObject capture)");
    Mockito.doReturn(mockResultSet).when(mockPreparedStatement).getResultSet();
    Mockito.doReturn(false).when(mockResultSet).next();
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    Mockito.doReturn(0).when(mockMetaData).getColumnCount();
    Mockito.doReturn(mockMetaData).when(mockResultSet).getMetaData();
    Mockito.doAnswer(invocation -> {
      capturedSetObjectArgs.add(new Object[]{invocation.getArgument(0), invocation.getArgument(1)});
      return null;
    }).when(mockPreparedStatement).setObject(Mockito.anyInt(), Mockito.any());
    Mockito.doAnswer(invocation -> {
      capturedSetNullArgs.add(new Object[]{invocation.getArgument(0), invocation.getArgument(1)});
      return null;
    }).when(mockPreparedStatement).setNull(Mockito.anyInt(), Mockito.anyInt());
    Mockito.doReturn(mockPreparedStatement).when(this).prepareStatement(Mockito.any());
  }

  public final void mockColumnAlias(String columnName, String columnLabel) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for column alias)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.VARCHAR);
    // Override with different column name and label
    Mockito.when(mockMetaData.getColumnName(1)).thenReturn(columnName);
    Mockito.when(mockMetaData.getColumnLabel(1)).thenReturn(columnLabel);
    Mockito.when(mockResultSet.getObject(1)).thenReturn("DummyString");
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final List<Object[]> getCapturedSetObjectArgs() {
    return capturedSetObjectArgs;
  }

  public final List<Object[]> getCapturedSetNullArgs() {
    return capturedSetNullArgs;
  }


  public final void mockTimestampResult(LocalDateTime localDateTime) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(for timestamp)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    mockGeneralResultSetMetaData(mockMetaData, Types.TIMESTAMP);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
    Timestamp timestamp = Timestamp.valueOf(localDateTime);
    Mockito.when(mockResultSet.getObject(1, LocalDateTime.class)).thenReturn(localDateTime);
    Mockito.when(mockResultSet.getTimestamp(1)).thenReturn(timestamp);
    Mockito.when(mockResultSet.getObject(1)).thenReturn(localDateTime);
    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }

  public final ResultSet verifyResultSet() {
    return Mockito.verify(mockResultSet);
  }

  /** Set up a multi-column mock result for testing mixed-type queries.
   *  @param sqlTypes  JDBC type codes for each column
   *  @param values    Java objects to return for each column (via getObject)
   */
  public final void mockMultiColumnResult(int[] sqlTypes, Object[] values) throws SQLException {
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.execute()).thenReturn(true);
    mockResultSet = Mockito.mock(ResultSet.class, "ResultSet(multi-column)");
    Mockito.when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.next()).thenReturn(true);

    ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
    int colCount = sqlTypes.length;
    Mockito.when(mockMetaData.getColumnCount()).thenReturn(colCount);
    for (int i = 0; i < colCount; i++) {
      int col = i + 1; // JDBC is 1-based
      Mockito.when(mockMetaData.getColumnType(col)).thenReturn(sqlTypes[i]);
      Mockito.when(mockMetaData.getColumnTypeName(col)).thenReturn(JDBCType.valueOf(sqlTypes[i]).getName());
      Mockito.when(mockMetaData.getColumnName(col)).thenReturn("col_" + col);
      Mockito.when(mockMetaData.getColumnLabel(col)).thenReturn("col_" + col);
      Mockito.when(mockMetaData.isNullable(col)).thenReturn(mockMetaData.columnNullableUnknown);
      Mockito.when(mockMetaData.getPrecision(col)).thenReturn(0);
      Mockito.when(mockMetaData.getScale(col)).thenReturn(0);
    }
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);

    for (int i = 0; i < colCount; i++) {
      int col = i + 1;
      Object value = values[i];
      Mockito.when(mockResultSet.getObject(col)).thenReturn(value);
      // Mock type-specific getters that the Arrow JDBC consumer calls.
      // Use Number to handle both Integer and Long (JPype passes Python int as Long).
      if (value instanceof String) {
        Mockito.when(mockResultSet.getString(col)).thenReturn((String) value);
      } else if (value instanceof Number) {
        Number num = (Number) value;
        Mockito.when(mockResultSet.getInt(col)).thenReturn(num.intValue());
        Mockito.when(mockResultSet.getLong(col)).thenReturn(num.longValue());
        Mockito.when(mockResultSet.getDouble(col)).thenReturn(num.doubleValue());
      } else if (value instanceof BigDecimal) {
        Mockito.when(mockResultSet.getBigDecimal(col)).thenReturn((BigDecimal) value);
      } else if (value instanceof Boolean) {
        Mockito.when(mockResultSet.getBoolean(col)).thenReturn((Boolean) value);
      }
    }

    Mockito.when(this.prepareStatement(Mockito.any())).thenReturn(mockPreparedStatement);
  }
}

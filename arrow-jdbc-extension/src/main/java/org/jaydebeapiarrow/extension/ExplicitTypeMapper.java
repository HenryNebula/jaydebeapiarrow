package org.jaydebeapiarrow.extension;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import com.jakewharton.fliptables.FlipTable;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;

public class ExplicitTypeMapper {

    private static final Logger logger = Logger.getLogger(ExplicitTypeMapper.class.getName());
    private int defaultDecimalPrecision = 38;
    private int defaultDecimalScale = 17;

    public ExplicitTypeMapper() {
    }

    public ExplicitTypeMapper(int defaultDecimalPrecision, int defaultDecimalScale) {
        this.defaultDecimalScale = defaultDecimalScale;
        this.defaultDecimalPrecision = defaultDecimalPrecision;
    }


    static Map<Integer, List<Integer>> parseMetaData(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        List<String[]> tabularMetaData = new ArrayList<>();
        Map<Integer, List<Integer>> parsedMetaData = new HashMap<>();

        String[] headers = {
                "columnName",
                "columnTypeName",
                "inferredColumnTypeName",
                "columnNullable",
        };

        for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
            int columnType = metaData.getColumnType(columnIndex);
            String columnName = metaData.getColumnName(columnIndex);
            String columnTypeName = metaData.getColumnTypeName(columnIndex);
            String inferredColumnTypeName;
            try {
                inferredColumnTypeName = JDBCType.valueOf(columnType).getName();
            } catch (IllegalArgumentException e) {
                inferredColumnTypeName = columnTypeName;
            }
            int columnNullable = metaData.isNullable(columnIndex);

            String[] columnMetaData = {
                    columnName,
                    columnTypeName,
                    inferredColumnTypeName,
                    ((Integer) columnNullable).toString(),
            };
            tabularMetaData.add(columnMetaData);

            List<Integer> columnsWithSameType = parsedMetaData.getOrDefault(columnType, new ArrayList<Integer>());
            columnsWithSameType.add(columnIndex);
            parsedMetaData.put(columnType, columnsWithSameType);
        }

        String[][] columnMetaDataArray = new String[tabularMetaData.size()][];
        logger.fine("\n" + FlipTable.of(
                headers,
                tabularMetaData.toArray(columnMetaDataArray)
        ));

        return parsedMetaData;
    }

    private JdbcFieldInfo createDefaultDecimalFieldInfo(int precision, int scale) {
        if (precision < 1) {
            return new JdbcFieldInfo(
                    Types.DECIMAL,
                    defaultDecimalPrecision,
                    defaultDecimalScale
                    );
        }
        else {
            return new JdbcFieldInfo(
                    Types.DECIMAL,
                    precision,
                    scale
            );
        }
    }

    public Map<Integer, JdbcFieldInfo> createExplicitTypeMapping(ResultSet resultSet) throws SQLException {
        Map<Integer, List<Integer>> parsedMetaData = parseMetaData(resultSet);

        Map<Integer, JdbcFieldInfo> explicitMapping = new HashMap<>();

        /* correctly marked as Decimal */
        List<Integer> decimalColumnIndices = parsedMetaData.getOrDefault(Types.DECIMAL, new ArrayList<>());
        decimalColumnIndices.addAll(parsedMetaData.getOrDefault(Types.NUMERIC, new ArrayList<>()));

        /* inferred as Decimal */
        for (int columnIndex: parsedMetaData.getOrDefault(Types.INTEGER, new ArrayList<>())) {
            if (resultSet.getMetaData().getColumnName(columnIndex).contains("DECIMAL")) {
                logger.fine(String.format("Inferred column %1s (%2s) as a Decimal", columnIndex, resultSet.getMetaData().getColumnName(columnIndex)));
                decimalColumnIndices.add(columnIndex);
            }
        }

        /* Detect columns whose JDBC type code is not recognized by the standard
         * JDBCType enum. Use column type NAME matching instead of hardcoded type
         * codes so this works driver-agnostically.
         * Known cases: Oracle reports BINARY_DOUBLE as type 101, and
         * TIMESTAMP WITH TIME ZONE as type 101 (ojdbc8) or 2013 (ojdbc11). */
        for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
            int columnType = resultSet.getMetaData().getColumnType(columnIndex);
            String columnTypeName = resultSet.getMetaData().getColumnTypeName(columnIndex);
            try {
                JDBCType.valueOf(columnType);
            } catch (IllegalArgumentException e) {
                String upperTypeName = columnTypeName.toUpperCase();
                if (upperTypeName.contains("BINARY_DOUBLE")) {
                    explicitMapping.put(columnIndex, new JdbcFieldInfo(Types.DOUBLE));
                    logger.fine(String.format(
                            "Detected column %1s (%2s) as DOUBLE from type name '%3s' (JDBC type %4$s)",
                            columnIndex, resultSet.getMetaData().getColumnName(columnIndex),
                            columnTypeName, columnType));
                } else if (upperTypeName.contains("TIMESTAMP") && upperTypeName.contains("TIME ZONE")) {
                    explicitMapping.put(columnIndex, new JdbcFieldInfo(Types.TIMESTAMP_WITH_TIMEZONE));
                    logger.fine(String.format(
                            "Detected column %1s (%2s) as TIMESTAMP_WITH_TIMEZONE from type name '%3s' (JDBC type %4$s)",
                            columnIndex, resultSet.getMetaData().getColumnName(columnIndex),
                            columnTypeName, columnType));
                } else if (upperTypeName.contains("TIMESTAMP")) {
                    explicitMapping.put(columnIndex, new JdbcFieldInfo(Types.TIMESTAMP));
                    logger.fine(String.format(
                            "Detected column %1s (%2s) as TIMESTAMP from type name '%3s' (JDBC type %4$s)",
                            columnIndex, resultSet.getMetaData().getColumnName(columnIndex),
                            columnTypeName, columnType));
                }
            }
        }

        /* Detect TIMESTAMPTZ columns (e.g., PostgreSQL reports them as Types.TIMESTAMP) */
        List<Integer> timestamptzColumnIndices = new ArrayList<>();
        for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
            int columnType = resultSet.getMetaData().getColumnType(columnIndex);
            String columnTypeName = resultSet.getMetaData().getColumnTypeName(columnIndex);
            if (columnType == Types.TIMESTAMP && "timestamptz".equalsIgnoreCase(columnTypeName)) {
                timestamptzColumnIndices.add(columnIndex);
                logger.fine(String.format("Detected column %1s (%2s) as TIMESTAMPTZ, overriding to TIMESTAMP_WITH_TIMEZONE",
                        columnIndex, resultSet.getMetaData().getColumnName(columnIndex)));
            }
        }
        for (int columnIndex : timestamptzColumnIndices) {
            explicitMapping.put(columnIndex, new JdbcFieldInfo(Types.TIMESTAMP_WITH_TIMEZONE));
        }

        /* Detect TIME columns misreported as VARCHAR (e.g., SQLite JDBC) */
        for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
            int columnType = resultSet.getMetaData().getColumnType(columnIndex);
            String columnTypeName = resultSet.getMetaData().getColumnTypeName(columnIndex);
            if (columnType == Types.VARCHAR && "TIME".equalsIgnoreCase(columnTypeName)) {
                explicitMapping.put(columnIndex, new JdbcFieldInfo(Types.TIME));
                logger.fine(String.format("Detected column %1s (%2s) as TIME (was reported as VARCHAR)",
                        columnIndex, resultSet.getMetaData().getColumnName(columnIndex)));
            }
        }

        /* Detect JSON/JSONB/UUID columns reported as Types.OTHER (e.g., PostgreSQL).
         * Map to VARCHAR so they are read as strings via the default Arrow path. */
        for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
            int columnType = resultSet.getMetaData().getColumnType(columnIndex);
            String columnTypeName = resultSet.getMetaData().getColumnTypeName(columnIndex);
            if (columnType == Types.OTHER) {
                String upperTypeName = columnTypeName.toUpperCase();
                if (upperTypeName.contains("JSON") || upperTypeName.contains("UUID")) {
                    explicitMapping.put(columnIndex, new JdbcFieldInfo(Types.VARCHAR));
                    logger.fine(String.format(
                            "Detected column %1s (%2s) as VARCHAR from type name '%3s' (JDBC type OTHER)",
                            columnIndex, resultSet.getMetaData().getColumnName(columnIndex),
                            columnTypeName));
                }
            }
        }

        /* Detect ARRAY columns - not natively supported by Arrow JDBC adapter.
         * Map to VARCHAR as a degraded fallback (toString representation). */
        List<Integer> arrayColumnIndices = parsedMetaData.getOrDefault(Types.ARRAY, new ArrayList<>());
        for (int columnIndex : arrayColumnIndices) {
            explicitMapping.put(columnIndex, new JdbcFieldInfo(Types.VARCHAR));
            logger.warning(String.format(
                    "Column %1s (%2s) is ARRAY type, which is not natively supported. "
                    + "Falling back to VARCHAR (toString representation).",
                    columnIndex, resultSet.getMetaData().getColumnName(columnIndex)));
        }

        for (int columnIndex: decimalColumnIndices) {
            int precision = resultSet.getMetaData().getPrecision(columnIndex);
            int scale = resultSet.getMetaData().getScale(columnIndex);
            String columnName = resultSet.getMetaData().getColumnName(columnIndex);
            JdbcFieldInfo decimalFieldInfo = createDefaultDecimalFieldInfo(precision, scale);
            explicitMapping.put(columnIndex, decimalFieldInfo);
            logger.fine(String.format("Detected column %1s (%2s) as a Decimal: (%3s, %4s) -> (%5s, %6s)",
                    columnIndex, columnName, precision, scale,
                    decimalFieldInfo.getPrecision(), decimalFieldInfo.getScale()
                    )
            );
        }

        return explicitMapping;
    }

}

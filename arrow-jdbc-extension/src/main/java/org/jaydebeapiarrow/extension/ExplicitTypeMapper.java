package org.jaydebeapiarrow.extension;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import com.jakewharton.fliptables.FlipTable;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;

public class ExplicitTypeMapper {

    private static final Logger logger = Logger.getLogger(ExplicitTypeMapper.class.getName());


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
            // No declared precision (unbounded numeric): transfer as strings.
            return new JdbcFieldInfo(Types.VARCHAR);
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

        /* Map Types.OTHER columns to VARCHAR (e.g., PostgreSQL inet/interval/uuid/json,
         * HSQLDB OTHER/OBJECT columns). Arrow's JDBC adapter has no native representation
         * for OTHER, and getString() is the JDBC-sanctioned universal fallback, so every
         * OTHER column is read as a string regardless of its type name. Structured types
         * (STRUCT/REF/JAVA_OBJECT/DISTINCT/REF_CURSOR) are NOT covered and still fail —
         * stringifying a composite silently would be worse than a clear error. */
        for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
            int columnType = resultSet.getMetaData().getColumnType(columnIndex);
            String columnTypeName = resultSet.getMetaData().getColumnTypeName(columnIndex);
            if (columnType == Types.OTHER) {
                explicitMapping.put(columnIndex, new JdbcFieldInfo(Types.VARCHAR));
                logger.fine(String.format(
                        "Column %1s (%2s) has JDBC type OTHER ('%3s'), mapping to VARCHAR for string retrieval.",
                        columnIndex, resultSet.getMetaData().getColumnName(columnIndex),
                        columnTypeName));
            }
        }

        /* Structured types have no flat representation: stringifying a
         * composite silently would be worse than an error. Fail fast, at
         * iterator-creation time, with the column and type named, instead
         * of Arrow's generic "Unmapped JDBC type" / "No consumer for Arrow
         * type" from deep inside the adapter (#153). */
        int[] structuredTypes = {
                Types.STRUCT, Types.JAVA_OBJECT, Types.DISTINCT, Types.REF, Types.REF_CURSOR};
        for (int structuredType : structuredTypes) {
            for (int columnIndex : parsedMetaData.getOrDefault(structuredType, new ArrayList<>())) {
                String columnName = resultSet.getMetaData().getColumnName(columnIndex);
                String typeName = resultSet.getMetaData().getColumnTypeName(columnIndex);
                String typeDetail = (typeName == null || typeName.isEmpty())
                        ? "" : String.format(", type name '%s'", typeName);
                throw new SQLException(String.format(
                        "Column %d (%s) has JDBC type %s (%d%s), which has no flat "
                        + "Arrow representation and is not auto-converted. "
                        + "Cast the column to text in SQL to fetch it as a string.",
                        columnIndex, columnName,
                        JDBCType.valueOf(structuredType).getName(), structuredType, typeDetail));
            }
        }

        /* Map flat types with no native Arrow representation to VARCHAR so they are
         * read as strings via getString():
         * - SQLXML (2009): document text
         * - ROWID (-8): e.g., Oracle ROWID pseudo-columns
         * - NCLOB (2011): national-character LOBs (CLOB is handled natively)
         * - TIME_WITH_TIMEZONE (2013): no Arrow time-with-zone type
         * - DATALINK (70): URL text */
        int[] stringFallbackTypes = {
                Types.SQLXML, Types.ROWID, Types.NCLOB, Types.TIME_WITH_TIMEZONE, Types.DATALINK};
        for (int fallbackType : stringFallbackTypes) {
            for (int columnIndex : parsedMetaData.getOrDefault(fallbackType, new ArrayList<>())) {
                explicitMapping.put(columnIndex, new JdbcFieldInfo(Types.VARCHAR));
                logger.fine(String.format(
                        "Column %1s (%2s) is %3s type, mapping to VARCHAR for string retrieval.",
                        columnIndex, resultSet.getMetaData().getColumnName(columnIndex),
                        JDBCType.valueOf(fallbackType).getName()));
            }
        }

        /* ARRAY columns are now read natively via the C Data Interface.
         * Element type mapping is handled by createArraySubTypeMapping(). */

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

    /**
     * Build element-type mappings for ARRAY columns so the upstream Arrow JDBC
     * adapter can create the correct ListVector child vectors.
     * Called separately from createExplicitTypeMapping because the Arrow config
     * builder consumes these via a dedicated setter.
     */
    public Map<Integer, JdbcFieldInfo> createArraySubTypeMapping(ResultSet resultSet) throws SQLException {
        Map<Integer, JdbcFieldInfo> arraySubTypes = new HashMap<>();
        ResultSetMetaData metaData = resultSet.getMetaData();

        for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
            if (metaData.getColumnType(columnIndex) != Types.ARRAY) {
                continue;
            }

            String typeName = metaData.getColumnTypeName(columnIndex);
            JdbcFieldInfo elementFieldInfo = inferElementJdbcType(typeName);
            arraySubTypes.put(columnIndex, elementFieldInfo);

            logger.fine(String.format(
                    "ARRAY column %d (%s) element type inferred as %s from type name '%s'",
                    columnIndex, metaData.getColumnName(columnIndex),
                    JDBCType.valueOf(elementFieldInfo.getJdbcType()).getName(), typeName));
        }
        return arraySubTypes;
    }

    /**
     * Infer the JDBC element type of a SQL ARRAY column from its type name.
     * SQL ARRAYs are homogeneous, so one element type describes the whole column.
     * Falls back to VARCHAR when the type name is unrecognized — getString() works
     * for any JDBC type, so elements come through as strings.
     */
    /*package*/ JdbcFieldInfo inferElementJdbcType(String columnTypeName) {
        String upper = columnTypeName.toUpperCase().trim();

        // PostgreSQL: _int4, _int8, _float4, _float8, _varchar, _text, _bool
        // HSQLDB:     INTEGER ARRAY, VARCHAR ARRAY, etc.
        // Generic:    INT[], VARCHAR[], TIMESTAMP[], etc.

        if (upper.equals("_INT4") || upper.equals("_INT") || upper.contains("INTEGER ARRAY")
                || upper.equals("INT[]") || upper.equals("INT ARRAY")) {
            return new JdbcFieldInfo(Types.INTEGER);
        }
        if (upper.equals("_INT8") || upper.contains("BIGINT ARRAY")
                || upper.equals("BIGINT[]") || upper.equals("BIGINT ARRAY")) {
            return new JdbcFieldInfo(Types.BIGINT);
        }
        if (upper.equals("_FLOAT4") || upper.contains("REAL ARRAY")
                || upper.equals("REAL[]") || upper.equals("FLOAT[]")) {
            return new JdbcFieldInfo(Types.REAL);
        }
        if (upper.equals("_FLOAT8") || upper.contains("DOUBLE PRECISION ARRAY")
                || upper.contains("DOUBLE ARRAY") || upper.equals("DOUBLE[]")) {
            return new JdbcFieldInfo(Types.DOUBLE);
        }
        if (upper.equals("_BOOL") || upper.contains("BOOLEAN ARRAY")
                || upper.equals("BOOLEAN[]")) {
            return new JdbcFieldInfo(Types.BOOLEAN);
        }
        if (upper.equals("_VARCHAR") || upper.equals("_TEXT")
                || upper.contains("CHARACTER VARYING ARRAY")
                || upper.contains("VARCHAR ARRAY") || upper.contains("VARCHAR[]")
                || upper.contains("TEXT ARRAY") || upper.contains("TEXT[]")) {
            return new JdbcFieldInfo(Types.VARCHAR);
        }
        if (upper.contains("DECIMAL ARRAY") || upper.contains("NUMERIC ARRAY")
                || upper.equals("DECIMAL[]") || upper.equals("NUMERIC[]")
                || upper.equals("_NUMERIC") || upper.equals("_DECIMAL")) {
            // Element precision/scale is unknowable from ResultSetMetaData
            // for ARRAY columns, so decimal elements are always unbounded
            // here: transfer them as strings, mirroring the unbounded
            // top-level policy in createDefaultDecimalFieldInfo(). A bare
            // JdbcFieldInfo(Types.DECIMAL) would also throw — DECIMAL
            // requires precision and scale. See issue #121.
            return new JdbcFieldInfo(Types.VARCHAR);
        }
        if (upper.contains("DATE ARRAY") || upper.equals("DATE[]")) {
            return new JdbcFieldInfo(Types.DATE);
        }
        if (upper.contains("TIMESTAMP WITH TIME ZONE ARRAY")
                || upper.equals("TIMESTAMPTZ[]")) {
            return new JdbcFieldInfo(Types.TIMESTAMP_WITH_TIMEZONE);
        }
        if (upper.contains("TIMESTAMP ARRAY") || upper.equals("TIMESTAMP[]")
                || upper.contains("TIMESTAMP WITHOUT TIME ZONE ARRAY")) {
            return new JdbcFieldInfo(Types.TIMESTAMP);
        }

        logger.warning(String.format(
                "Unknown ARRAY element type name '%s', defaulting to VARCHAR element type. "
                + "Array elements will be returned as strings.",
                columnTypeName));
        return new JdbcFieldInfo(Types.VARCHAR);
    }

}

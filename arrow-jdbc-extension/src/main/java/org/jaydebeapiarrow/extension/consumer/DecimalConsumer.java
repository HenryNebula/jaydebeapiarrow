/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jaydebeapiarrow.extension.consumer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.adapter.jdbc.consumer.JdbcConsumer;
import org.apache.arrow.vector.DecimalVector;

/**
 * Custom DecimalConsumer that handles JDBC drivers (like SQLite) which return
 * Double or Integer from getBigDecimal() instead of a proper BigDecimal.
 *
 * The default Arrow DecimalConsumer calls rs.getBigDecimal() directly, which
 * for SQLite returns a BigDecimal with the exact binary representation of the
 * double (e.g., 12.4 becomes 12.4000000000000003552713678800500929355621337890625)
 * with precision far exceeding the Arrow DecimalVector capacity (38 digits).
 *
 * This consumer normalizes values via BigDecimal.valueOf() to get a clean
 * decimal representation before setting the scale to match the vector.
 */
public class DecimalConsumer {

    public static JdbcConsumer<DecimalVector> createConsumer(
            DecimalVector vector, int index, boolean nullable, RoundingMode roundingMode) {
        return createConsumer(vector, index, nullable, roundingMode, vector.getScale(), 38);
    }

    public static JdbcConsumer<DecimalVector> createConsumer(
            DecimalVector vector, int index, boolean nullable, RoundingMode roundingMode, int scale, int precision) {
        System.err.println("[DEBUG DecimalConsumer.createConsumer] vector.scale=" + vector.getScale() + ", vector.precision=" + vector.getPrecision() + ", scale=" + scale + ", precision=" + precision + ", nullable=" + nullable);
        if (nullable) {
            return new NullableDecimalConsumer(vector, index, roundingMode, scale, precision);
        } else {
            return new NonNullableDecimalConsumer(vector, index, roundingMode, scale, precision);
        }
    }

    static class NullableDecimalConsumer implements JdbcConsumer<DecimalVector> {

        private final RoundingMode roundingMode;
        private final int scale;
        private final int precision;
        private final int columnIndexInResultSet;
        private DecimalVector vector;
        private int currentIndex;

        public NullableDecimalConsumer(DecimalVector vector, int index, RoundingMode roundingMode, int scale, int precision) {
            this.vector = vector;
            this.columnIndexInResultSet = index;
            this.roundingMode = roundingMode;
            this.scale = scale;
            this.precision = precision;
        }

        @Override
        public void consume(ResultSet resultSet) throws SQLException {
            try {
                BigDecimal bd = getCleanBigDecimal(resultSet, columnIndexInResultSet);
                if (!resultSet.wasNull()) {
                    System.err.println("[DEBUG NullableDecimalConsumer] bd=" + bd + ", bd.precision()=" + bd.precision() + ", bd.scale()=" + bd.scale() + ", targetScale=" + scale + ", targetPrecision=" + precision);
                    bd = bd.setScale(scale, roundingMode);
                    System.err.println("[DEBUG NullableDecimalConsumer] after setScale: bd=" + bd + ", bd.precision()=" + bd.precision() + ", bd.scale()=" + bd.scale());
                    validateDecimalFitsVector(bd, precision);
                    vector.set(currentIndex, bd);
                }
            } catch (ArithmeticException | IllegalArgumentException e) {
                System.err.println("[DEBUG NullableDecimalConsumer] caught exception: " + e.getClass().getName() + ": " + e.getMessage());
                throw createDecimalConversionException(e, currentIndex, columnIndexInResultSet, precision, scale);
            }
            currentIndex++;
        }

        @Override
        public void resetValueVector(DecimalVector vector) {
            this.vector = vector;
            this.currentIndex = 0;
        }

        @Override
        public void close() {
        }
    }

    static class NonNullableDecimalConsumer implements JdbcConsumer<DecimalVector> {

        private final RoundingMode roundingMode;
        private final int scale;
        private final int precision;
        private final int columnIndexInResultSet;
        private DecimalVector vector;
        private int currentIndex;

        public NonNullableDecimalConsumer(DecimalVector vector, int index, RoundingMode roundingMode, int scale, int precision) {
            this.vector = vector;
            this.columnIndexInResultSet = index;
            this.roundingMode = roundingMode;
            this.scale = scale;
            this.precision = precision;
        }

        @Override
        public void consume(ResultSet resultSet) throws SQLException {
            try {
                BigDecimal bd = getCleanBigDecimal(resultSet, columnIndexInResultSet);
                System.err.println("[DEBUG NonNullableDecimalConsumer] bd=" + bd + ", bd.precision()=" + bd.precision() + ", bd.scale()=" + bd.scale() + ", targetScale=" + scale + ", targetPrecision=" + precision);
                bd = bd.setScale(scale, roundingMode);
                System.err.println("[DEBUG NonNullableDecimalConsumer] after setScale: bd=" + bd + ", bd.precision()=" + bd.precision() + ", bd.scale()=" + bd.scale());
                validateDecimalFitsVector(bd, precision);
                vector.set(currentIndex, bd);
            } catch (ArithmeticException | IllegalArgumentException e) {
                System.err.println("[DEBUG NonNullableDecimalConsumer] caught exception: " + e.getClass().getName() + ": " + e.getMessage());
                throw createDecimalConversionException(e, currentIndex, columnIndexInResultSet, precision, scale);
            }
            currentIndex++;
        }

        @Override
        public void resetValueVector(DecimalVector vector) {
            this.vector = vector;
            this.currentIndex = 0;
        }

        @Override
        public void close() {
        }
    }

    private static SQLException createDecimalConversionException(
            RuntimeException cause, int rowIndex, int columnIndex, int precision, int scale) {
        return new SQLException(String.format(
                "Could not convert DECIMAL/NUMERIC value at row %d, column %d to Arrow DECIMAL(%d, %d). " +
                "The value may exceed Arrow decimal precision or require a different scale. " +
                "Cast the column in SQL to a supported DECIMAL/NUMERIC precision and scale, " +
                "for example CAST(column AS DECIMAL(38, %d)), or cast it to VARCHAR to preserve the exact value as text. " +
                "Cause: %s",
                rowIndex, columnIndex, precision, scale, scale,
                cause.getMessage()),
                cause);
    }

    private static void validateDecimalFitsVector(BigDecimal bd, int precision) {
        if (bd.precision() > precision) {
            throw new IllegalArgumentException(String.format(
                    "value precision %d exceeds Arrow decimal precision %d",
                    bd.precision(), precision));
        }
    }

    /**
     * Retrieves a BigDecimal from the ResultSet, normalizing the value if the
     * JDBC driver returns a Double or Integer instead of a BigDecimal.
     */
    static BigDecimal getCleanBigDecimal(ResultSet resultSet, int columnIndex) throws SQLException {
        Object obj = resultSet.getObject(columnIndex);
        if (obj == null) {
            return null;
        }
        if (obj instanceof BigDecimal) {
            return (BigDecimal) obj;
        }
        // For drivers like SQLite that return Double/Integer for DECIMAL columns,
        // use BigDecimal.valueOf() for a clean representation instead of the
        // exact binary expansion from new BigDecimal(double).
        if (obj instanceof Double || obj instanceof Float) {
            return BigDecimal.valueOf(((Number) obj).doubleValue());
        }
        if (obj instanceof Number) {
            return BigDecimal.valueOf(((Number) obj).longValue());
        }
        return new BigDecimal(obj.toString());
    }
}

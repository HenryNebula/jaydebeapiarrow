/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
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
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;

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
 *
 * Supports both decimal128 (DecimalVector, up to 38 digits) and decimal256
 * (Decimal256Vector, up to 76 digits) targets.
 */
public class DecimalConsumer {

    /** Maximum precision representable by a 16-byte decimal128 vector. */
    public static final int DECIMAL128_MAX_PRECISION = 38;
    /** Maximum precision representable by a 32-byte decimal256 vector. */
    public static final int DECIMAL256_MAX_PRECISION = 76;

    /** Writes a BigDecimal value into either a DecimalVector or Decimal256Vector. */
    private interface DecimalWriter {
        void set(int index, BigDecimal value);
    }

    public static JdbcConsumer<FieldVector> createConsumer(
            FieldVector vector, int index, boolean nullable, RoundingMode roundingMode,
            int scale, int precision) {
        DecimalWriter writer = writerFor(vector);
        // Cap at what the vector holds so oversized values fail below with
        // an actionable error instead of a raw DecimalUtility exception.
        int effectivePrecision = Math.min(precision, maxPrecision(vector));
        if (nullable) {
            return new NullableDecimalConsumer(writer, index, roundingMode, scale, effectivePrecision);
        } else {
            return new NonNullableDecimalConsumer(writer, index, roundingMode, scale, effectivePrecision);
        }
    }

    static class NullableDecimalConsumer implements JdbcConsumer<FieldVector> {

        private final RoundingMode roundingMode;
        private final int scale;
        private final int precision;
        private final int columnIndexInResultSet;
        private DecimalWriter writer;
        private int currentIndex;

        public NullableDecimalConsumer(DecimalWriter writer, int index, RoundingMode roundingMode, int scale, int precision) {
            this.writer = writer;
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
                    bd = bd.setScale(scale, roundingMode);
                    validateDecimalFitsVector(bd, precision);
                    writer.set(currentIndex, bd);
                }
            } catch (ArithmeticException | IllegalArgumentException | UnsupportedOperationException e) {
                throw createDecimalConversionException(e, currentIndex, columnIndexInResultSet, precision, scale);
            }
            currentIndex++;
        }

        @Override
        public void resetValueVector(FieldVector vector) {
            this.writer = writerFor(vector);
            this.currentIndex = 0;
        }

        @Override
        public void close() {
        }
    }

    static class NonNullableDecimalConsumer implements JdbcConsumer<FieldVector> {

        private final RoundingMode roundingMode;
        private final int scale;
        private final int precision;
        private final int columnIndexInResultSet;
        private DecimalWriter writer;
        private int currentIndex;

        public NonNullableDecimalConsumer(DecimalWriter writer, int index, RoundingMode roundingMode, int scale, int precision) {
            this.writer = writer;
            this.columnIndexInResultSet = index;
            this.roundingMode = roundingMode;
            this.scale = scale;
            this.precision = precision;
        }

        @Override
        public void consume(ResultSet resultSet) throws SQLException {
            try {
                BigDecimal bd = getCleanBigDecimal(resultSet, columnIndexInResultSet);
                bd = bd.setScale(scale, roundingMode);
                validateDecimalFitsVector(bd, precision);
                writer.set(currentIndex, bd);
            } catch (ArithmeticException | IllegalArgumentException | UnsupportedOperationException e) {
                throw createDecimalConversionException(e, currentIndex, columnIndexInResultSet, precision, scale);
            }
            currentIndex++;
        }

        @Override
        public void resetValueVector(FieldVector vector) {
            this.writer = writerFor(vector);
            this.currentIndex = 0;
        }

        @Override
        public void close() {
        }
    }

    private static DecimalWriter writerFor(FieldVector vector) {
        if (vector instanceof Decimal256Vector) {
            final Decimal256Vector decimal256Vector = (Decimal256Vector) vector;
            return new DecimalWriter() {
                @Override
                public void set(int index, BigDecimal value) {
                    decimal256Vector.set(index, value);
                }
            };
        }
        if (vector instanceof DecimalVector) {
            final DecimalVector decimalVector = (DecimalVector) vector;
            return new DecimalWriter() {
                @Override
                public void set(int index, BigDecimal value) {
                    decimalVector.set(index, value);
                }
            };
        }
        throw new IllegalArgumentException(
                "Unsupported decimal vector type: " + vector.getClass().getName());
    }

    private static int maxPrecision(FieldVector vector) {
        return vector instanceof Decimal256Vector ? DECIMAL256_MAX_PRECISION : DECIMAL128_MAX_PRECISION;
    }

    private static SQLException createDecimalConversionException(
            RuntimeException cause, int rowIndex, int columnIndex, int precision, int scale) {
        return new SQLException(String.format(
                "Could not convert DECIMAL/NUMERIC value at row %d, column %d to Arrow DECIMAL(%d, %d). " +
                "The value may exceed Arrow decimal precision or require a different scale. " +
                "Cast the column in SQL to a supported DECIMAL/NUMERIC precision and scale, " +
                "for example CAST(column AS DECIMAL(%d, %d)), or cast it to VARCHAR to preserve the exact value as text. " +
                "Cause: %s",
                rowIndex, columnIndex, precision, scale, precision, scale,
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

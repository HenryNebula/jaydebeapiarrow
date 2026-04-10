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

import org.apache.arrow.adapter.jdbc.consumer.BaseConsumer;
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
        if (nullable) {
            return new NullableDecimalConsumer(vector, index, roundingMode);
        } else {
            return new NonNullableDecimalConsumer(vector, index, roundingMode);
        }
    }

    static class NullableDecimalConsumer extends BaseConsumer<DecimalVector> {

        private final RoundingMode roundingMode;

        public NullableDecimalConsumer(DecimalVector vector, int index, RoundingMode roundingMode) {
            super(vector, index);
            this.roundingMode = roundingMode;
        }

        @Override
        public void consume(ResultSet resultSet) throws SQLException {
            BigDecimal bd = getCleanBigDecimal(resultSet, columnIndexInResultSet);
            if (!resultSet.wasNull()) {
                bd = bd.setScale(vector.getScale(), roundingMode);
                vector.set(currentIndex, bd);
            }
            currentIndex++;
        }
    }

    static class NonNullableDecimalConsumer extends BaseConsumer<DecimalVector> {

        private final RoundingMode roundingMode;

        public NonNullableDecimalConsumer(DecimalVector vector, int index, RoundingMode roundingMode) {
            super(vector, index);
            this.roundingMode = roundingMode;
        }

        @Override
        public void consume(ResultSet resultSet) throws SQLException {
            BigDecimal bd = getCleanBigDecimal(resultSet, columnIndexInResultSet);
            bd = bd.setScale(vector.getScale(), roundingMode);
            vector.set(currentIndex, bd);
            currentIndex++;
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

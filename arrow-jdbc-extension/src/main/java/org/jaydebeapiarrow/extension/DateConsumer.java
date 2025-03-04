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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jaydebeapiarrow.extension;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.arrow.adapter.jdbc.consumer.BaseConsumer;
import org.apache.arrow.adapter.jdbc.consumer.JdbcConsumer;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;

public class DateConsumer {

    private static final Logger logger = Logger.getLogger(ExplicitTypeMapper.class.getName());

    /**
     * Creates a consumer for {@link DateMilliVector}.
     */
    public static JdbcConsumer<DateDayVector> createConsumer(
            DateDayVector vector, int index, boolean nullable, Calendar calendar) {
        if (nullable) {
            return new NullableDateConsumer(vector, index, calendar);
        } else {
            return new NonNullableDateConsumer(vector, index, calendar);
        }
    }

    /**
     * Nullable consumer for date.
     */
    static class NullableDateConsumer extends BaseConsumer<DateDayVector> {

        protected final Calendar calendar;

        /**
         * Instantiate a DateConsumer.
         */
        public NullableDateConsumer(DateDayVector vector, int index) {
            this(vector, index, /* calendar */null);
        }

        /**
         * Instantiate a DateConsumer.
         */
        public NullableDateConsumer(DateDayVector vector, int index, Calendar calendar) {
            super(vector, index);
            this.calendar = calendar;
        }

        @Override
        public void consume(ResultSet resultSet) throws SQLException {
            long millis = TimeUtils.parseDateAsMilliSeconds(resultSet, columnIndexInResultSet);
            if (!resultSet.wasNull()) {
                // for fixed width vectors, we have allocated enough memory proactively,
                // so there is no need to call the setSafe method here.
                vector.set(currentIndex, Math.toIntExact(TimeUnit.MILLISECONDS.toDays(millis)));
            }
            currentIndex++;
        }
    }

    /**
     * Non-nullable consumer for date.
     */
    static class NonNullableDateConsumer extends BaseConsumer<DateDayVector> {

        protected final Calendar calendar;

        /**
         * Instantiate a DateConsumer.
         */
        public NonNullableDateConsumer(DateDayVector vector, int index) {
            this(vector, index, /* calendar */null);
        }

        /**
         * Instantiate a DateConsumer.
         */
        public NonNullableDateConsumer(DateDayVector vector, int index, Calendar calendar) {
            super(vector, index);
            this.calendar = calendar;
        }

        @Override
        public void consume(ResultSet resultSet) throws SQLException {
            long millis = TimeUtils.parseDateAsMilliSeconds(resultSet, columnIndexInResultSet);
            // for fixed width vectors, we have allocated enough memory proactively,
            // so there is no need to call the setSafe method here.
            vector.set(currentIndex, Math.toIntExact(TimeUnit.MILLISECONDS.toDays(millis)));
            currentIndex++;
        }
    }
}

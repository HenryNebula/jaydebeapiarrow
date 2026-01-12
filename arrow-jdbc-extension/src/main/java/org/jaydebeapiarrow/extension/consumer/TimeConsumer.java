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

package org.jaydebeapiarrow.extension.consumer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.adapter.jdbc.consumer.JdbcConsumer;
import org.apache.arrow.adapter.jdbc.consumer.BaseConsumer;
import org.apache.arrow.vector.TimeMilliVector;

import org.jaydebeapiarrow.extension.TimeUtils;


public abstract class TimeConsumer {
    public TimeConsumer() {
    }

    public static JdbcConsumer<TimeMilliVector> createConsumer(TimeMilliVector vector, int index, boolean nullable) {
        return (nullable ?
                new NullableTimeConsumer(vector, index) :
                new NonNullableTimeConsumer(vector, index)
        );
    }

    static class NonNullableTimeConsumer extends BaseConsumer<TimeMilliVector> {

        private final AtomicBoolean useLegacy = new AtomicBoolean(false);

        public NonNullableTimeConsumer(TimeMilliVector vector, int index) {
            super(vector, index);
        }

        public void consume(ResultSet resultSet) throws SQLException {
            int millis = TimeUtils.parseTimeAsMilliSeconds(resultSet, columnIndexInResultSet, null, useLegacy);
            vector.set(this.currentIndex, millis);
            ++this.currentIndex;
        }
    }

    static class NullableTimeConsumer extends BaseConsumer<TimeMilliVector> {

        private final AtomicBoolean useLegacy = new AtomicBoolean(false);

        public NullableTimeConsumer(TimeMilliVector vector, int index) {
            super(vector, index);
        }

        public void consume(ResultSet resultSet) throws SQLException {
            int millis = TimeUtils.parseTimeAsMilliSeconds(resultSet, columnIndexInResultSet, null, useLegacy);
            if (!resultSet.wasNull()) {
                vector.set(this.currentIndex, millis);
            }
            ++this.currentIndex;
        }
    }
}

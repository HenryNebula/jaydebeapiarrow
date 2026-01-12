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

import java.util.Calendar;
import java.util.TimeZone;
import java.sql.Types;

import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.adapter.jdbc.consumer.JdbcConsumer;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.TimeUnit;

public class OverriddenConsumer {

    private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    public OverriddenConsumer() {
    }

    public ArrowType getJdbcToArrowTypeConverter(final JdbcFieldInfo fieldInfo) {
        switch (fieldInfo.getJdbcType()) {
            case Types.TIMESTAMP_WITH_TIMEZONE:
                final String timezone = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeZone().getID();
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, timezone);
            case Types.TIMESTAMP:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            default:
                return JdbcToArrowUtils.getArrowTypeFromJdbcType(fieldInfo, null);
        }
    }

    public static JdbcConsumer getConsumer(ArrowType arrowType, int columnIndex, boolean nullable,
                                    FieldVector vector, JdbcToArrowConfig config) {

        Calendar calendar = UTC_CALENDAR;

        switch (arrowType.getTypeID()) {
            /*
             * We override Date, Time, and Timestamp consumers because the default consumers
             * in the Apache Arrow JDBC library do not provide the specific precision or
             * calendar-based conversion logic we require.
             * 
             * Most notably, the standard Timestamp consumer does not handle microsecond
             * precision natively in the way this project expects, and our custom 
             * implementations ensure consistent behavior across different JDBC drivers.
             */
            case Date:
                return DateConsumer.createConsumer((DateDayVector) vector, columnIndex, nullable, calendar);
            case Time:
                return TimeConsumer.createConsumer((TimeMilliVector) vector, columnIndex, nullable);
            case Timestamp:
                if (((ArrowType.Timestamp) arrowType).getTimezone() == null) {
                    return TimestampConsumer.createConsumer((TimeStampMicroVector) vector, columnIndex, nullable);
                }
                else {
                    return TimestampTZConsumer.createConsumer((TimeStampMicroTZVector) vector, columnIndex, nullable, calendar);
                }
            default:
                return JdbcToArrowUtils.getConsumer(arrowType, columnIndex, nullable, vector, config);
        }
    }
}
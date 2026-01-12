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

import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.util.List;
import java.util.logging.Logger;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcParameterBinder;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.binder.TimeStampBinder;
import org.apache.arrow.adapter.jdbc.binder.DateDayBinder;
import org.apache.arrow.adapter.jdbc.binder.DateMilliBinder;
import org.jaydebeapiarrow.extension.binder.Time32BinderWithCalendar;
import org.jaydebeapiarrow.extension.binder.Time64BinderWithCalendar;
import org.jaydebeapiarrow.extension.consumer.OverriddenConsumer;


public class JDBCUtils {

    private static final Logger logger = Logger.getLogger(JDBCUtils.class.getName());

    private static final Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    public JDBCUtils() {}

    public static void prepareStatementFromStream(long cStreamPointer, PreparedStatement statement, boolean isBatch) throws Exception {
        try (final ArrowArrayStream stream = ArrowArrayStream.wrap(cStreamPointer);
            BufferAllocator allocator = AllocatorSingleton.getChildAllocator();
            final ArrowReader input = Data.importArrayStream(allocator, stream)) {
            VectorSchemaRoot root = input.getVectorSchemaRoot();
            
            // Setup
            JdbcParameterBinder.Builder builder = JdbcParameterBinder.builder(statement, root);
            List<FieldVector> vectors = root.getFieldVectors();

            logger.info("Preparing statement with " + vectors.size() + " parameters.");

            for (int i = 0; i < vectors.size(); i++) {
                FieldVector vector = vectors.get(i);
                int paramIndex = i + 1; // JDBC is 1-based
                
                // Check if the vector is a Timestamp type
                if (vector instanceof TimeStampVector) {
                    // Instantiate your custom binder for this specific vector
                    builder.bind(paramIndex, new TimeStampBinder((TimeStampVector) vector, utcCalendar));
                    logger.info("Binding TimestampVector at param index " + paramIndex);
                }
                else if (vector instanceof DateDayVector) {
                    // Date (Day precision - 32 bit)
                    builder.bind(paramIndex, new DateDayBinder((DateDayVector) vector, utcCalendar));
                }
                else if (vector instanceof DateMilliVector) {
                    // Date (Millisecond precision - 64 bit)
                    builder.bind(paramIndex, new DateMilliBinder((DateMilliVector) vector, utcCalendar));
                }
                else if (vector instanceof TimeSecVector) {
                    // Time (32-bit: Seconds or Milliseconds)
                    builder.bind(paramIndex, new Time32BinderWithCalendar((TimeSecVector) vector, utcCalendar));
                }
                else if (vector instanceof TimeMilliVector) {
                    // Time (32-bit: Seconds or Milliseconds)
                    builder.bind(paramIndex, new Time32BinderWithCalendar((TimeMilliVector) vector, utcCalendar));
                }
                else if (vector instanceof TimeMicroVector) {
                    // Time (64-bit: Microseconds or Nanoseconds)
                    builder.bind(paramIndex, new Time64BinderWithCalendar((TimeMicroVector) vector, utcCalendar));
                }
                else if (vector instanceof TimeNanoVector) {
                    // Time (64-bit: Microseconds or Nanoseconds)
                    builder.bind(paramIndex, new Time64BinderWithCalendar((TimeNanoVector) vector, utcCalendar));
                }
                else {
                    // Default behavior for non-temporal columns (Int, Varchar, etc.)
                    builder.bind(paramIndex, i);
                }
            }
            JdbcParameterBinder binder = builder.build();
            while (input.loadNextBatch()) {
                while (binder.next()) {
                    if (isBatch) {
                        statement.addBatch();
                    } else {
                        // For non-batch, we only bind the first row and return
                        return;
                    }
                }
                binder.reset();
            }
            System.out.println("Executing batch: " + statement.toString());
        }
        catch (Exception e) {
            logger.severe("Error preparing statement from stream: " + e.getMessage());
            throw e;
        }
    }

    public static ArrowVectorIterator convertResultSetToIterator(ResultSet resultSet, int batchSize) throws Exception {
        BufferAllocator allocator = AllocatorSingleton.getChildAllocator();
        OverriddenConsumer overriden_consumer = new OverriddenConsumer();
        JdbcToArrowConfig arrow_jdbc_config = (
            new JdbcToArrowConfigBuilder()
            .setAllocator(allocator)
            .setTargetBatchSize(batchSize)
            .setBigDecimalRoundingMode(RoundingMode.UNNECESSARY)
            .setExplicitTypesByColumnIndex(new ExplicitTypeMapper().createExplicitTypeMapping(resultSet))
            .setIncludeMetadata(true)
            .setJdbcToArrowTypeConverter((jdbcFieldInfo) -> overriden_consumer.getJdbcToArrowTypeConverter(jdbcFieldInfo))
            .setJdbcConsumerGetter(OverriddenConsumer::getConsumer)
            .build()
        );
        ArrowVectorIterator iterator = JdbcToArrow.sqlToArrowVectorIterator(resultSet, arrow_jdbc_config);
        return iterator;
    }

}




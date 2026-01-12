/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jaydebeapiarrow.extension.binder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.util.Calendar;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.adapter.jdbc.binder.BaseColumnBinder;

/** A binder for 64-bit time types. */
public class Time64BinderWithCalendar extends BaseColumnBinder<BaseFixedWidthVector> {
  private static final long TYPE_WIDTH = 8;

  private final long factor;
  private final Calendar calendar;

  public Time64BinderWithCalendar(TimeMicroVector vector, Calendar calendar) {
    this(vector, Types.TIME, calendar);
  }

  public Time64BinderWithCalendar(TimeNanoVector vector, Calendar calendar) {
    this(vector, Types.TIME, calendar);
  }

  public Time64BinderWithCalendar(TimeMicroVector vector, int jdbcType, Calendar calendar) {
    this(vector, /*factor*/ 1_000, jdbcType, calendar);
  }

  public Time64BinderWithCalendar(TimeNanoVector vector, int jdbcType, Calendar calendar) {
    this(vector, /*factor*/ 1_000_000, jdbcType, calendar);
  }

  Time64BinderWithCalendar(BaseFixedWidthVector vector, long factor, int jdbcType, Calendar calendar) {
    super(vector, jdbcType);
    this.factor = factor;
    this.calendar = calendar;
  }

  @Override
  public void bind(PreparedStatement statement, int parameterIndex, int rowIndex)
      throws SQLException {
    // TODO: option to throw on truncation (vendor Guava IntMath#multiply)
    final Time value = new Time(vector.getDataBuffer().getLong(rowIndex * TYPE_WIDTH) / factor);
    
    if (calendar != null) {
      statement.setTime(parameterIndex, value, calendar);
    } else {
      statement.setTime(parameterIndex, value);
    }
  }
}
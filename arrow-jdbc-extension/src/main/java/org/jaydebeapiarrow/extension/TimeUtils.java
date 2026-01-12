package org.jaydebeapiarrow.extension;

import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TimeUtils {

    private static final Logger logger = Logger.getLogger(ExplicitTypeMapper.class.getName());

    public static long parseDateAsMilliSeconds(ResultSet resultSet, int columnIndexInResultSet) throws SQLException {
        long millis = 0;
        try {
            LocalDate date = resultSet.getObject(columnIndexInResultSet, LocalDate.class);
            if (! resultSet.wasNull() && date != null) {
                millis = date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            }
        }
        catch (SQLException e) {
            logger.log(Level.FINE, "Can not consume date using getObject (possibly due to lack of support for LocalDate)", e);
            Date date = resultSet.getDate(columnIndexInResultSet, JdbcToArrowUtils.getUtcCalendar());
            if (! resultSet.wasNull() && date != null) {
                millis = date.getTime();
            }
        }
        return millis;
    }

    public static int parseTimeAsMilliSeconds(ResultSet resultSet, int columnIndexInResultSet) throws SQLException {
        int millis = 0;
        try {
            LocalTime time = resultSet.getObject(columnIndexInResultSet, LocalTime.class);
            if (! resultSet.wasNull() && time != null) {
                millis = time.toSecondOfDay() * 1000;
            }
        }
        catch (SQLException e) {
            logger.log(Level.FINE, "Can not consume time using getObject (possibly due to lack of support for LocalTime)", e);
            Time time = resultSet.getTime(columnIndexInResultSet, JdbcToArrowUtils.getUtcCalendar());
            if (! resultSet.wasNull() && time != null) {
                millis = (int) time.getTime(); /* since date components set to the "zero epoch" by driver */
            }
        }
        return millis;
    }

    public static long parseTimestampAsMicroSeconds(ResultSet resultSet, int columnIndexInResultSet) throws SQLException {
        long micros = 0;
        try {
            LocalDateTime timestamp = resultSet.getObject(columnIndexInResultSet, LocalDateTime.class);
            if (! resultSet.wasNull() && timestamp != null) {
                int fractionalMicroSeconds = timestamp.getNano() / 1000;
                long integralMicroSeconds = timestamp.toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
                micros = integralMicroSeconds + fractionalMicroSeconds;
            }
        }
        catch (SQLException e) {
            logger.log(Level.FINE, "Can not consume timestamp using getObject (possibly due to lack of support for LocalDateTime)", e);
            Timestamp timestamp = resultSet.getTimestamp(columnIndexInResultSet, JdbcToArrowUtils.getUtcCalendar());
            if (! resultSet.wasNull() && timestamp != null) {
                micros = timestamp.getTime() * 1000 + (timestamp.getNanos() / 1000) % 1000;
            }
        }
        return micros;
    }
}
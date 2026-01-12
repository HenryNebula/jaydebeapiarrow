package org.jaydebeapiarrow.extension;

import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TimeUtils {

    private static final Logger logger = Logger.getLogger(ExplicitTypeMapper.class.getName());

    public static long parseDateAsMilliSeconds(ResultSet resultSet, int columnIndexInResultSet, Calendar calendar, AtomicBoolean useLegacy) throws SQLException {
        if (useLegacy.get()) {
            return parseDateLegacy(resultSet, columnIndexInResultSet, calendar);
        }
        try {
            LocalDate date = resultSet.getObject(columnIndexInResultSet, LocalDate.class);
            if (date != null) {
                return date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            }
            return 0;
        }
        catch (SQLException e) {
            if (useLegacy.compareAndSet(false, true)) {
                logger.log(Level.WARNING, "Can not consume date using getObject (possibly due to lack of support for LocalDate). Falling back to legacy consumption.", e);
            }
            return parseDateLegacy(resultSet, columnIndexInResultSet, calendar);
        }
    }

    private static long parseDateLegacy(ResultSet resultSet, int columnIndexInResultSet, Calendar calendar) throws SQLException {
        Date date = resultSet.getDate(columnIndexInResultSet, calendar != null ? calendar : JdbcToArrowUtils.getUtcCalendar());
        if (date != null) {
            return date.getTime();
        }
        return 0;
    }

    public static int parseTimeAsMilliSeconds(ResultSet resultSet, int columnIndexInResultSet, Calendar calendar, AtomicBoolean useLegacy) throws SQLException {
        if (useLegacy.get()) {
            return parseTimeLegacy(resultSet, columnIndexInResultSet, calendar);
        }
        try {
            LocalTime time = resultSet.getObject(columnIndexInResultSet, LocalTime.class);
            if (time != null) {
                return time.toSecondOfDay() * 1000;
            }
            return 0;
        }
        catch (SQLException e) {
            if (useLegacy.compareAndSet(false, true)) {
                logger.log(Level.WARNING, "Can not consume time using getObject (possibly due to lack of support for LocalTime). Falling back to legacy consumption.", e);
            }
            return parseTimeLegacy(resultSet, columnIndexInResultSet, calendar);
        }
    }

    private static int parseTimeLegacy(ResultSet resultSet, int columnIndexInResultSet, Calendar calendar) throws SQLException {
        Time time = resultSet.getTime(columnIndexInResultSet, calendar != null ? calendar : JdbcToArrowUtils.getUtcCalendar());
        if (time != null) {
            return (int) time.getTime(); /* since date components set to the "zero epoch" by driver */
        }
        return 0;
    }

    public static long parseTimestampAsMicroSeconds(ResultSet resultSet, int columnIndexInResultSet, Calendar calendar, AtomicBoolean useLegacy) throws SQLException {
        if (useLegacy.get()) {
            return parseTimestampLegacy(resultSet, columnIndexInResultSet, calendar);
        }
        try {
            LocalDateTime timestamp = resultSet.getObject(columnIndexInResultSet, LocalDateTime.class);
            if (timestamp != null) {
                int fractionalMicroSeconds = timestamp.getNano() / 1000;
                long integralMicroSeconds = timestamp.toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
                return integralMicroSeconds + fractionalMicroSeconds;
            }
            return 0;
        }
        catch (SQLException e) {
            if (useLegacy.compareAndSet(false, true)) {
                logger.log(Level.WARNING, "Can not consume timestamp using getObject (possibly due to lack of support for LocalDateTime). Falling back to legacy consumption.", e);
            }
            return parseTimestampLegacy(resultSet, columnIndexInResultSet, calendar);
        }
    }

    private static long parseTimestampLegacy(ResultSet resultSet, int columnIndexInResultSet, Calendar calendar) throws SQLException {
        Timestamp timestamp = resultSet.getTimestamp(columnIndexInResultSet, calendar != null ? calendar : JdbcToArrowUtils.getUtcCalendar());
        if (timestamp != null) {
            return timestamp.getTime() * 1000 + (timestamp.getNanos() / 1000) % 1000;
        }
        return 0;
    }
}

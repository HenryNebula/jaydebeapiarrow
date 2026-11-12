package org.jaydebeapi.mockdriver;

/**
 * A driver that, like many real JDBC drivers (sqlite-jdbc, Trino, ...),
 * references org.slf4j but deliberately does not bundle it (slf4j-api is
 * {@code provided}-scope in the pom, so assembly leaves it out).
 *
 * Used to test that the dynamic driver loader can back such drivers with
 * the extension jar's shaded slf4j when the JVM was started externally
 * without a classpath: without that fallback, class initialization dies
 * with {@code NoClassDefFoundError: org/slf4j/LoggerFactory}.
 */
public class Slf4jDependentDriver extends MockDriver {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(Slf4jDependentDriver.class);

    static {
        // Touch the logger so class initialization always resolves slf4j,
        // even if the field above is optimized away by a future change.
        LOG.getClass();
    }
}

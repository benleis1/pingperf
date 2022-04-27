package com.tableausoftware.ping;

import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class DatabaseConnection {
    private String m_host;
    private String m_port;
    private Connection m_connection;
    private String m_template;
    static final String WORKGROUP_URL_TEMPLATE = "jdbc:postgresql://%s:%s/workgroup?" +
            "ApplicationName=stats-collector";
    static final String SSL_CONFIG = "&ssl=true&sslrootcert=ca-root.pem";
    static final String RELATIONSHIP_URL_TEMPLATE = "jdbc:postgresql://%s:%s/Relationship?" +
            "ApplicationName=stats-collector";
    private Duration m_connOpenDuration;
    private Duration m_connSslOpenDuration;

    static final Logger LOGGER = LogManager.getLogger(DatabaseConnection.class);

    /**
     * Types for use in construction.
     */
    public enum DbType { WORKGROUP, RELATIONSHIP };

    /**
     *
     * @return
     */
    public Connection getConnection() {
        return m_connection;
    }

    /**
     * Access for conn open time.
     * @return Duration
     */
    public Duration getConnOpenDuration() {
        return m_connOpenDuration;
    }

    /**
     * Access for conn open time.
     * @return Duration
     */
    public Duration getConnSslOpenDuration() {
        return m_connSslOpenDuration;
    }

    /**
     *
     * @return
     */
    public String getHost() {
        return m_host;
    }

    /**
     * Create the connection. Try for SSL first and then downgrade to open.
     *
     * @param type supplies workgroup or relationship. Currrently we really only support workgroup
     * @param podsettings supplies all the host,name and password
     * @throws Exception
     */
    public DatabaseConnection (DbType type, Properties podsettings) throws Exception {
        m_host = podsettings.getProperty("pgsql.host");
        m_port = podsettings.get("pgsql.port").toString();
        if (type == DbType.WORKGROUP) {
            m_template = WORKGROUP_URL_TEMPLATE;
        } else if (type == DbType.RELATIONSHIP) {
            m_template = RELATIONSHIP_URL_TEMPLATE;
        } else {
            throw new InvalidParameterException();
        }

        m_connection = openConnection(podsettings.getProperty("jdbc.username"),
                podsettings.getProperty("jdbc.password"));
    }

    /**
     *  Return a string of the form:
     *  "PostgreSQL 9.6.18 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-11), 64-bit"
     */
    public String getPgVersion() throws SQLException {
        String verString;
        try (Statement st = getConnection().createStatement()) {
            ResultSet rs = st.executeQuery("SELECT version()");
            rs.next();
            verString = rs.getString(1);
        }
        return verString;
    }

    /**
     * Check if the connection is still valid and if not attempt to recreate it.
     */
    public void renew(Properties settings) throws SQLException {
        // Do a ping of the DB
        try (Statement st = getConnection().createStatement()) {
            ResultSet rs = st.executeQuery("SELECT 1");
            rs.next();
        } catch (Exception ex) {
            m_connection.close();
            m_connection = openConnection(settings.getProperty("jdbc.username"),
                    settings.getProperty("jdbc.password"));
        }
    }

    /**
     *  Open the DB connection using the current user/password. We'll open both with and without ssl
     *  for timing purposes and preferentially use the ssl connection if available.
     *
     * @param user
     * @param password
     * @return
     * @throws SQLException
     */
    private Connection openConnection(String user, String password) throws
            SQLException {
        Connection conn = null;
        Instant start;

        String url = String.format(m_template, m_host, m_port);

        try {
            start = Instant.now();
            conn = DriverManager.getConnection(url, user, password);
            m_connOpenDuration = Duration.between(start, Instant.now());
        } catch (Exception ex) {
            LOGGER.warn("Failed to connect to the DB without ssl: " + ex.getMessage());
            throw ex;
        }

        return conn;
    }
}

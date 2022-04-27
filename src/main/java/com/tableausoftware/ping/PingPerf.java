package com.tableausoftware.ping;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

/**
 *  Main class for the project. This was quickly adapted from elsewhere and retains some traces of
 *  its original purpose.
 */
public class PingPerf {
    static Logger s_logger = null;

    /**
     * Usage message
     */
    private static void usageAndExit() {
        System.out.printf("Usage: pingperf -n [num samples] -u [user]" +
                        " -P [password] -h [host] -p [port]\n");
        System.exit(1);
    }

    /**
     *  Do all the command line parsing
     *
     * @param args supplies the command line arguments
     *
     * @return a cmdLine object containing the results
     */
    public static CommandLine processCommandLine(String[] args)
    {
        Options opts = new Options();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmdline = null;

        opts.addOption( "P", "password", true, "database password");
        opts.addOption( "u", "user", true, "database user");
        opts.addOption( "h", "host", true, "database address");
        opts.addOption( "p", "port", true, "database port");

        Option opt = new Option("n", true, "number of samples to take");
        opt.setLongOpt("numsamples");
        opt.setType(Number.class);
        opts.addOption(opt);

        try {
            cmdline = parser.parse(opts, args);
        } catch(ParseException ex) {
            System.out.println("Failed to parse the command line: " + ex.getMessage());
            usageAndExit();
        }

        return cmdline;
    }

    static Results timeRunWarm(Connection conn, int samples) throws SQLException {

        double sum = 0;
        double powsum = 0;
        double[] vals = new double[samples];
        Results results = new Results();

        //warm things up;
        PreparedStatement stmt = conn.prepareStatement("SELECT 101010");
        stmt.execute();
        stmt.close();

        for (int index=0; index < samples; index++) {
            Instant start = Instant.now();
            String query = String.format("SELECT %d", index);
            stmt = conn.prepareStatement(query);
            stmt.execute();
            ResultSet rs  = stmt.getResultSet();
            rs.next();
            int val = rs.getInt(1);
            if (val != index) {
                s_logger.error("Invalid result {} returned expected {}", val, index);
            }
            rs.close();
            stmt.close();
            Duration duration = Duration.between(start, Instant.now());
            vals[index] = duration.toNanos() / 1000000.0;
            s_logger.info(String.format("Call time: %.02f", vals[index]));

            results.max = Math.max(vals[index], results.max);
            sum += vals[index];
        }

        results.mean = sum / samples;

        for (int index=0; index < samples; index++) {
            powsum += (vals[index]  - results.mean) * (vals[index] - results.mean);
        }

        results.stddev = Math.sqrt(powsum / samples);
        return results;
    }

    static CombinedResults timeRunCold(DatabaseConnection dbConn, Properties podsetting, int samples) throws SQLException {
        double pingsum = 0;
        double opensum = 0;
        double powsum = 0;
        double[] pingvals = new double[samples];
        double[] openvals = new double[samples];
        CombinedResults results  = new CombinedResults();

        // close whatever was already there.
        dbConn.getConnection().close();

        for (int index=0; index < samples; index++) {
            dbConn.renew(podsetting);
            Connection conn = dbConn.getConnection();
            Instant start = Instant.now();
            String query = String.format("SELECT %d", index);
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            ResultSet rs  = stmt.getResultSet();
            rs.next();
            int val = rs.getInt(1);
            if (val != index) {
                s_logger.error("Invalid result {} returned expected {}", val, index);
            }
            rs.close();
            stmt.close();
            conn.close();
            Duration duration = Duration.between(start, Instant.now());
            pingvals[index] = duration.toNanos() / 1000000.0;
            openvals[index] = dbConn.getConnOpenDuration().toNanos() / 1000000.0;
            s_logger.info(String.format("Open time: %.02f Call time: %.02f",
                openvals[index],
                pingvals[index]));

            results.opens.max = Math.max(openvals[index], results.opens.max);
            results.pings.max = Math.max(pingvals[index], results.pings.max);
            pingsum += pingvals[index];
            opensum += openvals[index];
        }

        // Calc the stats

        results.opens.mean = opensum / samples;
        results.pings.mean = pingsum / samples;

        for (int index=0; index < samples; index++) {
            powsum += (pingvals[index]  - results.pings.mean) * (pingvals[index] - results.pings.mean);
        }
        results.pings.stddev = Math.sqrt(powsum / samples);

        powsum = 0;
        for (int index=0; index < samples; index++) {
            powsum += (openvals[index]  - results.opens.mean) * (openvals[index] - results.opens.mean);
        }
        results.opens.stddev = Math.sqrt(powsum / samples);
        return results;
    }

    /**
     *  Time stuff.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        CommandLine cmdline = processCommandLine(args);

        // Logging directory is configured on the cmdline normally
        s_logger = LogManager.getLogger(PingPerf.class);

        Properties podsettings = new Properties();

        if (cmdline.hasOption("password")) {
            podsettings.put("jdbc.password", cmdline.getOptionValue("password"));
        }

        if (cmdline.hasOption("user")) {
            podsettings.put("jdbc.username", cmdline.getOptionValue("user"));
        }

        if (cmdline.hasOption("host")) {
            podsettings.put("pgsql.host", cmdline.getOptionValue("host"));
        }

        if (cmdline.hasOption("port")) {
            podsettings.put("pgsql.port", cmdline.getOptionValue("port"));
        }

        if (!podsettings.containsKey("jdbc.username") || !podsettings.containsKey("jdbc.password")) {
            s_logger.error("A database username and password was not specified directly or via a podsettings file.");
            usageAndExit();
        }

        if (!podsettings.containsKey("pgsql.port") || !podsettings.containsKey("pgsql.host")) {
            s_logger.error("A database host and port was not specified directly or via a podsettings file.");
            usageAndExit();
        }

        int samples = 10;
        if (cmdline.hasOption("numsamples")) {
            samples = ((Long)cmdline.getParsedOptionValue("numsamples")).intValue();
        }

        try {
            DatabaseConnection dbconn = new DatabaseConnection(DatabaseConnection.DbType.WORKGROUP, podsettings);

            s_logger.info("Starting timing: {} samples (All timing in ms)", samples);
            s_logger.info("New connection test", samples);

            CombinedResults coldResult  = timeRunCold(dbconn, podsettings, samples);
            s_logger.info("\nWarm connection test", samples);
            dbconn.renew(podsettings);
            Connection conn = dbconn.getConnection();
            Results warmResult = timeRunWarm(conn, samples);
            conn.close();

            s_logger.info(String.format("\nOpen perf: max: %.02f ms mean: %.02f ms stddev: %.02f", coldResult.opens.max,
                coldResult.opens.mean, coldResult.opens.stddev));
            s_logger.info(String.format("Cold run queries: max: %.02f ms mean: %.02f ms stddev: %.02f", coldResult.pings.max,
                    coldResult.pings.mean, coldResult.pings.stddev));
            s_logger.info(String.format("Warm run queries: max: %.02f ms mean: %.02f ms stddev: %.02f", warmResult.max,
                    warmResult.mean, warmResult.stddev));

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}


class CombinedResults {
    public Results opens;
    public Results pings;

    CombinedResults() {
        opens = new Results();
        pings = new Results();
    }
}

class Results {
    public double max;
    public double mean;
    public double stddev;

    public Results() {
        max = 0;
        mean = 0;
        stddev = 0;
    }
}

package com.getindata;


import org.apache.commons.cli.*;

import java.util.logging.Logger;

public class TestPresto {
    private final static Logger LOGGER = Logger.getLogger(PrestoJDBCConnectionTester.class.getName());

    public static void main(String[] args) {



        Options options = new Options();

        Option user = new Option("u", "user", true, "User to connect");
        user.setRequired(true);
        options.addOption(user);

        Option host = new Option("h", "hostname", true, "Presto coordinator hostname");
        host.setRequired(true);
        options.addOption(host);

        Option port = new Option("p", "port", true, "Presto coordinator port");
        port.setRequired(true);
        options.addOption(port);

        Option keystorePath = new Option("kp", "keystore-path", true, "Keystore path");
        keystorePath.setRequired(true);
        options.addOption(keystorePath);

        Option keystorePass = new Option("ks", "keystore-pass", true, "Keystore password");
        keystorePass.setRequired(true);
        options.addOption(keystorePass);

        Option service = new Option("s", "presto-service", true, "Presto service name");
        service.setRequired(true);
        options.addOption(service);

        Option krbPrincipal = new Option("k5p", "krb-principal", true, "Kerberos principal");
        krbPrincipal.setRequired(true);
        options.addOption(krbPrincipal);

        Option krbKeytabPath = new Option("k5k", "krb-keytab", true, "Kerberos keytab path");
        krbKeytabPath.setRequired(true);
        options.addOption(krbKeytabPath);

        Option query = new Option("q", "query", true, "Query to run");
        query.setRequired(true);
        options.addOption(query);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            String pUsername = cmd.getOptionValue("user");
            String pHostname = cmd.getOptionValue("hostname");
            String pPort = cmd.getOptionValue("port");
            String pKeystorePath = cmd.getOptionValue("keystore-path");
            String pKeystorePass = cmd.getOptionValue("keystore-pass");
            String pService = cmd.getOptionValue("presto-service");
            String pKrbPrincipal = cmd.getOptionValue("krb-principal");
            String pKrbKeytabPath = cmd.getOptionValue("krb-keytab");
            String pQuery = cmd.getOptionValue("query");

            PrestoJDBCConnectionTester tester = new PrestoJDBCConnectionTester(
                    pUsername,
                    pHostname,
                    pPort,
                    pKeystorePath,
                    pKeystorePass,
                    pService,
                    pKrbPrincipal,
                    pKrbKeytabPath
            );
            LOGGER.info(String.format("Options parsed %s", pHostname));
            tester.connect();
            tester.runTestQuery(pQuery);
            tester.close();



        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("Presto jdbc Kerberos tester", options);
            System.exit(1);
        }
    }
}

package com.getindata;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PrestoJDBCConnectionTester {

    private final static Logger LOGGER = Logger.getLogger(PrestoJDBCConnectionTester.class.getName());

    private String username;
    private String host;
    private String port;
    private String keystorePath;
    private String keystorePass;
    private String service;
    private String krbPrincipal;
    private String krbKeytabPath;
    private String query;
    private Connection connection;
    private String jdbcConnStringTemplate = "jdbc:presto://%s:%s/hive/" +
            "?SSL=true" +
            "&SSLKeyStorePath=%s" +
            "&SSLKeyStorePassword=%s" +
            "&KerberosRemoteServiceName=%s" +
            "&KerberosPrincipal=%s&" +
            "KerberosKeytabPath=%s&" +
            "user=%s";

    public PrestoJDBCConnectionTester(String username, String host, String port, String keystorePath, String keystorePass, String service,
                                      String krbPrincipal, String krbKeytabPath){
        this.username = username;
        this.host = host;
        this.port = port;
        this.keystorePath = keystorePath;
        this.keystorePass = keystorePass;
        this.service = service;
        this.krbPrincipal = krbPrincipal;
        this.krbKeytabPath = krbKeytabPath;

        LOGGER.setLevel(Level.ALL);
    }



    /**
     * Connect to the JDBC
     * @return
     */
    public int connect () {


        String driverName = "io.prestosql.jdbc.PrestoDriver";
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }

        String connString =
                String.format(this.jdbcConnStringTemplate,
                                this.host,
                                this.port,
                                this.keystorePath,
                                this.keystorePass,
                                this.service,
                                this.krbPrincipal,
                                this.krbKeytabPath,
                                this.username
        );
        try {
            LOGGER.info(String.format("Connection string: %s",connString));
            this.connection = DriverManager.getConnection(connString);
            LOGGER.info(String.format("Connections status: %s",!this.connection.isClosed()));
            if(!this.connection.isClosed() ) {
                return 0;
            }
            else {
                return -1;
            }
        }
        catch (Exception e) {
            LOGGER.info(String.format("Exception: %s", e.getMessage()) );
        }
        finally {
            return -1;
        }

    }

    public int runTestQuery(String query) {
        try {
            LOGGER.info(String.format("Running a test query: %s", query) ) ;
            Statement stmt = this.connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            LOGGER.info("Bulding output...:");

            List<String> columnNames = new ArrayList<>();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                columnNames.add(rsmd.getColumnLabel(i));
            }

            int rowIndex = 0;
            while (rs.next()) {
                rowIndex++;
                // collect row data as objects in a List
                List<Object> rowData = new ArrayList<>();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    rowData.add(rs.getObject(i));
                }
                LOGGER.info(String.format("Row %d%n", rowIndex));
                for (int colIndex = 0; colIndex < rsmd.getColumnCount(); colIndex++) {
                    String objType = "null";
                    String objString = "";
                    Object columnObject = rowData.get(colIndex);
                    if (columnObject != null) {
                        objString = columnObject.toString() + " ";
                        objType = columnObject.getClass().getName();
                    }
                    LOGGER.info(String.format("  %s: %s(%s)%n",
                            columnNames.get(colIndex), objString, objType));
                }
            }
        }
        catch (Exception e){
            LOGGER.info(String.format("Exception: %s", e.getMessage()) );
        }


        return 0;
    }

    public void close () {
        try {
            this.connection.close();
        }
        catch (Exception e){
            LOGGER.info(String.format("Exception: %s", e.getMessage()) );
        }

    }



}

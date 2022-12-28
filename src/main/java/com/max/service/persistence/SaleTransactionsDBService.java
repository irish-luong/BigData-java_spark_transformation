package com.max.service.persistence;


import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Comparator;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;

@Slf4j
public final class SaleTransactionsDBService {

    private static final String APP_NAME = "SparkTest";
    private static final String LOCAL_NODE_ID = "local[*]";
    private static final String SPARK_FILES_FORMAT = "csv";

    private static final String PATH_RESOURCE = "src/main/resources/spark-data/electronic-card-transactions.csv";
    private static final String PATH_FOLDER_RESOURCES = "src/main/resources/spark-data/enriched_transactions";


    public static final String TABLE_NAME = "INIT";
    public static final String DB_URL = "jdbc:derby:firstdb;create=true;user=app;password=derby";
    public static final String EMBEDDED_DRIVER_STRING = "org.apache.derby.jdbc.EmbeddedDriver";

    /**
     *
     * @throws SQLException
     */
    public void startDB() throws SQLException {
        System.out.println("Starting db");
        Connection conn = DriverManager.getConnection(DB_URL);
        createDatabase(conn);
    }

    public static String getDbUrl() {
        return DB_URL;
    }

    public static String getTableName() {
        return TABLE_NAME;
    }

    /**
     *
     * @return
     */
    public static Properties buildDBProperties () {
         Properties props = new Properties();
         props.setProperty("driver", "org.apache.derby.jdbc.EmbeddedDriver");
         props.setProperty("user", "app");
         props.setProperty("password", "derby");
         return props;
    }

    public void displayTableResult(int numRecords) throws SQLException {

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet resultSet = null;

        try {

            conn = DriverManager.getConnection(DB_URL);
            stmt = conn.prepareStatement("SELECT * FROM " + TABLE_NAME);
            resultSet = stmt.executeQuery();

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int printed = 0;
            int columnCount = resultSetMetaData.getColumnCount();

            while(resultSet.next() && printed < numRecords) {

                log.info("======== RECORD" + printed + "========");

                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = resultSet.getString(i);
                    log.info("COLUMN: " + resultSetMetaData.getColumnName(i) +  " - VALUE:  " + columnValue);
                }

                log.info("================================");
                printed++;
            }

        } catch (Exception error) {
            throw error;
        } finally {
            closeConnection(conn, stmt, resultSet);
        }
    }

    /**
     *
     * @param conn
     * @param stmt
     * @param resultSet
     */
    public void closeConnection(Connection conn, PreparedStatement stmt, ResultSet resultSet) {

        try {
            if (conn != null) {
                conn.close();
            }

            if (stmt != null) {
                stmt.close();
            }

            if (resultSet != null) {
                resultSet.close();
            }
        } catch (Exception err) {
            log.error("Got error in close connection DB.  " + err.getMessage());
        }


    }

    /**
     *
     * @param conn
     */
    private void createDatabase(Connection conn) {

        try (Statement stmt = conn.createStatement()) {
            String query = "CREATE TABLE INIT" + " (id integer)";
            stmt.execute(query);
        } catch (SQLException err) {
            log.error("Error while create table in DB: " + err.getMessage());
        }
    }

    private static void init() throws SQLException {

        deletePreviousFile(PATH_FOLDER_RESOURCES);

        SaleTransactionsDBService saleTransactionsDBService = new SaleTransactionsDBService();
        saleTransactionsDBService.startDB();

        SparkSession spark = SparkSession.builder()
                .appName(APP_NAME)
                .master(LOCAL_NODE_ID)
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format(SPARK_FILES_FORMAT)
                .option("header", "true")
                .load(PATH_RESOURCE);

        Dataset<Row> resultDF;
        resultDF = df
                .withColumn(
                        "total_units_value",
                        col("Data_value").multiply(col("magnitude"))
                );
        // Persist data to DB
        resultDF.write()
                .mode(SaveMode.Overwrite)
                .jdbc(
                        SaleTransactionsDBService.getDbUrl(),
                        SaleTransactionsDBService.getTableName(),
                        SaleTransactionsDBService.buildDBProperties()
                );

        saleTransactionsDBService.displayTableResult(5);

        resultDF.write()
                .format(SPARK_FILES_FORMAT)
                .mode(SaveMode.Overwrite)
                .save(PATH_FOLDER_RESOURCES);

        resultDF.show(5);

    }

    private static void deletePreviousFile(String sourcePath) {

        try {
            Stream<Path> allContents = Files.list(Paths.get(PATH_FOLDER_RESOURCES));
            allContents.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (Exception err) {
            log.error("Get error when delete file. " + err.getMessage());
        }


    }



}

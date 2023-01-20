package com.max.repository.impl;


import com.max.config.properties.ApplicationProperties;
import com.max.datasource.DerbyDbManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public class StudentDatasetRepository {

    private static final String TABLE_NAME = "student_grade";

    private final ApplicationProperties applicationProperties;

    private final SparkSession sparkSession;

    private DerbyDbManager derbyDbManager;


    public void startSession() throws SQLException {

        if (derbyDbManager == null) {
            String url = applicationProperties.getDerby().getUrl();
            derbyDbManager = new DerbyDbManager(url);
        };

        if (derbyDbManager.getConnection() == null) {
            derbyDbManager.createConnection();
        }

    }

    public void safeCreateTable() throws SQLException {
        Connection connection = derbyDbManager.getConnection();

        try {

            log.info("======== Create table " + TABLE_NAME + " ==========");

            String createStatement = "CREATE TABLE " + TABLE_NAME + " (" +
                    "   id INT NOT NULL GENERATED ALWAYS AS IDENTITY," +
                    "   name VARCHAR(255)," +
                    "   grade_1 VARCHAR(255)," +
                    "   grade_2 VARCHAR(255)," +
                    "   grade_3 VARCHAR(255)," +
                    "   grade_4 VARCHAR(255)," +
                    "   grade_5 VARCHAR(255)," +
                    "   PRIMARY KEY (Id))";

            Statement statement = connection.createStatement();
            statement.executeUpdate(createStatement);

            log.info("================ DONE ================");

        } catch (SQLException e) {
            if (e.getSQLState().equals("X0Y32")) {
                log.error("Table " + TABLE_NAME + " exist");
            } else {
                throw e;
            }
        }
    }


    public Dataset<Row> queryByNames(List<String> names) {

        String where = "(select * from " +
                TABLE_NAME +
                " where name in ('" + String.join("','", names) + "')" +
                ") as subset";


        return sparkSession.read().jdbc(
                applicationProperties.getDerby().getUrl(), where, applicationProperties.getDerby().getDerbyProperties()
        );

    }


    public void upsertStudent(Dataset<Row> df) {

        Dataset<Row> currentDF = queryByNames(
                df.select("name")
                        .as(Encoders.STRING())
                        .collectAsList()
        ).transform(this::renameColumnFromDb);


        log.info("============== Exist records " + currentDF.count() + " ==============");


        Dataset<Row> newRecords = df.join(
                currentDF, df.col("name").equalTo(currentDF.col("name")), "left_anti"
        );

        log.info("============== New records " + newRecords.count() + " ==============");

        currentDF.show();
        newRecords.show();

        insertStudent(newRecords);
        updateStudent(currentDF);

    }

    private Dataset<Row> renameColumnFromDb(Dataset<Row> df) {

        return df.withColumnRenamed("ID", "id")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("GRADE_1", "grade_1")
                .withColumnRenamed("GRADE_2", "grade_2")
                .withColumnRenamed("GRADE_3", "grade_3")
                .withColumnRenamed("GRADE_4", "grade_4")
                .withColumnRenamed("GRADE_5", "grade_5");
    }

    private void insertStudent(Dataset<Row> df) {
        df.write().mode(SaveMode.Append).jdbc(
                applicationProperties.getDerby().getUrl(), TABLE_NAME, applicationProperties.getDerby().getDerbyProperties());
    }


    private void updateStudent(Dataset<Row> df) {
        df.mapPartitions(new UpdateMapper(derbyDbManager, applicationProperties), RowEncoder.apply(df.schema()));
    }

}


class UpdateMapper implements MapPartitionsFunction<Row, Row> {

    private DerbyDbManager derbyDbManager;

    private final ApplicationProperties applicationProperties;

    UpdateMapper(DerbyDbManager derbyDbManager, ApplicationProperties applicationProperties) {
        this.derbyDbManager = derbyDbManager;
        this.applicationProperties = applicationProperties;
    }

    private static final String UPDATE_STATEMENT = "update student set" +
            " grade_1 = ?," +
            " grade_2 = ?," +
            " grade_3 = ?," +
            " grade_4 = ?," +
            " grade_5 = ?" +
            " where id = ?";

    @Override
    public Iterator<Row> call(Iterator<Row> iterator) throws Exception {

        // Start session
        startSession();

        Connection connection = derbyDbManager.getConnection();

        PreparedStatement statement = connection.prepareStatement(UPDATE_STATEMENT);

        while(iterator.hasNext()) {
            int index = 0;
            Row row = iterator.next();

            statement.setString(0, row.getAs("grade_1"));
            statement.setString(1, row.getAs("grade_1"));
            statement.setString(2, row.getAs("grade_3"));
            statement.setString(3, row.getAs("grade_4"));
            statement.setString(4, row.getAs("grade_5"));
            statement.setString(5, row.getAs("id"));

            statement.addBatch();
        }

        statement.executeBatch();

        return iterator;
    }

    public void startSession() throws SQLException {

        if (derbyDbManager == null) {
            String url = applicationProperties.getDerby().getUrl();
            derbyDbManager = new DerbyDbManager(url);
        };

        if (derbyDbManager.getConnection() == null) {
            derbyDbManager.createConnection();
        }

    }
}
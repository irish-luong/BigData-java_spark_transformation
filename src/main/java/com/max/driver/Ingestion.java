package com.max.driver;


import com.max.model.pojo.Car;
import com.max.service.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

import static org.apache.spark.sql.functions.*;
import static com.max.driver.CliArgs.*;


@Slf4j
@Component
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
@CommandLine.Command(name = "data-ingest")
public final class Ingestion {

//    private static final String FILE_NAME = "--file-name";



    private final SaleIngestionService saleIngestionService;

    private final CarIngestionService carIngestionService;

    private final FoodIngestionService foodIngestionService;

    private final StudentSubjectsService studentSubjectsService;

    private final FarmService farmService;

    private final AirlineService airlineService;

    private final CovidService covidService;

    private final NetworkService networkService;

    private final ComplexFileIngestionService complexFileIngestionService;


    Logger LOGGER = LoggerFactory.getLogger(Ingestion.class);

    @CommandLine.Command(name = "master-sale")

    public void ingestMasterSaleData(
            @CommandLine.Option(names = FILE_NAME, required = true) String fileName
    ) {
        Dataset<Row> masterSale = saleIngestionService.loadCleanMasterSaleData(fileName);

        masterSale.show();
    }

    @CommandLine.Command(name = "books")
    public void ingestBookData(
            @CommandLine.Option(names = FILE_NAME, required = true) String fileName
    ) {
        Dataset<Row> bookDF = saleIngestionService.loadBooks(fileName);

        bookDF.show();
    }

    @CommandLine.Command(name = "subjects")
    public void ingestSubjectData(
            @CommandLine.Option(names = FILE_NAME, required = true) String fileName
    ) {
        Dataset<Row> subjectDF = saleIngestionService.loadSubjects(fileName);

        subjectDF.show();
    }

    @CommandLine.Command(name = "cars")
    public void ingestCarData(
            @CommandLine.Option(names = FILE_NAME, required = true) String fileName
    ) {

        Dataset<Car> carDF = carIngestionService.loadData(fileName);
        carDF.show();

    }

    @CommandLine.Command(name = "generic-foods")
    public void ingestGenericFoodData(
            @CommandLine.Option(names = FILE_NAME, required = true) String fileName
    ) {

        Dataset<Row> foodDF = foodIngestionService.loadData(fileName);
        foodDF.show();
    }

    @CommandLine.Command(name = "student-subjects")
    public void ingestStudentSubjectData(
            @CommandLine.Option(names = FILE_NAME, required = true) String fileName
    ) {

        Dataset<Row> studentSubjectsDF = studentSubjectsService.loadData(fileName);
        studentSubjectsDF.show();
    }

    @CommandLine.Command(name = "farm-data")
    public void ingestFarmData(
            @CommandLine.Option(names = FILE_NAME, required = true) String fileName
    ) {

        Dataset<Row> farmDF = farmService.loadData(fileName);
        farmDF.show();

        farmDF.printSchema();

        Row aggData = farmService.reduceDF(farmDF);

        log.info("Total Calculated Sales " + aggData.prettyJson());

        Dataset<Row> maxSalesDf = farmDF.agg(max(col("total_sales")));
        maxSalesDf.show();

        /** MIN */

        Dataset<Row> minSalesDf = farmDF.agg(min(col("total_sales")));
        minSalesDf.show();

        /** MIN */

        Dataset<Row> meanSalesDf = farmDF.agg(mean(col("total_sales")));
        meanSalesDf.show();

        // Show partition information
        farmService.showPartitionInfo(farmDF);
    }

    @CommandLine.Command(name = "airline")
    public void ingestAirlineData(
            @CommandLine.Option(names = FILE_NAME, required = true) String fileName
    ) {

        Dataset<Row> airlineDF = airlineService.loadData(fileName);
        airlineDF.show();

        log.info("[AIRLINE] Group by Code and Month");
        Dataset<Row> airlineDF1 = airlineService.summarizeByCodeAndMonth(airlineDF);
        airlineDF1.show();


        log.info("[AIRLINE] Group by key");
        Dataset<Row> airlineDF2 = airlineService.summarizeByKey(airlineDF);
        airlineDF2.show();

    }

    @CommandLine.Command(name = "covid-19")
    public void ingestCovidData(
            @CommandLine.Option(names = FILE_NAME, required = true) String fileName
    ) {

        Dataset<Row> covidDF = covidService.loadData(fileName);
        covidDF.show();

    }

    @CommandLine.Command(name = "network-transit")
    public void ingestNetworkTransitData(
            @CommandLine.Option(names = FILE_NAME, required = true) String fileName
    ) {

        Dataset<Row> networkTransitDF = networkService.loadData(fileName);
        networkTransitDF.show();

    }

    @CommandLine.Command(name = "complex-file")
    public void ingestComplexFileData(
            @CommandLine.Option(names = FILE_NAME, required = true) String fileName,
            @CommandLine.Option(names = FORMAT, required = true, description = "csv, json, ..etc") String format
    ) {

        Dataset<Row> df = null;

        switch (format.toLowerCase()) {
            case "csv":
                df = complexFileIngestionService.loadComplexCsv(fileName);
                break;
            case "json":
                df = complexFileIngestionService.loadJsonLine(fileName);
                break;
            case "txt":
                df = complexFileIngestionService.loadText(fileName);
                break;
            case "xml":
                df = complexFileIngestionService.loadXML(fileName);
                break;
        }

        if (df != null) {
            df.printSchema();
            df.show();
        }

        log.info("===== END ======");
    }
}

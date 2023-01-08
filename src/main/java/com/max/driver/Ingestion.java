package com.max.driver;


import com.max.service.SaleIngestionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

@Slf4j
@Component
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
@CommandLine.Command(name = "data-ingest")
public final class Ingestion {

    private final SaleIngestionService saleIngestionService;


    Logger LOGGER = LoggerFactory.getLogger(Ingestion.class);

    @CommandLine.Command(name = "master-sale")

    public void ingestMasterSaleData(
            @CommandLine.Option(names = "--file-name", required = true) String fileName
    ) {
        Dataset<Row> masterSale = saleIngestionService.loadCleanMasterSaleData(fileName);

        masterSale.show();
    }

    @CommandLine.Command(name = "books")
    public void ingestBookData(
            @CommandLine.Option(names = "--file-name", required = true) String fileName
    ) {
        Dataset<Row> bookDF = saleIngestionService.loadBooks(fileName);

        bookDF.show();
    }

    @CommandLine.Command(name = "subjects")
    public void ingestSubjectData(
            @CommandLine.Option(names = "--file-name", required = true) String fileName
    ) {
        Dataset<Row> subjectDF = saleIngestionService.loadSubjects(fileName);

        subjectDF.show();
    }
}

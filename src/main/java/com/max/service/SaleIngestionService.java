package com.max.service;

import com.max.repository.impl.MasterSaleReadRepository;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions.col;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Load - clean - transform Spark dataframe
 */
@Service
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public final class SaleIngestionService {


    private final MasterSaleReadRepository masterSaleReadRepository;


    /**
     * Load master sale data
     *
     * @param path location of datastore
     * @return master sale dataset
     */
    public Dataset<Row> loadCleanMasterSaleData(String path) {

        Map<String, String> options = Stream.of(new String[][] {
                {"header", "true"}
        })
                .collect(Collectors.toMap(data -> data[0], data -> data[1]));

        Dataset<Row> masterSaleData = masterSaleReadRepository.loadByLocation(path, options);

        return masterSaleData.transform(this::dropGarbageRows);

    }

    private Dataset<Row> dropGarbageRows(Dataset<Row> data) {
        return data.na().drop(new String[]{});
    }

    /**
     * Drop non-required columns
     * @param data Dataset
     * @return a new dataset
     */
//    private Dataset<Row> dropNonRequiredColumns(Dataset<Row> data) {
//        return data.drop()
//    }


    private List<Column> nonRequiredColumn() {
        return Arrays.asList(
                col("metadata.downloads"),
                col("metadata.id"),
                col("metadata.rank"),
                col("metadata.url"),
                col("metadata.downloads"),
                col("metrics.difficulty.automated readability index"),
                col("_c18"),
                col("_c19")
        );
    }


}

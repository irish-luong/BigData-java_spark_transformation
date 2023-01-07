package com.max.repository.impl;

import com.max.repository.DatasetReadRepository;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;


@Component
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public class MasterSaleReadRepository implements DatasetReadRepository<Row> {

    private final SparkSession spark;


    @Override
    public Dataset<Row> loadAll() {
        throw new UnsupportedOperationException("loadAll operation is unsupported for MasterSale dataset");
    }

    @Override
    public Dataset<Row> loadByQuery(String query) {
        throw new UnsupportedOperationException("loadByQuery operation is unsupported for MasterSale dataset");
    }

    @Override
    public Dataset<Row> loadByLocation(String path) {
        Map<String, String> options = new HashMap<>();
        return loadByCSV(path, options);
    }

    @Override
    public Dataset<Row> loadByLocation(String path, Map<String, String> options) {
        return loadByCSV(path, options);
    }

    private Dataset<Row> loadByCSV(String path, Map<String, String> options)  {
        return spark
                .read()
                .format("csv")
                .options(options).load(path);
    }
}

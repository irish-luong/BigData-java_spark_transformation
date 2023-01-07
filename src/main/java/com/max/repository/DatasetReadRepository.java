package com.max.repository;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

public interface DatasetReadRepository<T> {

    /**
     * Load full dataset
     * @return a dataset type of T
     */
    Dataset<T> loadAll();


    /**
     * Load dataset as result of the query
     * @param query query string
     * @return a dataset type of T
     */
    Dataset<T> loadByQuery(String query);

    /**
     * Load dataset from a storage location
     * @param path absolute path address to data storage location
     * @return a dataset type of T
     */
    Dataset<T> loadByLocation(String path);

    Dataset<Row> loadByLocation(String path, Map<String, String> options);
}

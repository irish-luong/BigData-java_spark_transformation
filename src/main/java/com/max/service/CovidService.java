package com.max.service;

import com.max.repository.impl.CSVReadRepository;
import com.max.udfs.Udfs;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public final class CovidService {

    private final CSVReadRepository csvReadRepository;

    public Dataset<Row> loadData(String path) {

        Map<String, String> options = Stream.of(
                new String[][] {
                        {"header", "true"},
                        {"inferSchema", "true"}
                })
                .collect(Collectors.toMap(data -> data[0], data -> data[1]));


        return csvReadRepository.loadByLocation(path, options)
                .transform(this::transformation);
    }

    private Dataset<Row> transformation(Dataset<Row> df) {
        return df.withColumn("EventDateParsed", parseDate())
                .drop("EventDate");
    }

    private Column parseDate() {
        return Udfs.dateParse().apply( col("EventDate"), lit("YYYY/MM/DD HH:mm:ss"));
    }
}

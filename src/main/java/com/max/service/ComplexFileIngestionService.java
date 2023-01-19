package com.max.service;

import com.max.repository.impl.FileReadRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Slf4j
@Service
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public final class ComplexFileIngestionService {

    private final FileReadRepository fileReadRepository;

    public Dataset<Row> loadComplexCsv(String path) {

        Map<String, String> options = Stream.of(
                new String[][] {
                        {"header", "true"},
                        {"sep", "|"},
                        {"quote", "\""},
                        {"multiline", "true"},
                        {"inferSchema", "true"}
                })
                .collect(Collectors.toMap(data -> data[0], data -> data[1]));


        return fileReadRepository.loadByLocation(path, options);
    }


    public Dataset<Row> loadJsonLine(String path) {

        Map<String, String> options = Stream.of(
                new String[][] {
//                        {"multiline", "true"}
                }
        ).collect(Collectors.toMap(data -> data[0], data -> data[1]));

        return fileReadRepository.loadByLocation(path, options, "json");
    }

    public Dataset<Row> loadXML(String path) {

        Map<String, String> options = Stream.of(
                new String[][] {
                        {"rowTag", "widget"}
                }
        ).collect(Collectors.toMap(data -> data[0], data -> data[1]));

        return fileReadRepository.loadByLocation(path, options, "xml");
    }

    public Dataset<Row> loadText(String path) {
        Map<String, String> options = Stream.of(
                new String[][] {

                }
        ).collect(Collectors.toMap(data -> data[0], data -> data[1]));

        return fileReadRepository.loadByLocation(path, options, "text");
    }

}


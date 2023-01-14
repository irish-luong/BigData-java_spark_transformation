package com.max.service;

import com.max.repository.impl.CSVReadRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public final class AirlineService {

    private static final int MONTHS = 12;

    private static final int AIRPORT_CODE_INDEX = 0;

    private static final Map<Integer, String> monthLookup = Stream.of(new Object[][] {
            {1, "Jan"},
            {2, "Feb"},
            {3, "March"},
            {4, "Apr"},
            {5, "May"},
            {6, "Jun"},
            {7, "July"},
            {8, "Aug"},
            {9, "Sept"},
            {10, "Oct"},
            {11, "Nov"},
            {12, "Dec"}
    }).collect(Collectors.toMap(x -> (Integer) x[0], x -> (String) x[1]));

    private final CSVReadRepository csvReadRepository;

    public Dataset<Row> loadData(String path) {

        Map<String, String> options = Stream.of(
                new String[][] {
                        {"header", "true"},
                        {"inferSchema", "true"}
                })
                .collect(Collectors.toMap(data -> data[0], data -> data[1]));


        return csvReadRepository.loadByLocation(path, options);
    }

    public Dataset<Row> summarizeByCodeAndMonth(Dataset<Row> df) {
        return df.groupBy("`Airport.Code`", "`Time.Month`")
                .agg(sum("`Statistics.Flights.Total`"));
    }

    public Dataset<Row> summarizeByKey(Dataset<Row> df) {

        Dataset<Row> filteredDF = df.filter(col("`Time.Label`").contains("2005"));

        KeyValueGroupedDataset<String, Row> keyValueGroupedDataset = filteredDF.groupByKey(
                (MapFunction<Row, String>) row -> row.get(AIRPORT_CODE_INDEX).toString(),
                Encoders.STRING()
        );

        return keyValueGroupedDataset
                .mapGroups(new AirportMapGroup(), RowEncoder.apply(new StructType(definedSchema())));
    }

    private static StructField[] definedSchema() {
        StructField[] fields = new StructField[MONTHS + 1];

        fields[AIRPORT_CODE_INDEX] = new StructField(
                "Airport Code", DataTypes.StringType, true, Metadata.empty()
        );

        for(int i = 1; i < MONTHS + 1; i++) {
            fields[i] = new StructField(
                    monthLookup.get(i), DataTypes.FloatType, true, Metadata.empty()
            );
        }

        return fields;
    }

    private static class AirportMapGroup implements MapGroupsFunction<String, Row, Row> {

        @Override
        public Row call(String s, Iterator<Row> iterator) throws Exception {

            int flightFieldIndex = 4, monthFieldIndex = 3, total = 0;

            Object[] rowValue = new Object[MONTHS + 1];

            int[] partitalResult = new int[MONTHS];

            String airportCode = null;

            while(iterator.hasNext()) {

                Row currentRow = iterator.next();

                if(Objects.isNull(airportCode)) {
                    airportCode = (String) currentRow.get(AIRPORT_CODE_INDEX);
                }

                int monthFlights = currentRow.getInt(flightFieldIndex);

                // Calculate total monthFlight
                total += monthFlights;

                // Calculate total monthFlight by this month
                partitalResult[currentRow.getInt(monthFieldIndex) - 1] += monthFlights;
            }

            // Add Airport code as the first row
            rowValue[AIRPORT_CODE_INDEX] = airportCode;
            for(int i = 1, j = 0; i < MONTHS + 1; i++, j++) {
                rowValue[i] = (partitalResult[j] * 100.0F ) / total ;
            }

            return new GenericRowWithSchema(rowValue, new StructType(definedSchema()));
        }
    }
}

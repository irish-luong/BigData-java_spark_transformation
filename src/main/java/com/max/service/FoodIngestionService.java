package com.max.service;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.api.java.function.MapFunction;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

import com.max.repository.impl.FoodReadRepository;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;

@Service
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public final class FoodIngestionService {

    private final FoodReadRepository foodReadRepository;

    public Dataset<Row> loadData(String path) {

        Map<String, String> options = Stream.of(
                new String[][] {
                        {"header", "true"}
                })
                .collect(Collectors.toMap(data -> data[0], data -> data[1]));


        Dataset<Row> df =  foodReadRepository
                .loadByLocation(path, options)
                .transform(this::projection);

        return df.map(new EmptyToUnderscoreMapper("group"), RowEncoder.apply(df.schema()))
                .filter(new HerbsFilter());
    }

    private Dataset<Row> projection(Dataset<Row> df) {
        return df.select(
                col("FOOD NAME").as("food_name"),
                col("SCIENTIFIC NAME").as("scientific_name"),
                col("GROUP").as("group"),
                col("SUB GROUP").as("sub_group")
        );
    }

}


class EmptyToUnderscoreMapper implements MapFunction<Row, Row> {

    private final String columnName;

    public EmptyToUnderscoreMapper(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public Row call(Row row) throws Exception {

        String[] fieldNames = row.schema().fieldNames();

        String[] newValues = populateRowValues(fieldNames, row);

        return new GenericRowWithSchema(newValues, row.schema());
    }

    private String[] populateRowValues(String[] fieldNames, Row rowOrigin) {
        String[] newValues = new String[fieldNames.length];

        for(int i = 0; i < fieldNames.length; i++) {
            if(fieldNames[i].equals(columnName)) {
                newValues[i] = ((String) rowOrigin.getAs(fieldNames[i])).replace(" ", "_");
            } else {
                newValues[i] = rowOrigin.getAs(fieldNames[i]);
            }
        }

        return newValues;
    }
}


class HerbsFilter implements FilterFunction<Row> {

    @Override
    public boolean call(Row row) throws Exception {
        return row.getAs("sub_group").equals("Herbs");
    }
}
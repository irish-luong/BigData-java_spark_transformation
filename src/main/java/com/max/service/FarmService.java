package com.max.service;

import com.max.repository.impl.CSVReadRepository;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;


@Service
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public final class FarmService {

    private final CSVReadRepository csvReadRepository;

    public Dataset<Row> loadData(String path) {

        Map<String, String> options = Stream.of(
                new String[][] {
                        {"header", "true"}
                })
                .collect(Collectors.toMap(data -> data[0], data -> data[1]));


        return csvReadRepository.loadByLocation(path, options)
                .drop("farm_stand","month","year","days","visitors")
                .select(
                        col("total_sales").cast(DataTypes.DoubleType),
                        col("total_snap_sales").cast(DataTypes.DoubleType),
                        col("total_double_sales").cast(DataTypes.DoubleType)

                );

    }

    public Row reduceDF(Dataset<Row> df) {
        return df.reduce(new FarmStandSalesReducer());
    }

    private static class FarmStandSalesReducer implements ReduceFunction<Row> {

        @Override
        public Row call(Row row, Row t1) throws Exception {
            Object[] newRow = new Object[3];

            newRow[0] = ((Double) row.getAs("total_sales")) +
                    ((Double) t1.getAs("total_sales"));

            newRow[1] = ((Double) row.getAs("total_snap_sales")) +
                    ((Double) t1.getAs("total_snap_sales"));

            newRow[2] = ((Double) row.getAs("total_double_sales")) +
                    ((Double) t1.getAs("total_double_sales"));

            return new GenericRowWithSchema(newRow, t1.schema());
        }
    }
}

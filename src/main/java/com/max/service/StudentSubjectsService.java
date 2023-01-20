package com.max.service;

import com.max.repository.impl.FileReadRepository;

import com.max.repository.impl.StudentDatasetRepository;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.springframework.beans.factory.annotation.Autowired;


import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.WrappedArray;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;


@Service
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public final class StudentSubjectsService {

    private final StudentDatasetRepository studentDatasetRepository;
    private final FileReadRepository fileReadRepository;

    private static final StructType STUDENT_PLATEN_SCHEMA = new StructType()
            .add("name", DataTypes.StringType)
            .add("grade_1", DataTypes.StringType)
            .add("grade_2", DataTypes.StringType)
            .add("grade_3", DataTypes.StringType)
            .add("grade_4", DataTypes.StringType)
            .add("grade_5", DataTypes.StringType);

    public Dataset<Row> loadData(String path) {

        Map<String, String> options = Stream.of(
                new String[][] {
                        {"header", "true"}
                })
                .collect(Collectors.toMap(data -> data[0], data -> data[1]));


        return fileReadRepository
                .loadByLocation(path, options)
                .transform(this::splitGrade)
                .flatMap(new SplitGrade(), RowEncoder.apply(STUDENT_PLATEN_SCHEMA))
                .distinct();

    }

    private Dataset<Row> splitGrade(Dataset<Row> df) {
        return df.withColumn("Grades", split(col("Grades"), " "));
    }


    private static class SplitGrade implements FlatMapFunction<Row, Row> {

        @Override
        public Iterator<Row> call(Row row) {
            Row newRow = new GenericRowWithSchema(mapAndFlattenArrayValue(row), STUDENT_PLATEN_SCHEMA);

            return Collections.singletonList(newRow).iterator();
        }

        private Object[] mapAndFlattenArrayValue(Row row) {

            WrappedArray<Object> arrayColumns = row.getAs("Grades");

            int finalRowSize = arrayColumns.length() + 1;
            int rowIndex = 0;

            Object[] finalRow = new Object[finalRowSize];

            // Add first columns as StudentName
            finalRow[rowIndex++] = row.getAs("Student_Name");

            String[] arrayColumnValues = (String[]) arrayColumns.array();

            for (int i = 0; i < arrayColumns.length(); i++) {
                String currentValue = arrayColumnValues[i];

                if (Integer.parseInt(currentValue) < 6) {
                    currentValue = "R";
                }
               finalRow[rowIndex++] = currentValue;
            }

            return finalRow;
        }
    }

    public void writeData(Dataset<Row> df) throws SQLException {

        studentDatasetRepository.startSession();

        studentDatasetRepository.safeCreateTable();

        List<String> names = df.select("name")
                .as(Encoders.STRING())
                .collectAsList();

        studentDatasetRepository.queryByNames(names).show();

        studentDatasetRepository.upsertStudent(df);
    }
}

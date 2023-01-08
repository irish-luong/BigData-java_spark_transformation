package com.max.service;

import com.max.repository.impl.MasterSaleReadRepository;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;



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

        return masterSaleData
                .transform(this::projectMasterData)
                .transform(this::dropGarbageRows);

    }

    public Dataset<Row> loadSubjects(String path) {

        return loadCleanMasterSaleData(path)
                .transform(this::projectSubjectColumns);
    }

    public Dataset<Row> loadBooks(String path) {
        return loadCleanMasterSaleData(path)
                .transform(this::projectBookColumns);
    }

    private Dataset<Row> dropGarbageRows(Dataset<Row> data) {
        return data.na().drop(new String[]{"subjects"});
    }


    private Dataset<Row> projectMasterData(Dataset<Row> df) {
        return df.drop("_c18", "_c19", "metadata.downloads", "metadata.id", "metadata.rank",
                        "metadata.url", "metadata.downloads","metrics.difficulty.automated readability index")
                .withColumnRenamed("bibliography.congress classifications", "classifications")
                .withColumnRenamed("bibliography.languages", "languages")
                .withColumnRenamed("bibliography.subjects", "subjects")
                .withColumnRenamed("bibliography.title","title")
                .withColumnRenamed("bibliography.type","type")
                .withColumnRenamed("bibliography.author.birth", "author_birth")
                .withColumnRenamed("bibliography.author.death", "author_death")
                .withColumnRenamed("bibliography.author.name", "author_name")
                .withColumnRenamed("bibliography.publication.day", "publication_day")
                .withColumnRenamed("bibliography.publication.full", "publication_full")
                .withColumnRenamed("bibliography.publication.month","publication_month")
                .withColumnRenamed("bibliography.publication.month name", "publication_month_name")
                .withColumnRenamed("bibliography.publication.year", "publication_year");
    }

    private Dataset<Row> projectSubjectColumns(Dataset<Row> df) {

         return df.withColumn("SubjectID", concat(col("subjects"), lit("_S")))
                 .drop("classifications", "languages", "title", "type", "author_birth", "author_death",
                         "author_name", "publication_day", "publication_full", "publication_month", "publication_month_name",
                         "publication_month_name", "publication_year")
                 .distinct();

    }

    private Dataset<Row> projectBookColumns(Dataset<Row> df) {
        return df.withColumn("BookID", concat(col("title"), lit("_"), col("author_name")))
                .withColumn("SubjectID", concat(col("subjects"), lit("_S")))
                .drop("subjects");
    }


}

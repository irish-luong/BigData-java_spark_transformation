package com.max.service;

import com.max.model.Car;
import com.max.repository.impl.CarReadRepository;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;


/**
 * Load - clean - transform Spark dataframe
 */
@Service
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public final class CarIngestionService {


    private final CarReadRepository carReadRepository;


    /**
     * Load master sale data
     *
     * @param path location of datastore
     * @return master sale dataset
     */
    public Dataset<Car> loadData(String path) {

        Map<String, String> options = Stream.of(new String[][] {
                {"header", "true"}
        })
                .collect(Collectors.toMap(data -> data[0], data -> data[1]));

        return carReadRepository
                .loadByLocation(path, options)
                .transform(this::transformCarDataset);

    };

    private Dataset<Car> transformCarDataset(Dataset<Row> df) {
        return df.map(new CarMapper(), Encoders.bean(Car.class));
    }

}

class CarMapper implements MapFunction<Row, Car> {

    @Override
    public Car call(Row row) throws Exception {
        Car car = new Car();
        car.setId(row.getAs("Identification.ID"));
        car.setMake(row.getAs("Identification.Make"));
        car.setYear(row.getAs("Identification.Year"));
        car.setTransmission(row.getAs("Fuel_Information.Fuel_Type"));
        car.setFuelType(row.getAs("Engine_Information.Transmission"));
        return car;
    }
}

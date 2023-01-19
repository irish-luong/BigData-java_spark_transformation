package com.max.service;

import com.max.mapper.TransmitterCompanyMapper;
import com.max.mapper.TransmitterSpeedMapper;
import com.max.model.dto.PingAnomaly;
import com.max.repository.impl.FileReadRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import scala.reflect.ClassTag$;


import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Slf4j
@Service
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public final class NetworkService {

    private final FileReadRepository fileReadRepository;

    public Dataset<Row> loadData(String path) {

        Map<String, String> options = Stream.of(
                new String[][] {
                        {"header", "true"},
                        {"inferSchema", "true"}
                })
                .collect(Collectors.toMap(data -> data[0], data -> data[1]));


        TransmitterSpeedMapper transmitterSpeedMapper = new TransmitterSpeedMapper();
        Dataset<Row> df = fileReadRepository.loadByLocation(path, options);

        Dataset<Row> result = df.map(transmitterSpeedMapper, RowEncoder.apply(transmitterSpeedMapper.structType()));

        // Long accumulator
        LongAccumulator longAccumulator = df.sparkSession().sparkContext().longAccumulator();
        df.foreach((ForeachFunction<Row>) row -> {
            if(row.anyNull()){
                longAccumulator.add(1L);
            }

        });

        log.info("======== Null Values ======== " + longAccumulator.value());

        CollectionAccumulator<PingAnomaly> pingAccumulator = df.sparkSession().sparkContext()
                .collectionAccumulator();

        df.foreach((ForeachFunction<Row>) row -> {

            if(!row.anyNull() && row.getInt(2) != 0) {
                pingAccumulator.add(new PingAnomaly(row.getString(0), row.getString(1)));
            }
        });

        log.info("============ Amount of rows with anomalies: ============= ");

        pingAccumulator.value().forEach(ping -> log.info("Source - dest " +
                ping.getSource() + " " + ping.getDestination()));

        log.info("========================================================= ");


        Dataset<Row> slowTransmitters = result.filter((FilterFunction<Row>) row ->
                row.getAs("Category").equals("OVLD"));

        Map<String, String> lookupTable = new HashMap<>();
        lookupTable.put("ans","American Networking");
        lookupTable.put("xty","Expert Transmitters");
        lookupTable.put("xfh","XFH");

        // Create a broadcast variable;
        Broadcast<Map<String, String>> broadcastVariable = df.sparkSession()
                .sparkContext()
                .broadcast(lookupTable, ClassTag$.MODULE$.apply(Map.class));


        df.show();
        TransmitterCompanyMapper transmitterCompanyMapper = new TransmitterCompanyMapper(broadcastVariable);
        Dataset<Row> manufactures = df.map(transmitterCompanyMapper,
                RowEncoder.apply(transmitterCompanyMapper.structType()));

        log.info("============ Result for Manufacturers: ================== ");
        manufactures.show();
        log.info("========================================================= ");

        // Destroy broadcast variable from cluster
        broadcastVariable.destroy();

        return result;
    }

}


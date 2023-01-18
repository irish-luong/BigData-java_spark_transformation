package com.max.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.Objects;


public class TransmitterCompanyMapper extends TransmitterMapperBase implements MapFunction<Row, Row> {

    Broadcast<Map<String, String>> lookupTable;

    public TransmitterCompanyMapper(Broadcast<Map<String, String>> lookupTable) {
        this.lookupTable = lookupTable;
    }

    @Override
    public StructType structType() {
        return new StructType()
                .add("Transmitter_id", DataTypes.StringType)
                .add("Manufacturer", DataTypes.StringType);
    }

    @Override
    public Row call(Row row) throws Exception {

        try {
            String transitID = row.getString(0).substring(0, 3);
            String manufacturer = lookupTable.getValue().get(transitID);
            return produceRow(transitID, manufacturer);
        } catch (NullPointerException | StringIndexOutOfBoundsException e) {
            return produceRow(ERR_VAL, ERR_VAL);
        }

    }
}

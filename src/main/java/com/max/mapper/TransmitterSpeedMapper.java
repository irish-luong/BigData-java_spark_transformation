package com.max.mapper;


import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.MapFunction;


public class TransmitterSpeedMapper extends TransmitterMapperBase implements MapFunction<Row, Row> {

    @Override
    public StructType structType() {
        return new StructType()
                .add("Transmitter_id", DataTypes.StringType)
                .add("Category", DataTypes.StringType);
    }

    @Override
    public Row call(Row row) throws Exception {

        if (row.anyNull()) {
            return produceRow(ERR_VAL, ERR_VAL);
        }

        int ping = row.getInt(2);
        String category = getCategory(ping);
        String transId = row.getString(0);

        return produceRow(transId, category);
    }

    private String getCategory(int ping) {
        if(ping <= 50) {
            return ACCEPTABLE;
        } else if (ping <= 100) {
            return CONGESTED;
        } else {
            return OVERLOADED;
        }
    }
}


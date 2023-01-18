package com.max.mapper;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

public abstract class TransmitterMapperBase {

    protected static final String ERR_VAL = "ERR";
    protected static final String CONGESTED = "CGTD";
    protected static final String ACCEPTABLE = "ACPT";
    protected static final String OVERLOADED = "OVLD";


    protected Row produceRow(String val1, String val2) {

        Object[] rows = new Object[]{val1, val2};

        return new GenericRowWithSchema(rows, structType());
    }

    protected abstract StructType structType();

}

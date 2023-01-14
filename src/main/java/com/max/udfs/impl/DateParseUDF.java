package com.max.udfs.impl;

import com.max.udfs.UdfInterface;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.apache.spark.sql.api.java.UDF2;
import org.joda.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.udf;

import java.util.Date;

public class DateParseUDF implements UDF2<String, String, Date>, UdfInterface {
    @Override
    public Date call(String dateString, String dateFormat) throws Exception {
        DateTimeFormatter formatter = DateTimeFormat.forPattern(dateFormat);
        DateTime dateTime = formatter.parseDateTime(dateString);
        return new java.sql.Date(dateTime.getMillis());
    }

    public static UserDefinedFunction create() {
        return udf(
                new DateParseUDF(),
                DataTypes.DateType
        );
    }

}

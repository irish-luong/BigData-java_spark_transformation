package com.max.udfs;


import com.max.udfs.impl.DateParseUDF;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.expressions.UserDefinedFunction;


@UtilityClass
public class Udfs {

    public static UserDefinedFunction dateParse() { return DateParseUDF.create(); };
}


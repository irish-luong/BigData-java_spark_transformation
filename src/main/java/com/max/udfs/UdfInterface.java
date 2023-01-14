package com.max.udfs;

import org.apache.spark.sql.expressions.UserDefinedFunction;

public interface UdfInterface {


    static UserDefinedFunction create() {
        return null;
    }

}

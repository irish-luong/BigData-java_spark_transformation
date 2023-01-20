package com.max.datasource;


import java.io.Serializable;

public class DerbyDbManager extends BaseDatabaseManager implements Serializable {

    public DerbyDbManager(String url) {
        super(url);
    }
}

package com.max.entity;

public final class SubjectsEntitySpec implements DataEntitySpec{

    public static final String ENTITY_NAME = "subjects";


    @Override
    public String getName() {
        return null;
    }

    @Override
    public String[] getPartitionKeys() {
        return new String[0];
    }

    @Override
    public String[] getUniqueKeys() {
        return new String[0];
    }

    @Override
    public String[] getColumns() {
        return new String[0];
    }
}

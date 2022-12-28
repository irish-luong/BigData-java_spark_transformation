package com.max.entity;

public final class BooksEntitySpec implements DataEntitySpec{

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

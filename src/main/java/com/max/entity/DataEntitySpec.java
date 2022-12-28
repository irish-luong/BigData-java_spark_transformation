package com.max.entity;


/**
 * Dataset output entity spec
 */
public interface DataEntitySpec {

    public static final String ENTITY_NAME = null;

    /**
     * Get name of this entity
     * @return entity name
     */
    String getName();

    /**
     * Get list partition keys
     * @return list of keys use for partitions
     */
    String[] getPartitionKeys();


    /**
     * Get tuple of unique keys
     * @return
     */
    String[] getUniqueKeys();


    /**
     * Get list columns of this entity
     * @return
     */
    String[] getColumns();

}

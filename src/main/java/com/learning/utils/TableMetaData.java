package com.learning.utils;

import java.io.Serializable;
import java.util.List;

public class TableMetaData implements Serializable {
    private static final long serialVersionUID = 1L;

    public TableMetaData() {

    }

    public TableMetaData(String name) {
        this.tableName = name;
    }

    private String tableName;
    private List<TableColumn> columns;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<TableColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<TableColumn> columns) {
        this.columns = columns;
    }
}

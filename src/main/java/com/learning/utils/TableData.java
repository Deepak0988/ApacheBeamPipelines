package com.learning.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TableData implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<Object> data = new ArrayList<>();

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public List<Object> getData() {
        return data;
    }

    public void setData(List<Object> data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableData tableData = (TableData) o;
        return Objects.equals(data, tableData.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public String toString() {
        return "TableData{" +
                "data=" + data +
                '}';
    }
}

package de.ddm.actors.profiling;

import lombok.Getter;

import java.util.HashSet;

public class Column {
    private final int id;
    //To make sure that the values are unique
    private final HashSet<String> columnValues;
    @Getter
    private String columnName;
    @Getter
    private final boolean isString;
    @Getter
    private String nameOfDataset;

    Column(int id, boolean isString, String columnName, String nameOfDataset){
        this.id = id;
        this.isString = isString;
        this.columnName = columnName;
        this.nameOfDataset = nameOfDataset;
        columnValues = new HashSet<>();
    }
    void addValueToColumn(String value){
        columnValues.add(value);
    }

    int getId(){
        return id;
    }

    HashSet<String> getColumnValues(){
        return columnValues;
    }



}

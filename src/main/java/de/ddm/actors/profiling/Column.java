package de.ddm.actors.profiling;

import lombok.Getter;

import java.util.HashSet;

public class Column {
    private int id;
    //To make sure that the values are unique
    private HashSet<String> columnValues;
    private String type;
    @Getter
    private String columnName;
    @Getter
    private String nameOfDataset;

    Column(int id, String type, String columnName, String nameOfDataset){
        this.id = id;
        this.type = type;
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

    void addColumnValues(String columnValues){
        this.columnValues.add(columnValues);
    }

    public String getType() {
    	return this.type;
    }

}

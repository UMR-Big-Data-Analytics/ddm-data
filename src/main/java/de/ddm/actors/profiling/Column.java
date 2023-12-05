package de.ddm.actors.profiling;

import lombok.Getter;

import java.util.HashSet;

public class Column {
    private int id;
    //To make sure that the values are unique
    private HashSet<String> columnValues;
    @Getter
    private String nameOfDataset;
    @Getter
    private String nameOfColumn;

    private String type;

    Column(int id, String type, String nameOfDataset, String nameOfColumn){
        this.id = id;
        this.type = type;
        this.nameOfColumn = nameOfColumn;
        this.nameOfDataset = nameOfDataset;
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

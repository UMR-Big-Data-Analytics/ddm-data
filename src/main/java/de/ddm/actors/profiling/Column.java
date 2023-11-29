package de.ddm.actors.profiling;

import java.util.HashSet;

public class Column {
    private int id;
    private HashSet<String> columnValues;

    Column(int id){
        this.id = id;
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

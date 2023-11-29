package de.ddm.actors.profiling;

import java.util.List;

public class Table {

    private int idNameOfDataset;
    private List<Column> columns;
    private String nameOfDataset;
    public void addColumn(Column column) {
        columns.add(column);
    }

    int amountOfColumns() {
        return columns.size();
    }

    public List<Column> getColumnsList(){
        return columns;
    }

    public String getNameOfDataset(){
        return nameOfDataset;
    }

    public int getIdNameOfDataset(){
        return idNameOfDataset;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public void setNameOfDataset(String nameOfDataset){
        this.nameOfDataset = nameOfDataset;
    }

    public void setIdNameOfDataset(int idNameOfDataset){
        this.idNameOfDataset = idNameOfDataset;
    }

}

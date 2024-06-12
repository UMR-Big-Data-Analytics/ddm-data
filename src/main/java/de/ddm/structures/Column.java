package de.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.beans.ConstructorProperties;
import java.util.HashSet;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class Column {
    private int tableId;
    private String name;
    private HashSet<String> values;

    public Column(int tableId, String name) {
        this.tableId = tableId;
        this.name = name;
        this.values = new HashSet<>();
    }
}
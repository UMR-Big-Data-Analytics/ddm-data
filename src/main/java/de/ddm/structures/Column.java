package de.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class Column {
    private int tableId;
    private String name;
    private List<String> values;
}
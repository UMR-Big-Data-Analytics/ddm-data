package de.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class Table {
    private int id;
    private List<Column> columns = new ArrayList<Column>();
    private boolean fullyLoaded;
    private List<InclusionDependency> dependencies = new ArrayList<InclusionDependency>();
}

package de.ddm.actors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class TaskId {
    ColumnId referencedColumn;
    ColumnId dependentColumn;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskId taskId = (TaskId) o;
        return referencedColumn.equals(taskId.referencedColumn) && dependentColumn.equals(taskId.dependentColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(referencedColumn, dependentColumn);
    }
}
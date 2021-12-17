package de.ddm.actors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class TaskId {
    int referencedFileId;
    int dependentFileId;
    int referencedFileColumnId;
    int dependentFileColumnId;

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TaskId taskId = (TaskId) o;

        if (referencedFileId != taskId.referencedFileId)
            return false;
        if (dependentFileId != taskId.dependentFileId)
            return false;
        if (referencedFileColumnId != taskId.referencedFileColumnId)
            return false;
        return dependentFileColumnId == taskId.dependentFileColumnId;
    }

    @Override
    public int hashCode() {
        int result = referencedFileId;
        result = 31 * result + dependentFileId;
        result = 31 * result + referencedFileColumnId;
        result = 31 * result + dependentFileColumnId;
        return result;
    }
}
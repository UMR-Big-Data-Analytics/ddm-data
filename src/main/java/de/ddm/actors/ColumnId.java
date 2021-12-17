package de.ddm.actors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class ColumnId {
    int fileId;
    int columnId;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnId columnId1 = (ColumnId) o;
        return fileId == columnId1.fileId && columnId == columnId1.columnId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileId, columnId);
    }

    public boolean isDifferentFile(ColumnId columnId) {
        return columnId.getFileId() != this.fileId;
    }

    @Override
    public String toString() {
        return "ColumnId{" +
                "fileId=" + fileId +
                ", columnId=" + columnId +
                '}';
    }
}

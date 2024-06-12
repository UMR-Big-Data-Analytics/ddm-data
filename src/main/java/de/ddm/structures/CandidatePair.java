package de.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class CandidatePair {
    private int firstTableIndex;
    private String firstColumnName;
    private int secondTableIndex;
    private String secondColumnName;
}
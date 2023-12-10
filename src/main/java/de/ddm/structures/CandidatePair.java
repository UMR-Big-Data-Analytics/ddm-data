package de.ddm.structures;

import java.util.List;

public class CandidatePair {
    private final int firstTableIndex;
    private final int firstColumnIndex;
    private final int secondTableIndex;
    private final int secondColumnIndex;


    public CandidatePair(int firstTableIndex, int firstColumnIndex,
                         int secondTableIndex, int secondColumnIndex) {
        this.firstTableIndex = firstTableIndex;
        this.firstColumnIndex = firstColumnIndex;
        this.secondTableIndex = secondTableIndex;
        this.secondColumnIndex = secondColumnIndex;
    }

    public int getFirstTableIndex() {
        return this.firstTableIndex;
    }

    public int getFirstColumnIndex() {
        return this.firstColumnIndex;
    }



    public int getSecondTableIndex() {
        return this.secondTableIndex;
    }

    public int getSecondColumnIndex() {
        return this.secondColumnIndex;
    }

}


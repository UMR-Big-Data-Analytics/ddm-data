package de.ddm.actors.profiling;
import de.ddm.structures.InclusionDependency;
import lombok.Getter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Results {
    private File dependent;

    private File referenced;

    String[] dependentAttributes;
    String[] referencedAttributes;
    @Getter
    List<InclusionDependency> inclusionDependencies;

    public Results(File dependent, File referenced, String[] dependentAttributes, String[] referencedAttributes) {
        this.dependent = dependent;
        this.referenced = referenced;
        this.dependentAttributes = dependentAttributes;
        this.referencedAttributes = referencedAttributes;
        InclusionDependency inclusionDependency = new InclusionDependency(dependent, dependentAttributes,referenced, referencedAttributes);
        inclusionDependencies = new ArrayList<>();
        inclusionDependencies.add(inclusionDependency);
    }


}

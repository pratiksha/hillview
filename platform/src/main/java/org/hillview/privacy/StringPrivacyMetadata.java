package org.hillview.privacy;

import org.hillview.dataset.api.IJson;

public class StringPrivacyMetadata extends PrivacyMetadata implements IJson {
    /**
     * Explicit left boundaries for leaves.
     * These must be in lexicographic order.
     */
    public String[] leafLeftBoundaries;

    /**
     * End of the rightmost bucket.
     */
    public String globalMax;

    public StringPrivacyMetadata(double epsilon, String[] leafLeftBoundaries, String globalMax) {
        super(epsilon);

        for (int i = 0; i < leafLeftBoundaries.length - 1; i++) {
            if (leafLeftBoundaries[i].compareTo(leafLeftBoundaries[i+1]) >= 0) { // TODO equality might be ok here
                throw new RuntimeException("Leaf boundaries for string histogram must be in lexicographic order");
            }
        }

        this.leafLeftBoundaries = leafLeftBoundaries;
        this.globalMax = globalMax;
    }
}

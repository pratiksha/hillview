package org.hillview.sketches;

import org.hillview.dataset.api.Pair;
import org.hillview.table.api.IColumn;
import org.hillview.privacy.StringPrivacyMetadata;

import java.util.ArrayList;

public class StringPrivateBuckets extends TreeHistogramBuckets<String> {
    private String[] leafLeftBoundaries;

    public StringPrivateBuckets(final String minValue, final String maxValue,
                                final int numOfBuckets, StringPrivacyMetadata metadata) {
        super(minValue, maxValue, metadata.leafLeftBoundaries[0], metadata.globalMax,
               "a", numOfBuckets);

        this.leafLeftBoundaries = metadata.leafLeftBoundaries;
        this.numLeaves = this.leafLeftBoundaries.length;

        this.bucketLeftBoundaries = new String[this.numOfBuckets];

        init();
    }

    @Override
    protected String leafLeftBoundary(final int leafIdx) {
        if (leafIdx < this.leafLeftBoundaries.length) {
            return this.leafLeftBoundaries[leafIdx];
        } else if (leafIdx == this.leafLeftBoundaries.length) {
            return this.globalMax;
        } else {
            throw new ArrayIndexOutOfBoundsException();
        }
    }

    @Override
    protected ArrayList<Pair<Integer, Integer>> bucketDecomposition(int bucketIdx, boolean cdf) {
        int startLeaf = this.bucketLeftLeaves[bucketIdx];
        int endLeaf;
        if (bucketIdx == this.numOfBuckets) { // last bucket
            endLeaf = this.numLeaves;
        } else {
            endLeaf = this.bucketLeftLeaves[bucketIdx+1];
        }

        if (startLeaf >= endLeaf) {
            throw new RuntimeException("Tried to initialize bucket with invalid number of leaves");
        }

        ArrayList<Pair<Integer, Integer>> ret = new ArrayList<>();
        for (int i = startLeaf; i < endLeaf; i++) {
            ret.add(new Pair(i, i+1));
        }

        return ret;
    }

    @Override
    public int indexOf(IColumn column, int rowIndex) {
        String item = column.asString(rowIndex);
        return this.indexOf(item);
    }
}

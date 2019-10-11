package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.results.BucketsInfo;
import org.hillview.sketches.results.DataRange;
import org.hillview.sketches.results.StringBucketLeftBoundaries;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.JsonList;

import javax.annotation.Nullable;

/**
 * This class is mainly for use with the PrecomputedSketch.
 * Since the notion of adding a set of left boundaries is ill-defined, the add function does nothing here.
 */
public class StringBoundariesSketch implements ISketch<ITable, BucketsInfo> {
    private final String col;

    public StringBoundariesSketch(String col) {
        this.col = col;
    }

    @Override
    public BucketsInfo create(@Nullable final ITable data) {
        IColumn column = Converters.checkNull(data).getLoadedColumn(this.col);
        DataRange result = new DataRange();
        final IRowIterator myIter = data.getMembershipSet().getIterator();
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            if (!column.isMissing(currRow)) {
                double val = column.asDouble(currRow);
                result.add(val);
            } else {
                result.addMissing();
            }
            currRow = myIter.getNextRow();
        }
        return result;
    }

    @Override
    public BucketsInfo zero() { return new StringBucketLeftBoundaries(new JsonList<String>(), "", true, 0, 0); }

    @Override
    public BucketsInfo add(@Nullable final BucketsInfo left, @Nullable final BucketsInfo right) {
        return left;
    }
}

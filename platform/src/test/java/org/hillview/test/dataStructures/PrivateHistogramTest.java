package org.hillview.test.dataStructures;

import org.hillview.sketches.DoubleHistogramBuckets;
import org.hillview.sketches.Histogram;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.test.BaseTest;
import org.hillview.test.table.DoubleArrayTest;
import org.junit.Test;

public class PrivateHistogramTest extends BaseTest {
    @Test
    public void testPrivateHistogram() {
        final int bucketNum = 110;
        final int colSize = 10000;

        DoubleHistogramBuckets buckDes = new DoubleHistogramBuckets(0, 100, bucketNum);

        Histogram hist = new Histogram(buckDes);
        DoubleArrayColumn col = DoubleArrayTest.generateDoubleArray(colSize, 100);
        FullMembershipSet fMap = new FullMembershipSet(colSize);
        hist.create(col, fMap, 1.0, 0, false);
        int size = 0;
        for (int i = 0; i < bucketNum; i++)
            size += hist.getCount(i);

        Histogram hist1 = new Histogram(buckDes);
        DoubleArrayColumn col1 = DoubleArrayTest.generateDoubleArray(2 * colSize, 100);
        FullMembershipSet fMap1 = new FullMembershipSet(2 * colSize);
        hist1.create(col1, fMap1, 1.0, 0, false);

        Histogram hist2 = hist1.union(hist);
        size = 0;
        for (int i = 0; i < bucketNum; i++)
            size += hist2.getCount(i);

        Histogram hist3 = new Histogram(buckDes);
        hist3.create(col, fMap, 0.1, 0, false);
        size = 0;
        for (int i = 0; i < bucketNum; i++)
            size += hist3.getCount(i);

        Histogram privateHist = hist3.addLaplaceNoise(0.3);
    }
}

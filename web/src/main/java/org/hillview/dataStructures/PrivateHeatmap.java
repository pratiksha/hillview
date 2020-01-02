package org.hillview.dataStructures;

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;
import org.hillview.security.SecureLaplace;
import org.hillview.sketches.results.Heatmap;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PrivateHeatmap implements Serializable, IJson {
    public Heatmap heatmap;
    private double epsilon;
    private SecureLaplace laplace;
    static final boolean coarsen = false;
    long totalLeaves;
    double scale;
    double baseVariance;

    private IntervalDecomposition dx;
    private IntervalDecomposition dy;

    private boolean uncertainCells[][];

    public PrivateHeatmap(IntervalDecomposition d0, IntervalDecomposition d1,
                          Heatmap heatmap, double epsilon, SecureLaplace laplace) {
        this.heatmap = heatmap;
        this.epsilon = epsilon;
        this.laplace = laplace;

        this.dx = d0;
        this.dy = d1;

        this.totalLeaves = (1 + d0.getQuantizationIntervalCount()) *
                (1 + d1.getQuantizationIntervalCount());  // +1 for the NULL bucket
        double scale = Math.log(totalLeaves) / Math.log(2);
        scale /= epsilon;
        this.scale = scale;
        this.baseVariance = 2 * (Math.pow(scale, 2));

        int xSize = this.heatmap.xBucketCount;
        int ySize = this.heatmap.yBucketCount;

        this.uncertainCells = new boolean[xSize][ySize];

        this.addDyadicLaplaceNoise(d0, d1);

        this.greedyXCoarsen();

        for (int i2 = 0; i2 < xSize; i2++) {
            for (int j = 0; j < ySize; j++) {
                this.heatmap.buckets[i2][j] = (long)this.heatmap.privateBuckets[i2][j];
            }
        }
    }

    /**
     * Compute mean to add to this bucket using the dyadic decomposition as the PRG seed.
     * If cdfBuckets is true, computes the mean based on the dyadic decomposition of the interval [0, bucket right leaf]
     * rather than [bucket left leaf, bucket right leaf].
     * Returns the mean and the total variance of the variables used to compute the mean.
     */
    private void noiseForDecomposition(
            List<Pair<Integer, Integer>> xIntervals,
            List<Pair<Integer, Integer>> yIntervals,
            double scale,
            double baseVariance,
            /*out*/Noise result) {
        result.clear();
        for (Pair<Integer, Integer> x : xIntervals) {
            for (Pair<Integer, Integer> y : yIntervals) {
                result.mean += this.laplace.sampleLaplace(x, y, scale);
                result.variance += baseVariance;
            }
        }
    }

    private static boolean notConfident(double value, double threshold) {
        return value < threshold;
    }

    private void noiseForRange(int x0, int x1, int y0, int y1, double scale, double baseVariance, Noise noise) {
        List<Pair<Integer, Integer>> xIntervals = IntervalDecomposition.kadicDecomposition(x0, x1, IntervalDecomposition.BRANCHING_FACTOR);
        List<Pair<Integer, Integer>> yIntervals = IntervalDecomposition.kadicDecomposition(y0, y1, IntervalDecomposition.BRANCHING_FACTOR);
        noiseForDecomposition(xIntervals, yIntervals, scale, baseVariance, noise);
    }

    private void noiseForRange(int x0, int x1, int y0, int y1, Noise noise) {
        this.noiseForRange(x0, x1, y0, y1, this.scale, this.baseVariance, noise);
    }

    private void addDyadicLaplaceNoise(IntervalDecomposition dx, IntervalDecomposition dy) {
        HillviewLogger.instance.info("Adding heatmap mean with", "epsilon={0}", this.epsilon);
        int xSize = this.heatmap.xBucketCount;
        int ySize = this.heatmap.yBucketCount;

        // Precompute all the intervals for efficient computation
        List<List<Pair<Integer, Integer>>> xIntervals = new ArrayList<List<Pair<Integer, Integer>>>(xSize);
        List<List<Pair<Integer, Integer>>> yIntervals = new ArrayList<List<Pair<Integer, Integer>>>(ySize);
        for (int i = 0; i < xSize; i++)
            xIntervals.add(dx.bucketDecomposition(i, false));
        for (int i = 0; i < ySize; i++)
            yIntervals.add(dy.bucketDecomposition(i, false));

        // Compute the mean.
        Noise noise = new Noise();
        this.heatmap.allocateConfidence();
        Converters.checkNull(this.heatmap.confidence);

        for (int i = 0; i < this.heatmap.buckets.length; i++) {
            for (int j = 0; j < this.heatmap.buckets[i].length; j++) {
                this.noiseForDecomposition(xIntervals.get(i), yIntervals.get(j), this.scale, this.baseVariance, noise);
                this.heatmap.privateBuckets[i][j] = this.heatmap.buckets[i][j] + noise.mean;
                this.heatmap.confidence[i][j] = noise.getConfidence();
                this.uncertainCells[i][j] = notConfident(this.heatmap.privateBuckets[i][j], this.heatmap.confidence[i][j]);
            }
        }
    }

    private boolean mergeXBuckets(int leftBucket, int rightBucket, int rowIdx) {
        assert (leftBucket < rightBucket);

        Pair<Integer, Integer> leftRange = this.dx.bucketRange(leftBucket, false);
        Pair<Integer, Integer> rightRange = this.dx.bucketRange(rightBucket, false);

        int x0 = leftRange.first;
        int x1 = rightRange.second;

        Noise noise = new Noise();
        this.noiseForRange(x0, x1, rowIdx, rowIdx+1, noise);
        int nBuckets = rightBucket - leftBucket;
        long totalValue = 0;
        for (int i = leftBucket; i < rightBucket; i++) {
            totalValue += this.heatmap.buckets[i][rowIdx];
        }

        double noisyCount = totalValue + noise.mean;

        for (int i = leftBucket; i < rightBucket; i++) {
            this.heatmap.privateBuckets[i][rowIdx] = noisyCount / nBuckets; // averaging is postprocessing, so ok
            this.heatmap.confidence[i][rowIdx] = noise.getConfidence(); // TODO not sure if this is exactly right
        }

        if (!notConfident(noisyCount, noise.getConfidence())) {
            HillviewLogger.instance.info("done! x0: " + x0 + " x1: " + x1 + " total: " + totalValue);
        }

        // Note that this "confidence" has to be computed using the noisy count, not the true count,
        // as everything here is postprocessing on the private, noisy histogram.
        // However, it is ok to recompute the noisy count on top of the true count,
        // as every possible count in the histogram has been released using the dyadic decomposition.
        return notConfident(noisyCount, noise.getConfidence());
    }

    /**
     * Coarsen left to right in the rows only.
     */
    private void greedyXCoarsen() {
        int xSize = this.heatmap.xBucketCount;
        int ySize = this.heatmap.yBucketCount;

        //for (int i = 0; i < xSize; i++) {
        for (int j = 0; j < ySize; j++) {
            int i = 0;
            while (i < xSize) {
                int rectSize = 1;
                while ((i + rectSize < xSize) &&
                        //uncertainCells[i + rectSize][j] && // only merge non-confident cells
                        (mergeXBuckets(i, i+rectSize, j))) {
                    rectSize++;
                }
                i += rectSize;
            }
        }
    }
}

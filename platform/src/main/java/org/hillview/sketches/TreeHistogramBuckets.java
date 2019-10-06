/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.sketches;

import org.hillview.dataset.api.Pair;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * This bucket class is intended for use with the binary mechanism for differential privacy
 * (Chan, Song, Shi TISSEC '11: https://eprint.iacr.org/2010/076.pdf) on numeric data.
 * The values are quantized to multiples of the granularity specified by the data curator (to create "leaves"
 * in the private range query tree). Since bucket boundaries may not fall on the quantized leaf boundaries,
 * leaves are assigned to buckets based on their left boundary value.
 */
public abstract class TreeHistogramBuckets<T extends Comparable<T>> implements IHistogramBuckets {
    // Range for this (possibly filtered) histogram.
    protected T minValue;
    protected T maxValue;

    // Range for the base (unfiltered) histogram. Used to compute appropriate amount of noise to add.
    public T globalMin;
    public T globalMax;

    public T zeroValue;

    protected int numOfBuckets;

    protected int numLeaves; // Number of leaves in filtered histogram.
    protected int globalNumLeaves; // Number of leaves in base histogram.

    /**
     * Covers leaves in [minLeafIdx, minLeafIdx + numLeaves). For infinite ranges, these values are absolute and centered
     * at zeroValue (i.e. minLeafIdx can be negative).
     */
    protected int minLeafIdx;

    protected int[] bucketLeftLeaves; // boundaries in terms of relative leaf indices

    /**
     * Boundaries in terms of leaf boundary values (for faster search).
     * Note that this is only used for computing bucket membership. The UI will render buckets as if they were
     * evenly spaced within the specified interval, as usual.
     */
    protected T[] bucketLeftBoundaries;

    // For testing
    public long bucketLeafIdx(final int bucketIdx) {
        if (bucketIdx < 0 || bucketIdx > this.numOfBuckets - 1) return -1;
        return this.bucketLeftLeaves[bucketIdx] + this.minLeafIdx;
    }

    protected abstract T leafLeftBoundary(final int leafIdx);

    /**
     * Assign leaves appropriately to this.numOfBuckets buckets such that each bucket contains approximately
     * the same number of leaves.
     */
    protected void populateBucketBoundaries() {
        double leavesPerBucket = (double)(this.numLeaves) / this.numOfBuckets; // approximate leaves per bucket
        int defaultLeaves = (int)Math.floor(leavesPerBucket);
        int overflow = (int)Math.ceil(1.0/(leavesPerBucket - defaultLeaves)); // interval at which to add extra leaf

        int prevLeafIdx = 0;
        this.bucketLeftLeaves[0] = prevLeafIdx;
        for (int i = 1; i < this.numOfBuckets; i++) {
            if (overflow > 0 && i % overflow == 0) {
                this.bucketLeftLeaves[i] = prevLeafIdx + defaultLeaves + 1;
            } else {
                this.bucketLeftLeaves[i] = prevLeafIdx + defaultLeaves;
            }

            prevLeafIdx = this.bucketLeftLeaves[i];
        }

        for (int i = 0; i < this.bucketLeftBoundaries.length; i++) {
            this.bucketLeftBoundaries[i] = leafLeftBoundary(this.minLeafIdx + this.bucketLeftLeaves[i]);
        }
    }

    /**
     * Compute the largest leaf whose left boundary is <= min
     */
    protected int computeMinLeafIdx(T min) {
        int minIdx = 0;
        if (min.compareTo(zeroValue) < 0) { // Should never be true for categorical values.
            while (this.leafLeftBoundary(minIdx).compareTo(min) > 0) {
                minIdx--;
            }
        } else {
            while (this.leafLeftBoundary(minIdx).compareTo(min) < 0) {
                minIdx++;
            }
            if (this.leafLeftBoundary(minIdx).compareTo(min) > 0) minIdx--;
        }

        return minIdx;
    }

    /**
     * @param minValue the minimum value in the filtered histogram.
     * @param maxValue the maximum value in the filtered histogram.
     * @param globalMin the minimum value for the unfiltered histogram.
     * @param globalMax the maximum value for the unfiltered histogram.
     * @param zeroValue the value corresponding to zero for the domain T.
     * @param numOfBuckets the number of histogram buckets.
     */
    public TreeHistogramBuckets(final T minValue, final T maxValue,
                                final T globalMin, final T globalMax,
                                final T zeroValue,
                                final int numOfBuckets) {
        if (maxValue.compareTo(minValue) < 0 || numOfBuckets <= 0)
            throw new IllegalArgumentException("Negative range or number of buckets");

        if (maxValue.compareTo(minValue) <= 0)
            this.numOfBuckets = 1;  // ignore specified number of buckets
        else
            this.numOfBuckets = numOfBuckets;

        this.bucketLeftLeaves = new int[this.numOfBuckets];

        this.zeroValue = zeroValue;

        this.globalMax = globalMax;
        this.globalMin = globalMin;

        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    /**
     * Additional initialization that calls abstract methods, so may depend on subclass variables.
     */
    protected void init() {
        this.minLeafIdx = computeMinLeafIdx(this.minValue);

        populateBucketBoundaries();
    }

    /**
     * Return the index of the leaf that contains value.
     */
    public int indexOf(T value) {
        if ((value.compareTo(this.minValue) < 0) || (value.compareTo(this.maxValue) > 0))
            return -1;

        int index = Arrays.binarySearch(this.bucketLeftBoundaries, value);
        // This method returns index of the search key, if it is contained in the array,
        // else it returns (-(insertion point) - 1). The insertion point is the point
        // at which the key would be inserted into the array: the index of the first
        // element greater than the key, or a.length if all elements in the array
        // are less than the specified key.
        if (index < 0) {
            index = -index - 1;
            if (index == 0)
                // before first element
                return -1;
            return index - 1;
        }
        return index;
    }

    /**
     * Get the number of leaves that would correspond to this bucket.
     * If bucket boundaries and leaf boundaries don't align,
     * we assign leaves to the bucket that their left boundary falls in.
     */
    public long numLeavesInBucket(int bucketIdx) {
        if (bucketIdx >= this.numOfBuckets || bucketIdx < 0) throw new IllegalArgumentException("Invalid bucket index");
        if (bucketIdx == this.numOfBuckets - 1) {
            return this.numLeaves - this.bucketLeftLeaves[bucketIdx];
        }

        return (this.bucketLeftLeaves[bucketIdx + 1] -
                this.bucketLeftLeaves[bucketIdx]);
    }

    /**
     * Return a list of leaf intervals that correspond to this bucket.
     * For numeric data, this could correspond to the dyadic decomposition of the bucket.
     * For categorical data (one-level tree), this can return a list of leaves that make up the bucket.
     * Leaves are zero-indexed. The returned intervals are right-exclusive.
     */
    abstract ArrayList<Pair<Integer, Integer>> bucketDecomposition(int bucketIdx, boolean cdf);

    public T getMin() { return this.minValue; }
    public T getMax() { return this.maxValue; }

    @Override
    public int getNumOfBuckets() { return this.numOfBuckets; }

    public int getGlobalNumLeaves() { return this.globalNumLeaves; }
}

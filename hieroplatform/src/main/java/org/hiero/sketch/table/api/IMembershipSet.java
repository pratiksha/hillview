/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
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
 *
 */

package org.hiero.sketch.table.api;

import org.hiero.utils.Randomness;
import org.hiero.sketch.table.SparseMembership;
import org.hiero.utils.IntSet;

import java.util.function.Predicate;

/**
 * A IMembershipSet is a representation of a set of integers.
 * These integers represent row indexes in a table.  If an integer
 * is in an IMembershipSet, then it is present in the table.
 */
public interface IMembershipSet extends IRowOrder {
    /**
     * @param rowIndex A non-negative row index.
     * @return True if the given rowIndex is a member of the set.
     */
    boolean isMember(int rowIndex);

    /**
     * @return an IMembershipSet containing k samples from the membership map. The samples are made
     * without replacement. Returns the full set if its size is smaller than k. There is no guarantee that
     * two subsequent samples return the same sample set.
     */
    IMembershipSet sample(int k);

    /**
     * Return a membership containing only the rows in the current one where
     * the predicate evaluates to true.
     * @param predicate  Predicate evaluated for each row.
     */
    IMembershipSet filter(Predicate<Integer> predicate);

    /**
     * @return an IMembershipSet containing k samples from the membership map. The samples are made
     * without replacement. Returns the full set if its size is smaller than k. The pseudo-random
     * generator is seeded with parameter seed.
     */
    IMembershipSet sample(int k, long seed);

    /**
     * @return a sample of size (rate * rowCount). randomizes between the floor and ceiling of this expression.
     */
    default IMembershipSet sample(double rate) {
        return this.sample(this.getSampleSize(rate, 0, false));
    }

    /**
     * @return same as sample(double rate) but with the seed for randomness specified by the caller.
     */
    default IMembershipSet sample(double rate, long seed) {
        return this.sample(this.getSampleSize(rate, seed, true), seed);
    }

    /**
     * @return a new map which is the union of current map and otherMap.
     */
    IMembershipSet union(IMembershipSet otherMap);

    IMembershipSet intersection(IMembershipSet otherMap);

    default IMembershipSet setMinus(IMembershipSet otherMap) {
        final IntSet setMinusSet = new IntSet();
        final IRowIterator iter = this.getIterator();
        int curr = iter.getNextRow();
        while (curr >= 0) {
            if (!otherMap.isMember(curr))
                setMinusSet.add(curr);
            curr = iter.getNextRow();
        }
        return new SparseMembership(setMinusSet);
    }

    default int getSampleSize(double rate, long seed, boolean useSeed) {
        if (rate >= 1)
            return this.getSize();
        Randomness r = Randomness.getInstance();
        if (useSeed)
            r.setSeed(seed);
        final int sampleSize;
        final double appSampleSize = rate * this.getSize();
        if (r.nextDouble() < (appSampleSize - Math.floor(appSampleSize)))
            sampleSize = (int) Math.floor(appSampleSize);
        else sampleSize = (int) Math.ceil(appSampleSize);
        return sampleSize;
    }
}
//TODO: Add a split membership set method to split a table into smaller tables.

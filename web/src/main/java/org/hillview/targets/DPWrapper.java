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

package org.hillview.targets;

import org.hillview.dataStructures.IDyadicDecomposition;
import org.hillview.dataStructures.NumericDyadicDecomposition;
import org.hillview.dataStructures.PrivacySchema;
import org.hillview.dataStructures.StringDyadicDecomposition;
import org.hillview.dataset.api.IJson;
import org.hillview.sketches.DoubleDataRangeSketch;
import org.hillview.sketches.HistogramSketch;
import org.hillview.sketches.PrecomputedSketch;
import org.hillview.sketches.StringBoundariesSketch;
import org.hillview.sketches.results.*;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnPrivacyMetadata;
import org.hillview.table.columns.DoubleColumnPrivacyMetadata;
import org.hillview.table.columns.StringColumnPrivacyMetadata;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.utils.JsonList;

import javax.annotation.Nullable;
import javax.swing.text.StyledEditorKit;
import java.io.File;
import java.util.HashMap;

/**
 * This class offers support for differentially-private queries on a data source.
 */
class DPWrapper {
    // For each column the range allowed after filtering
    final HashMap<String, RangeFilterDescription> columnLimits;
    /* Global parameters for differentially-private histograms using the binary mechanism. */
    protected PrivacySchema privacySchema;

    DPWrapper(PrivacySchema privacySchema) {
        this.columnLimits = new HashMap<String, RangeFilterDescription>();
        this.privacySchema = privacySchema;
    }

    DPWrapper(DPWrapper other) {
        this.privacySchema = other.privacySchema;
        this.columnLimits = new HashMap<String, RangeFilterDescription>(other.columnLimits);
    }

    /**
     * If a dataset composed of files is private, we expect that a corresponding directory exists at the root server
     * with a matching name and with such a file inside.
     */
    private static final String PRIVACY_METADATA_NAME = "privacy_metadata.json";

    /**
     * If the privacy metadata file exists return the file name.
     * @param folder  Folder where we look for the file.
     * @return  null if the file does not exist, the file name otherwise.
     */
    @Nullable
    static String privacyMetadataFile(String folder) {
        File metadataFile = new File(folder, PRIVACY_METADATA_NAME);
        if (metadataFile.getAbsoluteFile().exists())
            return metadataFile.toString();
        return null;
    }

    public static class PrivacySummary implements IJson {
        @Nullable
        public Schema schema;
        public long rowCount;
        @Nullable
        public PrivacySchema metadata;
    }

    PrivacySummary addPrivateMetadata(TableSummary summary) {
        PrivacySummary pSumm = new PrivacySummary();
        pSumm.schema = summary.schema;
        pSumm.metadata = this.privacySchema;
        // TODO: add noise to the row count too.
        pSumm.rowCount = summary.rowCount;
        return pSumm;
    }

    static class PrivateHistogramArgs {
        ColumnDescription cd = new ColumnDescription();
        double samplingRate = 1.0; // Fix to exact count
        long seed;

        double min;
        double max;

        @Nullable
        String minString;
        @Nullable
        String maxString; /* TODO this would be better as a generic class with dispatch for JSON. */

        int bucketCount;

        IDyadicDecomposition getDecomposition(ColumnPrivacyMetadata metadata) {
            if (cd.kind.isNumeric()) {
                // This bucket class ensures that computed buckets fall on leaf boundaries.
                return new NumericDyadicDecomposition(this.min, this.max,
                        this.bucketCount, (DoubleColumnPrivacyMetadata) metadata);
            } else if (cd.kind.isString()) {
                return new StringDyadicDecomposition(this.minString, this.maxString,
                        this.bucketCount, (StringColumnPrivacyMetadata) metadata);
            } else {
                throw new RuntimeException("Private buckets not supported for column class");
            }
        }

        // This bucket class ensures that computed buckets fall on leaf boundaries.
        HistogramSketch getSketch(ColumnPrivacyMetadata metadata) {
            IDyadicDecomposition dd = this.getDecomposition(metadata);
            return new HistogramSketch(dd.getHistogramBuckets(), this.cd.name,
                    this.samplingRate, this.seed, null);
        }
    }

    public PrecomputedSketch<ITable, BucketsInfo> getColumnRange(ColumnDescription cd) {
        if (cd.kind.isString()) {
            String min, max;
            StringColumnPrivacyMetadata md = (StringColumnPrivacyMetadata)this.privacySchema.get(cd.name);
            RangeFilterDescription filter = this.columnLimits.get(cd.name);
            if (filter == null) {
                min = md.leftBoundaries[0];
                max = md.globalMax;
            } else {
                min = md.roundDown(filter.minString);
                max = md.roundDown(filter.maxString);
            }

            JsonList<String> filteredBoundaries = new JsonList<>();
            for (int i = 0; i < md.leftBoundaries.length; i++) {
                if (md.leftBoundaries[i].compareTo(min) >= 0 && md.leftBoundaries[i].compareTo(max) < 0) {
                    filteredBoundaries.add(md.leftBoundaries[i]);
                }
            }
            StringBucketLeftBoundaries leftBoundaries =
                    new StringBucketLeftBoundaries(filteredBoundaries, max, true, -1, -1); // TODO present/missing count

            PrecomputedSketch<ITable, BucketsInfo> sk =
                    new PrecomputedSketch<>(leftBoundaries, new StringBoundariesSketch(cd.name));

            return sk;
        } else {
            double min, max;
            DoubleColumnPrivacyMetadata md = (DoubleColumnPrivacyMetadata)this.privacySchema.get(cd.name);
            RangeFilterDescription filter = this.columnLimits.get(cd.name);
            if (filter == null) {
                min = md.globalMin;
                max = md.globalMax;
            } else {
                min = md.roundDown(filter.min);
                max = md.roundUp(filter.max);
            }

            DataRange retRange = new DataRange(min, max);
            retRange.presentCount = -1;
            retRange.missingCount = -1;

            PrecomputedSketch<ITable, BucketsInfo> sk =
                    new PrecomputedSketch<>(retRange, new DoubleDataRangeSketch(cd.name));

            return sk;
        }
    }
}

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

package org.hillview;

import com.google.gson.*;
import org.hillview.dataStructures.*;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.dataset.api.Pair;
import org.hillview.maps.FindFilesMap;
import org.hillview.maps.LoadFilesMap;
import org.hillview.security.SecureLaplace;
import org.hillview.security.TestKeyLoader;
import org.hillview.sketches.HeatmapSketch;
import org.hillview.sketches.HistogramSketch;
import org.hillview.sketches.SummarySketch;
import org.hillview.sketches.results.DoubleHistogramBuckets;
import org.hillview.sketches.results.Heatmap;
import org.hillview.sketches.results.Histogram;
import org.hillview.sketches.results.TableSummary;
import org.hillview.storage.FileSetDescription;
import org.hillview.storage.IFileReference;
import org.hillview.table.ColumnDescription;
import org.hillview.table.PrivacySchema;
import org.hillview.table.Schema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;

import org.hillview.table.columns.DoubleColumnQuantization;

import org.hillview.table.columns.StringColumnQuantization;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;

@SuppressWarnings("FieldCanBeLocal")
public class HistogramAccuracyTest {
    private static String ontime_directory = "../data/ontime_private/";
    private static String privacy_metadata_name = "privacy_metadata.json";

    private static String results_filename = "../results/ontime_private_results.json";

    @Nullable
    IDataSet<ITable> loadData() {
        try {
            FileSetDescription fsd = new FileSetDescription();
            fsd.fileNamePattern = "../data/ontime_private/????_*.csv*";
            fsd.fileKind = "csv";
            fsd.schemaFile = "short.schema";

            Empty e = Empty.getInstance();
            LocalDataSet<Empty> local = new LocalDataSet<Empty>(e);
            IMap<Empty, List<IFileReference>> finder = new FindFilesMap(fsd);
            IDataSet<IFileReference> found = local.blockingFlatMap(finder);
            IMap<IFileReference, ITable> loader = new LoadFilesMap();
            return found.blockingMap(loader);
        }  catch (Exception ex) {
            // This can happen if the data files have not been generated
            return null;
        }
    }

    Schema loadSchema(IDataSet<ITable> data) {
        SummarySketch sk = new SummarySketch();
        TableSummary tableSummary = data.blockingSketch(sk);
        assert tableSummary != null;
        if (tableSummary.rowCount == 0)
            throw new RuntimeException("No file data loaded");
        return Converters.checkNull(tableSummary.schema);
    }

    void generateHeatmap(int xBuckets, int yBuckets, PrivacySchema ps, IDataSet<ITable> table) {
        String col0 = "DepDelay";
        String col1 = "ArrDelay";
        DoubleColumnQuantization q0 = (DoubleColumnQuantization)ps.quantization(col0);
        DoubleColumnQuantization q1 = (DoubleColumnQuantization)ps.quantization(col1);
        Converters.checkNull(q0);
        Converters.checkNull(q1);
        double epsilon = ps.epsilon(col0, col1);
        DoubleHistogramBuckets b0 = new DoubleHistogramBuckets(q0.globalMin, q0.globalMax, xBuckets);
        DoubleHistogramBuckets b1 = new DoubleHistogramBuckets(q1.globalMin, q1.globalMax, yBuckets);
        IntervalDecomposition d0 = new NumericIntervalDecomposition(q0, b0);
        IntervalDecomposition d1 = new NumericIntervalDecomposition(q1, b1);
        HeatmapSketch sk = new HeatmapSketch(b0, b1, col0, col1, 1.0, 0, q0, q1);
        Heatmap h = table.blockingSketch(sk);
        Assert.assertNotNull(h);
        TestKeyLoader tkl = new TestKeyLoader();
        SecureLaplace laplace = new SecureLaplace(tkl);
        PrivateHeatmap ph = new PrivateHeatmap(d0, d1, h, epsilon, laplace);
        Assert.assertNotNull(ph);
        h = ph.heatmap;
        Assert.assertNotNull(h);
    }

    @Test
    public void coarsenHeatmap() {
        @Nullable
        IDataSet<ITable> table = this.loadData();
        if (table == null) {
            System.out.println("Skipping test: no data");
            return;
        }

        PrivacySchema ps = PrivacySchema.loadFromFile(ontime_directory + privacy_metadata_name);
        Assert.assertNotNull(ps);
        Assert.assertNotNull(ps.quantization);
        this.generateHeatmap(220, 110, ps, table);
        this.generateHeatmap(110, 55, ps, table);
    }

    /**
     * Compute the absolute and L2 error vs. ground truth for an instantiation of a private histogram
     * averaged over all possible range queries.
     *
     * @param ph The histogram whose accuracy we want to compute.
     * @param totalLeaves The "global" number of leaves in case this histogram is computed only on a zoomed-in range.
     *                    This is needed to correctly compute the amount of mean to add to each leaf.
     * @return The average per-query absolute error.
     */
    private double computeAccuracy(PrivateHistogram ph, int totalLeaves) {
        double scale = Math.log(totalLeaves) / Math.log(2);
        scale /= ph.getEpsilon();
        double baseVariance = 2 * Math.pow(scale, 2);
        // Do all-intervals accuracy on leaves.
        int n = 0;
        double sqtot = 0.0;
        double abstot = 0.0;
        Noise noise = new Noise();
        for (int left = 0; left < ph.histogram.getBucketCount(); left++) {
            for (int right = left; right < ph.histogram.getBucketCount(); right++) {
                ph.noiseForRange(left, right,
                        scale, baseVariance, noise);
                sqtot += Math.pow(noise.mean, 2);
                abstot += Math.abs(noise.mean);
                n++;
            }
        }

        System.out.println("Bucket count: " + ph.histogram.getBucketCount());
        System.out.println("Num intervals: " + n);
        System.out.println("Average absolute error: " + abstot / (double) n);
        System.out.println("Average L2 error: " + Math.sqrt(sqtot) / (double) n);

        return abstot / (double) n;
    }

    /**
     * Compute the absolute and L2 error vs. ground truth for an instantiation of a private histogram
     * averaged over all possible range queries.
     *
     * @param ph The histogram whose accuracy we want to compute.
     * @param totalLeaves The "global" number of leaves in case this histogram is computed only on a zoomed-in range.
     *                    This is needed to correctly compute the amount of mean to add to each leaf.
     * @return The average per-query absolute error.
     */
    /*private double computeAccuracy(PrivateHeatmap ph, int totalLeaves) {
        double scale = Math.log(totalLeaves) / Math.log(2);
        scale /= ph.getEpsilon();
        double baseVariance = 2 * Math.pow(scale, 2);
        // Do all-intervals accuracy on leaves.
        int n = 0;
        double sqtot = 0.0;
        double abstot = 0.0;
        Noise noise = new Noise();
        for (int left = 0; left < ph.heatmap.getBucketCount(); left++) {
            for (int right = left; right < ph.heatmap.getBucketCount(); right++) {
                ph.noiseForRange(left, right,
                        scale, baseVariance, noise);
                sqtot += Math.pow(noise.mean, 2);
                abstot += Math.abs(noise.mean);
                n++;
            }
        }

        System.out.println("Bucket count: " + ph.histogram.getBucketCount());
        System.out.println("Num intervals: " + n);
        System.out.println("Average absolute error: " + abstot / (double) n);
        System.out.println("Average L2 error: " + Math.sqrt(sqtot) / (double) n);

        return abstot / (double) n;
    }*/

    private HistogramRequestInfo createHistogramRequest(String col, ColumnQuantization cq) {
        HistogramRequestInfo info;
        if (cq instanceof DoubleColumnQuantization) {
            DoubleColumnQuantization dq = (DoubleColumnQuantization)cq;
            info = new HistogramRequestInfo(new ColumnDescription(col, ContentsKind.Double),
                    0, dq.globalMin, dq.globalMax, dq.getIntervalCount());
        } else {
            // StringColumnQuantization
            StringColumnQuantization sq = (StringColumnQuantization)cq;
            info = new HistogramRequestInfo(new ColumnDescription(col, ContentsKind.String),0, sq.leftBoundaries);
        }

        return info;
    }

    private Pair<Double, Double> computeSingleColumnAccuracy(String col, ColumnQuantization cq, double epsilon, IDataSet<ITable> table,
                                             int iterations) {
        // Construct a histogram corresponding to the leaves.
        // We will manually aggregate buckets as needed for the accuracy test.
        HistogramRequestInfo info = createHistogramRequest(col, cq);
        HistogramSketch sk = info.getSketch(cq);
        IntervalDecomposition dd = info.getDecomposition(cq);

        System.out.println("Epsilon: " + epsilon);
        Histogram hist = table.blockingSketch(sk); // Leaf counts.
        Assert.assertNotNull(hist);

        int totalLeaves = dd.getQuantizationIntervalCount();
        TestKeyLoader tkl = new TestKeyLoader();

        ArrayList<Double> accuracies = new ArrayList<>();
        double totAccuracy = 0.0;
        for (int i = 0 ; i < iterations; i++) {
            tkl.setIndex(i);
            SecureLaplace laplace = new SecureLaplace(tkl);
            PrivateHistogram ph = new PrivateHistogram(dd, hist, epsilon, false, laplace);
            double acc = computeAccuracy(ph, totalLeaves);
            accuracies.add(acc);
            totAccuracy += acc;
        }
        return new Pair(totAccuracy / iterations, Utilities.stdev(accuracies));
    }

    /*private Pair<Double, Double> computeHeatmapAccuracy(String col1, String col2, ColumnQuantization cq1,
                                                        ColumnQuantization cq2,
                                                        double epsilon, IDataSet<ITable> table,
                                                        int iterations) {
        // Construct a histogram corresponding to the leaves.
        // We will manually aggregate buckets as needed for the accuracy test.
        HistogramRequestInfo[] info = new HistogramRequestInfo[]
                {
                        createHistogramRequest(col1, cq1),
                        createHistogramRequest(col2, cq2)
                };

        HeatmapSketch sk = new HeatmapSketch(
                info[0].getBuckets(),
                info[1].getBuckets(),
                info[0].cd.name,
                info[1].cd.name, 1.0, 0);
        IntervalDecomposition d0 = info[0].getDecomposition(cq1);
        IntervalDecomposition d1 = info[1].getDecomposition(cq2);

        System.out.println("Epsilon: " + epsilon);
        Heatmap heatmap = table.blockingSketch(sk); // Leaf counts.
        Assert.assertNotNull(heatmap);

        int totalLeaves = d0.getQuantizationIntervalCount() * d1.getQuantizationIntervalCount();
        TestKeyLoader tkl = new TestKeyLoader();

        ArrayList<Double> accuracies = new ArrayList<>();
        double totAccuracy = 0.0;
        for (int i = 0 ; i < iterations; i++) {
            tkl.setIndex(i);
            SecureLaplace laplace = new SecureLaplace(tkl);
            PrivateHeatmap ph = new PrivateHeatmap(d0, d1, heatmap, epsilon, laplace);
            double acc = computeAccuracy(ph, totalLeaves);
            accuracies.add(acc);
            totAccuracy += acc;
        }
        return new Pair(totAccuracy / iterations, Utilities.stdev(accuracies));
    }*/

    @Test
    public void computeAccuracyTest() throws IOException {
        HillviewLogger.instance.setLogLevel(Level.OFF);

        @Nullable
        IDataSet<ITable> table = this.loadData();
        if (table == null) {
            System.out.println("Skipping test: no data");
            return;
        }

        Schema schema = this.loadSchema(table);
        List<String> cols = schema.getColumnNames();

        PrivacySchema mdSchema = PrivacySchema.loadFromFile(ontime_directory + privacy_metadata_name);
        Assert.assertNotNull(mdSchema);
        Assert.assertNotNull(mdSchema.quantization);

        HashMap<String, ArrayList<Double>> results = new HashMap();
        int iterations = 5;
        for (String col : cols) {
            ColumnQuantization quantization = mdSchema.quantization.get(col);
            Assert.assertNotNull(quantization);

            double epsilon = mdSchema.epsilon(col);

            Pair<Double, Double> res = this.computeSingleColumnAccuracy(col, quantization, epsilon, table, iterations);
            System.out.println("Averaged absolute error over " + iterations + " iterations: " + res.first);

            // for JSON parsing convenience
            ArrayList<Double> resArr = new ArrayList();
            resArr.add(res.first); // mean
            resArr.add(res.second); // stdev

            results.put(col, resArr);
        }

        FileWriter writer = new FileWriter(results_filename);
        Gson resultsGson = new GsonBuilder().create();
        writer.write(resultsGson.toJson(results));
        writer.flush();
        writer.close();
    }
}

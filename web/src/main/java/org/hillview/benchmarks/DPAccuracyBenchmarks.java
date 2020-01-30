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

package org.hillview.benchmarks;

import com.google.gson.*;
import org.hillview.dataStructures.*;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.dataset.api.Pair;
import org.hillview.main.Benchmarks;
import org.hillview.maps.FindFilesMap;
import org.hillview.maps.LoadFilesMap;
import org.hillview.security.SecureLaplace;
import org.hillview.security.TestKeyLoader;
import org.hillview.sketches.HeatmapSketch;
import org.hillview.sketches.HistogramSketch;
import org.hillview.sketches.SummarySketch;
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
import org.hillview.utils.*;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;

public class DPAccuracyBenchmarks extends Benchmarks {
    private static String ontime_directory = "data/ontime_private/";
    private static String privacy_metadata_name = "privacy_metadata.json";

    private static final String histogram_results_filename = "results/ontime_private_histogram_binary.json";
    private static final String heatmap_results_filename = "results/ontime_private_heatmap_binary.json";

    String resultsFilename;

    private DPAccuracyBenchmarks(String resultsFilename) {
        this.resultsFilename = resultsFilename;
    }

    @Nullable
    IDataSet<ITable> loadData() {
        try {
            FileSetDescription fsd = new FileSetDescription();
            fsd.fileNamePattern = "data/ontime_private/????_*.csv*";
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

    /**
     * Compute the absolute and L2 error vs. ground truth for an instantiation of a private histogram
     * averaged over all possible range queries.
     *
     * @param ph The histogram whose accuracy we want to compute.
     * @param decomposition The leaf specification for the histogram.
     * @return The average per-query absolute error.
     */
    private double computeSampledAccuracy(PrivateHistogram ph, IntervalDecomposition decomposition, SecureLaplace laplace,
                                          int nsamples, int seed, boolean useIdentity) {
        double scale = PrivacyUtils.computeNoiseScale(ph.getEpsilon(), decomposition);
        if (useIdentity) {
            IntervalDecomposition.BRANCHING_FACTOR = -1;
            scale = 1.0 / ph.getEpsilon();
        }
        double baseVariance = PrivacyUtils.laplaceVariance(scale);
        // Do all-intervals accuracy on leaves.
        int n = 0;
        double sqtot = 0.0;
        double abstot = 0.0;
        double wcError = 0.0;
        Noise noise = new Noise();
        long totalIntervals = 0;
        Randomness rand = new Randomness(seed);

        for (int i = 0; i < nsamples; i++) {
            int left = rand.nextInt(decomposition.getQuantizationIntervalCount());
            int right = left + rand.nextInt(decomposition.getQuantizationIntervalCount() - left);
            long numInts = ph.noiseForRange(left, right, scale, baseVariance, laplace, noise);
            totalIntervals += numInts;
            double error = noise.getNoise();
            sqtot += Math.pow(error, 2);
            double abs = Math.abs(error);
            abstot += abs;
            if (abs > wcError) {
                wcError = abs;
            }

            n++;
        }

        //System.out.println("Average interval size: " + totalIntervals / (double)n);
        //System.out.println("Bucket count: " + ph.histogram.getBucketCount());
        //System.out.println("Num intervals: " + n);
        System.out.println("Average absolute error: " + abstot / (double) n);
        System.out.println("Average L2 error: " + Math.sqrt(sqtot) / (double) n);
        System.out.println("Worst-case error: " + wcError);

        return abstot / (double) n;
    }


    /**
     * Compute the absolute and L2 error vs. ground truth for an instantiation of a private histogram
     * averaged over all possible range queries.
     *
     * @param ph The histogram whose accuracy we want to compute.
     * @param decomposition The leaf specification for the histogram.
     * @return The average per-query absolute error.
     */
    private double computeAccuracy(PrivateHistogram ph, IntervalDecomposition decomposition, SecureLaplace laplace,
                                   boolean roundToZero) {

        double scale = PrivacyUtils.computeNoiseScale(ph.getEpsilon(), decomposition);
        //double scale = 1.0;
        //System.out.println("Scale: " + scale);
        double baseVariance = PrivacyUtils.laplaceVariance(scale);
        // Do all-intervals accuracy on leaves.
        int n = 0;
        double sqtot = 0.0;
        double abstot = 0.0;
        double wcError = 0.0;
        Noise noise = new Noise();

        long totalIntervals = 0;
        for (int left = 0; left < ph.histogram.getBucketCount(); left++) {
            for (int right = left + 1; right < ph.histogram.getBucketCount(); right++) {
                long numInts = ph.noiseForRange(left, right, scale, baseVariance, laplace, noise);
                totalIntervals += numInts;
                double error = noise.getNoise();
                sqtot += Math.pow(error, 2);
                double abs = Math.abs(noise.getNoise());
                abstot += abs;
                if (abs > wcError) {
                    wcError = abs;
                }

                n++;
            }
        }

        //System.out.println("Average interval size: " + totalIntervals / (double)n);
        //System.out.println("Bucket count: " + ph.histogram.getBucketCount());
        //System.out.println("Num intervals: " + n);
        System.out.println("Average absolute error: " + abstot / (double) n);
        System.out.println("Average L2 error: " + Math.sqrt(sqtot) / (double) n);
        System.out.println("Worst-case error: " + wcError);

        return abstot / (double) n;
    }

    /**
     * Compute the absolute and L2 error vs. ground truth for an instantiation of a private heat map
     * averaged over all possible range queries (every rectangle).
     *
     * @param ph The heat map whose accuracy we want to compute.
     * @param xd The leaf specification for the x-axis.
     * @param yd The leaf specification for the y-axis.
     * @return The average per-query absolute error.
     */
    private Double computeSampledAccuracy(PrivateHeatmapFactory ph, IntervalDecomposition xd, IntervalDecomposition yd,
                                          int nsamples, int seed, boolean useIdentity) {
        double scale = PrivacyUtils.computeNoiseScale(ph.getEpsilon(), xd, yd);
        if (useIdentity) {
            IntervalDecomposition.BRANCHING_FACTOR = -1;
            scale = 1.0 / ph.getEpsilon();
        }
        double baseVariance = PrivacyUtils.laplaceVariance(scale);

        // Do subsampled all-intervals accuracy on leaves.
        int n = 0;
        double sqtot = 0.0;
        double abstot = 0.0;
        Noise noise = new Noise();
        Randomness rand = new Randomness(seed);

        for (int i = 0; i < nsamples; i++) {
            int left = rand.nextInt(xd.getQuantizationIntervalCount());
            int right = left + rand.nextInt(xd.getQuantizationIntervalCount() - left);
            int top = rand.nextInt(yd.getQuantizationIntervalCount());
            int bot = top + rand.nextInt(yd.getQuantizationIntervalCount() - top);

            ph.noiseForRange(left, right, top, bot, scale, baseVariance, noise);

            double error = noise.getNoise();
            sqtot += Math.pow(error, 2);
            abstot += Math.abs(error);
            n++;
        }

        System.out.println("Bucket count: " + ph.heatmap.getXBucketCount() * ph.heatmap.getYBucketCount());
        System.out.println("Num intervals: " + n);
        System.out.println("Average absolute error: " + abstot / (double) n);
        System.out.println("Average L2 error: " + Math.sqrt(sqtot) / (double) n);

        return abstot / (double) n;
    }

    private HistogramRequestInfo createHistogramRequest(String col, ColumnQuantization cq) {
        // Construct a histogram corresponding to the leaves.
        // We will manually aggregate buckets as needed for the accuracy test.
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

    private Pair<Double, Double> computeSingleColumnAccuracy(String col, int colIndex,
                                                             ColumnQuantization cq, double epsilon, IDataSet<ITable> table,
                                                             int iterations, boolean useIdentity) {
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
        int nsamples = 500;
        for (int i = 0 ; i < iterations; i++) {
            tkl.setIndex(i);
            SecureLaplace laplace = new SecureLaplace(tkl);
            PrivateHistogram ph = new PrivateHistogram(colIndex, 
                                                       dd, hist, epsilon, false, laplace);
            double acc = computeSampledAccuracy(ph, dd, laplace, nsamples, i, useIdentity);
            //double acc = computeAccuracy(ph, dd, laplace, true);
            accuracies.add(acc);
            totAccuracy += acc;
        }
        return new Pair<Double, Double>(totAccuracy / iterations, Utilities.stdev(accuracies));
    }

    private Pair<Double, Double> computeHeatmapAccuracy(String col1, ColumnQuantization cq1,
                                                        String col2, ColumnQuantization cq2,
                                                        int columnsIndex,
                                                        double epsilon, IDataSet<ITable> table,
                                                        int iterations, boolean useIdentity) {
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

        int totalXLeaves = d0.getQuantizationIntervalCount();
        int totalYLeaves = d1.getQuantizationIntervalCount();
        TestKeyLoader tkl = new TestKeyLoader();

        ArrayList<Double> accuracies = new ArrayList<>();
        double totAccuracy = 0.0;
        int nsamples = 5000;
        for (int i = 0 ; i < iterations; i++) {
            tkl.setIndex(i);
            SecureLaplace laplace = new SecureLaplace(tkl);
            PrivateHeatmapFactory ph = new PrivateHeatmapFactory(columnsIndex, d0, d1, heatmap, epsilon, laplace);
            double acc = computeSampledAccuracy(ph, d0, d1, nsamples, i, useIdentity);
            accuracies.add(acc);
            totAccuracy += acc;
        }
        return new Pair<Double, Double>(totAccuracy / iterations, Utilities.stdev(accuracies));
    }

    public void benchmarkHistogramL2Accuracy(boolean useIdentity) throws IOException {
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

        HashMap<String, ArrayList<Double>> results = new HashMap<String, ArrayList<Double>>();
        int iterations = 50;
        for (String col : cols) {
            ColumnQuantization quantization = mdSchema.quantization.get(col);
            Assert.assertNotNull(quantization);

            double epsilon = mdSchema.epsilon(col);

            Pair<Double, Double> res = this.computeSingleColumnAccuracy(
                    col, mdSchema.getColumnIndex(col), quantization, epsilon, table, iterations, useIdentity);
            System.out.println(col + ": Averaged absolute error over " + iterations + " iterations: " + res.first);

            // for JSON parsing convenience
            ArrayList<Double> resArr = new ArrayList<Double>();
            resArr.add(res.first); // noise
            resArr.add(res.second); // stdev

            results.put(col, resArr);
        }

        FileWriter writer = new FileWriter(histogram_results_filename);
        Gson resultsGson = new GsonBuilder().create();
        writer.write(resultsGson.toJson(results));
        writer.flush();
        writer.close();
    }

    public void benchmarkHistogramBranching() throws IOException {
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

        HashMap<String, ArrayList<Double>> results = new HashMap<String, ArrayList<Double>>();
        int maxBranch = 1000;
        int iterations = 10;
        String col = "DepDelay";
        ColumnQuantization quantization = mdSchema.quantization.get(col);
        Assert.assertNotNull(quantization);

        for (int i = 2; i <= maxBranch; i += 50) {
            IntervalDecomposition.BRANCHING_FACTOR = i;

            String key = Integer.toString(i);
            double epsilon = mdSchema.epsilon(col);

            Pair<Double, Double> res = this.computeSingleColumnAccuracy(col, mdSchema.getColumnIndex(col), quantization, epsilon, table, iterations, false);
            System.out.println("Averaged absolute error over " + iterations + " iterations: " + res.first);

            // for JSON parsing convenience
            ArrayList<Double> resArr = new ArrayList<Double>();
            resArr.add(res.first); // noise
            resArr.add(res.second); // stdev

            results.put(key, resArr);
            System.out.println("Key: " + key + ", mean: " + res.first);
        }

        FileWriter writer = new FileWriter("histogram_branching_results.json");
        Gson resultsGson = new GsonBuilder().create();
        writer.write(resultsGson.toJson(results));
        writer.flush();
        writer.close();
    }

    public void benchmarkHeatmapBranching() throws IOException {
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

        HashMap<String, ArrayList<Double>> results = new HashMap<String, ArrayList<Double>>();
        int maxBranch = 400;
        int iterations = 5;
        String col1 = "FlightDate";
        String col2 = "OriginState";
        ColumnQuantization q1 = mdSchema.quantization.get(col1);
        Assert.assertNotNull(q1);
        ColumnQuantization q2 = mdSchema.quantization.get(col2);
        Assert.assertNotNull(q2);

        for (int i = 2; i <= maxBranch; i ++) {
            IntervalDecomposition.BRANCHING_FACTOR = i;

            String key = Integer.toString(i);
            double epsilon = mdSchema.epsilon(col1, col2);

            Pair<Double, Double> res = this.computeHeatmapAccuracy(col1, q1, col2, q2, mdSchema.getColumnIndex(col1, col2),
                    epsilon, table, iterations, false);
            System.out.println("Averaged absolute error over " + iterations + " iterations: " + res.first);

            // for JSON parsing convenience
            ArrayList<Double> resArr = new ArrayList<Double>();
            resArr.add(res.first); // noise
            resArr.add(res.second); // stdev

            System.out.println("Key: " + key + ", mean: " + res.first);
            results.put(key, resArr);
        }

        FileWriter writer = new FileWriter("branching_results.json");
        Gson resultsGson = new GsonBuilder().create();
        writer.write(resultsGson.toJson(results));
        writer.flush();
        writer.close();
    }


    public void benchmarkHeatmapL2Accuracy(boolean useIdentity) throws IOException {
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

        HashMap<String, ArrayList<Double>> results = new HashMap<String, ArrayList<Double>>();
        int iterations = 10;
        for (int i = 0; i < cols.size(); i++) {
            for (int j = i+1; j < cols.size(); j++) {
                String col1 = cols.get(i);
                String col2 = cols.get(j);

                if (col1.equals(col2)) continue;

                ColumnQuantization q1 = mdSchema.quantization.get(col1);
                Assert.assertNotNull(q1);
                ColumnQuantization q2 = mdSchema.quantization.get(col2);
                Assert.assertNotNull(q2);

                String key = mdSchema.getKeyForColumns(col1, col2);
                double epsilon = mdSchema.epsilon(col1, col2);
                //double epsilon = 1.0;

                Pair<Double, Double> res = this.computeHeatmapAccuracy(col1, q1, col2, q2, mdSchema.getColumnIndex(col1, col2),
                        epsilon, table, iterations, useIdentity);
                System.out.println("Averaged absolute error over " + iterations + " iterations: " + res.first);

                // for JSON parsing convenience
                ArrayList<Double> resArr = new ArrayList<Double>();
                resArr.add(res.first); // noise
                resArr.add(res.second); // stdev

                System.out.println("Key: " + key + ", mean: " + res.first);
                results.put(key, resArr);
            }
        }

        FileWriter writer = new FileWriter(heatmap_results_filename);
        Gson resultsGson = new GsonBuilder().create();
        writer.write(resultsGson.toJson(results));
        writer.flush();
        writer.close();
    }

    public static void main(String[] args) throws IOException, SQLException {
        if (args.length < 1) {
            return;
        }

        String resultsFilename = args[0];
        DPAccuracyBenchmarks bench = new DPAccuracyBenchmarks(resultsFilename);
        double timeStart = System.currentTimeMillis();
        double timeEnd = System.currentTimeMillis();

        System.out.println("Benchmark took " + (timeEnd - timeStart) / 1000 + " seconds.");
    }
}

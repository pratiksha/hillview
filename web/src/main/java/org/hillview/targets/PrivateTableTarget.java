package org.hillview.targets;

import org.hillview.*;
import org.hillview.dataStructures.*;
import org.hillview.dataset.ConcurrentSketch;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.Pair;
import org.hillview.maps.FilterMap;
import org.hillview.sketches.*;
import org.hillview.sketches.results.*;
import org.hillview.table.PrivacySchema;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.table.filters.RangeFilterPairDescription;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewException;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.JsonString;

import java.util.function.BiFunction;

public class PrivateTableTarget extends RpcTarget implements IPrivateDataset {
    public final IDataSet<ITable> table;
    public final DPWrapper wrapper;

    PrivateTableTarget(IDataSet<ITable> table, HillviewComputation computation,
                       PrivacySchema privacySchema, String schemaFilename) {
        super(computation);
        this.wrapper = new DPWrapper(privacySchema, schemaFilename);
        this.table = table;
        this.registerObject();
    }

    private PrivateTableTarget(IDataSet<ITable> table,
                               HillviewComputation computation,
                               DPWrapper wrapper) {
        super(computation);
        this.table = table;
        this.wrapper = new DPWrapper(wrapper);
        this.registerObject();
    }

    private PrivacySchema getPrivacySchema() {
        return this.wrapper.getPrivacySchema();
    }

    @HillviewRpc
    public void changePrivacy(RpcRequest request, RpcRequestContext context) {
        this.wrapper.setPrivacySchema(request.parseArgs(PrivacySchema.class));
        HillviewLogger.instance.info("Updated privacy schema");
        PrecomputedSketch<ITable, JsonString> empty =
                new PrecomputedSketch<ITable, JsonString>(new JsonString("{}"));
        this.runCompleteSketch(this.table, empty, (d, c) -> d, request, context);
    }

    @HillviewRpc
    public void savePrivacy(RpcRequest request, RpcRequestContext context) {
        this.wrapper.savePrivacySchema();
        HillviewLogger.instance.info("Saved privacy schema");
        PrecomputedSketch<ITable, JsonString> empty =
                new PrecomputedSketch<ITable, JsonString>(new JsonString("{}"));
        this.runCompleteSketch(this.table, empty, (d, c) -> d, request, context);
    }

    @HillviewRpc
    public void getSummary(RpcRequest request, RpcRequestContext context) {
        SummarySketch ss = new SummarySketch();
        this.runCompleteSketch(this.table, ss,
                (d, c) -> this.wrapper.addPrivateMetadata(d), request, context);
    }

    // Returns both the histogram and the precomputed CDF of the data.
    // Each histogram data structure will also contain the corresponding precomputed CDF,
    // but we still compute two of them for one request because the histogram buckets and CDF
    // are computed at different bucket granularities.
    @HillviewRpc
    public void histogram(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo[] info = request.parseArgs(HistogramRequestInfo[].class);
        assert info.length == 2;
        ColumnQuantization quantization = this.getPrivacySchema().quantization(info[0].cd.name);
        Converters.checkNull(quantization);
        HistogramSketch sk = info[0].getSketch(quantization); // Histogram
        HistogramSketch cdf = info[1].getSketch(quantization);
        IntervalDecomposition d0 = info[0].getDecomposition(quantization);
        IntervalDecomposition d1 = info[1].getDecomposition(quantization);
        double epsilon = this.getPrivacySchema().epsilon(info[0].cd.name);
        ConcurrentSketch<ITable, Histogram, Histogram> csk =
                new ConcurrentSketch<ITable, Histogram, Histogram>(sk, cdf);
        this.runCompleteSketch(this.table, csk, (e, c) ->
                new Pair<PrivateHistogram, PrivateHistogram>(
                        new PrivateHistogram(d0, Converters.checkNull(e.first), epsilon, false, this.wrapper.laplace),
                        new PrivateHistogram(d1, Converters.checkNull(e.second), epsilon, true, this.wrapper.laplace)),
                request, context);
    }

    @HillviewRpc
    public void getDataQuantiles1D(RpcRequest request, RpcRequestContext context) {
        this.wrapper.getDataQuantiles1D(request, context, this);
    }

    @HillviewRpc
    public void filterRange(RpcRequest request, RpcRequestContext context) {
        RangeFilterDescription filter = request.parseArgs(RangeFilterDescription.class);
        FilterMap map = new FilterMap(filter, this.getPrivacySchema().quantization);
        BiFunction<IDataSet<ITable>, HillviewComputation, IRpcTarget> constructor = (e, c) -> {
            PrivateTableTarget result = new PrivateTableTarget(e, c, this.wrapper);
            result.getWrapper().filter(filter);
            return result;
        };
        this.runMap(this.table, map, constructor, request, context);
    }

    @HillviewRpc
    public void filter2DRange(RpcRequest request, RpcRequestContext context) {
        RangeFilterPairDescription filter = request.parseArgs(RangeFilterPairDescription.class);
        FilterMap map = new FilterMap(filter, this.getPrivacySchema().quantization);
        BiFunction<IDataSet<ITable>, HillviewComputation, IRpcTarget> constructor = (e, c) -> {
            PrivateTableTarget result = new PrivateTableTarget(e, c, this.wrapper);
            result.getWrapper().filter(filter.first);
            result.getWrapper().filter(filter.second);
            return result;
        };
        this.runMap(this.table, map, constructor, request, context);
    }

    @HillviewRpc
    public void hLogLog(RpcRequest request, RpcRequestContext context) {
        DistinctCountRequestInfo col = request.parseArgs(DistinctCountRequestInfo.class);
        HLogLogSketch sketch = new HLogLogSketch(col.columnName, col.seed);
        // TODO(pratiksha): add mean to this count
        this.runSketch(this.table, sketch, request, context);
    }

    private void heavyHitters(RpcRequest request, RpcRequestContext context) {
        HeavyHittersRequestInfo info = request.parseArgs(HeavyHittersRequestInfo.class);
        MGFreqKSketch sk = new MGFreqKSketch(info.columns, info.amount/100,
                this.getPrivacySchema().quantization);
        // TODO(pratiksha): add mean to the counts
        this.runCompleteSketch(this.table, sk, (x, c) -> TableTarget.getTopList(x, info.columns, c),
                request, context);
    }

    //@HillviewRpc // TODO: we don't know how to do this privately
    public void heavyHittersMG(RpcRequest request, RpcRequestContext context) {
        this.heavyHitters(request, context);
    }

    //@HillviewRpc // TODO: we don't know how to do this privately
    public void heavyHittersSampling(RpcRequest request, RpcRequestContext context) {
        this.heavyHitters(request, context);
    }

    @HillviewRpc
    public void getDataQuantiles2D(RpcRequest request, RpcRequestContext context) {
        this.wrapper.getDataQuantiles2D(request, context, this);
    }

    @HillviewRpc
    public void heatmap(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo[] info = request.parseArgs(HistogramRequestInfo[].class);
        assert info.length == 2;
        ColumnQuantization q0 = this.getPrivacySchema().quantization(info[0].cd.name);
        ColumnQuantization q1 = this.getPrivacySchema().quantization(info[1].cd.name);
        Converters.checkNull(q0);
        Converters.checkNull(q1);
        double epsilon = this.getPrivacySchema().epsilon(info[0].cd.name, info[1].cd.name);
        IHistogramBuckets b0 = info[0].getBuckets(q0);
        IHistogramBuckets b1 = info[1].getBuckets(q1);
        IntervalDecomposition d0 = info[0].getDecomposition(q0);
        IntervalDecomposition d1 = info[1].getDecomposition(q1);
        HeatmapSketch sk = new HeatmapSketch(
                b0, b1, info[0].cd.name, info[1].cd.name, 1.0, 0, q0, q1);
        this.runCompleteSketch(this.table, sk, (e, c) ->
                new PrivateHeatmap(d0, d1, e, epsilon, this.wrapper.laplace).heatmap, request, context);
    }

    @HillviewRpc
    public void getNextK(RpcRequest request, RpcRequestContext context) {
        TableTarget.NextKArgs nextKArgs = request.parseArgs(TableTarget.NextKArgs.class);
        // Only allow this if the sort order is empty
        if (nextKArgs.order.getSize() != 0)
            throw new HillviewException("No column data can be displayed privately");
        RowSnapshot rs = TableTarget.asRowSnapshot(
                nextKArgs.firstRow, nextKArgs.order, nextKArgs.columnsNoValue);
        NextKSketch nk = new NextKSketch(nextKArgs.order, null, rs, nextKArgs.rowsOnScreen,
                this.getPrivacySchema().quantization);
        // TODO(pratiksha): add mean to the counts on the NextKList
        this.runSketch(this.table, nk, request, context);
    }

    @HillviewRpc
    public void quantile(RpcRequest request, RpcRequestContext context) {
        QuantileInfo info = request.parseArgs(QuantileInfo.class);
        SampleQuantileSketch sk = new SampleQuantileSketch(
                info.order, info.precision, info.tableSize, info.seed,
                this.getPrivacySchema().quantization);
        BiFunction<SampleList, HillviewComputation, RowSnapshot> getRow = (ql, c) -> ql.getRow(info.position);
        this.runCompleteSketch(this.table, sk, getRow, request, context);
    }

    @Override
    public IDataSet<ITable> getDataset() {
        return this.table;
    }

    @Override
    public DPWrapper getWrapper() {
        return this.wrapper;
    }
}

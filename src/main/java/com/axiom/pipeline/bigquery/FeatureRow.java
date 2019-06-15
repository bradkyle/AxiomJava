package com.axiom.pipeline.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.values.Row;
import com.google.common.collect.ImmutableList;

import com.google.api.services.bigquery.model.TableRow;
import java.util.stream.Collectors;

import com.axiom.pipeline.util.DoubleStatistics;
import com.axiom.pipeline.util.Compare;

// TODO row and table row aggregator
public class FeatureRow{
    private static final Logger logger = LoggerFactory.getLogger(FeatureRow.class);

    final TableRow tableRow;

    public FeatureRow(
    ) {
        this.tableRow = new TableRow();
    }

    public void aggregate(List<Row> events, String label) {
        try {

            // TODO
            List<Row> sortedTrades = events.stream().sorted(new Compare.TimeComparator()).collect(Collectors.toList());

            int count = sortedTrades.size();
            Double openQuantity = sortedTrades.get(0).getDouble("quantity");
            Double openPrice = sortedTrades.get(0).getDouble("price");
            Double closeQuantity = sortedTrades.get(count-1).getDouble("quantity");
            Double closePrice = sortedTrades.get(count-1).getDouble("price");

            DoubleStatistics priceStats = sortedTrades.stream().map(o -> o.getDouble("price")).collect(DoubleStatistics.collector());
            DoubleStatistics qtyStats = sortedTrades.stream().map(o -> o.getDouble("quantity")).collect(DoubleStatistics.collector());

            Double vwap = sortedTrades.stream()
                .mapToDouble(o -> o.getDouble("price")*o.getDouble("quantity"))
                .sum()/sortedTrades.stream()
                .mapToDouble(o -> o.getDouble("quantity"))
                .sum();

            this.tableRow
                .set(label+"_count", count)
                .set(label+"_volume", qtyStats.getSum())
                .set(label+"_open_quantity", openQuantity)
                .set(label+"_close_quantity", closeQuantity)
                .set(label+"_high_quantity", qtyStats.getMax())
                .set(label+"_low_quantity", qtyStats.getMin())
                .set(label+"_mean_quantity", qtyStats.getAverage())
                .set(label+"_std_quantity", qtyStats.getStandardDeviation())
                .set(label+"_open_price", openPrice)
                .set(label+"_close_price", closePrice)
                .set(label+"_high_price", priceStats.getMax())
                .set(label+"_low_price", priceStats.getMin())
                .set(label+"_mean_price", priceStats.getAverage())
                .set(label+"_std_price", priceStats.getStandardDeviation())
                .set(label+"_vwap", vwap);
        } catch(Exception e) {
            logger.error(e.toString()+":"+events.toString());
        }
    }

    public TableRow getTableRow() {
        return this.tableRow;
    }
}

package com.swift.flink_app.streamProcessing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.swift.flink_app.dto.Metric;
import com.swift.flink_app.dto.RemittanceTransaction;
import com.swift.flink_app.dto.RiskLabel;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.time.Duration;

import java.util.Properties;

//@Component
public class StreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // --- CRITICAL: Ensure Watermark generation is active for latency metrics ---
        env.getConfig().setAutoWatermarkInterval(500L); // Generate watermarks every 500ms

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:9093"); //inside docker network
        props.setProperty("group.id", "flink-group");
        // props.setProperty("auto.offset.reset", "earliest");

        // Purpose: This sets up a Kafka consumer object for Flink.
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "remittance-stream",
                new SimpleStringSchema(),
                props
        );
        FlinkKafkaConsumer<String> riskConsumer =  new FlinkKafkaConsumer<>(
                "remittance-stream",
                new SimpleStringSchema(),
                props
        );

        // --- 1. Risk Stream (for real-time risk labeling) ---

        DataStream<String>  riskStreamRaw = env.addSource(riskConsumer).name("Kafka Source - Risk");
        riskConsumer.setStartFromLatest();

        ObjectMapper mappers = new ObjectMapper();

        DataStream<RemittanceTransaction> riskStream = riskStreamRaw
                .map(json -> mappers.readValue(json, RemittanceTransaction.class)).name("JSON Parse - Risk")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<RemittanceTransaction>forBoundedOutOfOrderness( Duration.ofSeconds(5) )
                                .withTimestampAssigner((event, ts) -> event.getTimestamp())
                ).name("Watermark Strategy - Risk"); // Watermarks are assigned here

        DataStream<RiskLabel> labelledStream = riskStream
                .map(tx -> {
                    RiskLabel riskLabel = new RiskLabel();
                    long processingStart = System.currentTimeMillis();
                    riskLabel.setProcessingTime(String.valueOf(processingStart));

                    long latency = processingStart -  tx.getTimestamp();
                    riskLabel.setLatency(String.valueOf(latency));

                    riskLabel.setRiskType(tx.getAmount() > 1000? "RISK":"SAFE");
                    return riskLabel;
                }).name("Risk Labeler");

        // Print the enriched events
        labelledStream
                .map(tx -> String.format(
                        "TxId=%s, Amount=%.2f, Risk=%s, ProcessingTime=%s, Latency=%s ms",
                        tx.getTransactionId(),
                        tx.getAmount(),
                        tx.getRiskType(),
                        tx.getProcessingTime(),
                        tx.getLatency()
                )).name("Risk Printer")
                .print();

        // --- 2. Event Stream (for windowed metrics aggregation) ---

        DataStream<String> stream = env.addSource(consumer).name("Kafka Source - Metrics");
        consumer.setStartFromLatest();

        ObjectMapper mapper = new ObjectMapper();

        // Convert json to POJO and assign monotonous timestamps for metrics calculation
        DataStream<RemittanceTransaction> eventStream =
                stream.map(json -> mapper.readValue(json, RemittanceTransaction.class)).name("JSON Parse - Metrics")
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<RemittanceTransaction>forMonotonousTimestamps()
                                        .withTimestampAssigner((event, ts) -> event.getTimestamp())
                        ).name("Watermark Strategy - Metrics");

        // Windowed aggregation
        DataStream<String> metricsStream = eventStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))  // emits every 10s
                // The .name() call must be after the final transformation (.apply in this case).

                .apply(new AllWindowFunction<RemittanceTransaction, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<RemittanceTransaction> events, Collector<String> out) throws Exception {

                        int successCount = 0;
                        int failedCount = 0;
                        double totalAmount = 0;
                        double totalRate = 0;
                        double minAmount = Double.MAX_VALUE;
                        double maxAmount = Double.MIN_VALUE;

                        for (RemittanceTransaction e : events) {
                            try {
                                if (e.getExchangeRate() == 0){
                                    throw new IllegalArgumentException("Exchange rate cant be 0");
                                }
                                // Business logic for each event (simple aggregation)
                                totalAmount += e.getAmount();
                                totalRate += e.getExchangeRate();
                                if (e.getAmount() < minAmount) minAmount = e.getAmount();
                                if (e.getAmount() > maxAmount) maxAmount = e.getAmount();

                                successCount++;
                            } catch (Exception ex) {
                                failedCount++;
                            }
                        }

                        long totalCount = successCount + failedCount;
                        double avgAmount = successCount == 0 ? 0 : totalAmount / successCount;
                        double avgRate = successCount == 0 ? 0 : totalRate / successCount;

                        if (totalCount == 0) {
                            minAmount = 0;
                            maxAmount = 0;
                        }

                        // Generate metrics JSON string
                        String metricsJson = String.format(
                                "{ \"windowStart\": %d, " +
                                        "\"windowEnd\": %d, " +
                                        "\"count\": %d, " +
                                        "\"successCount\": %d, " +
                                        "\"failureCount\": %d, " +
                                        "\"avgAmount\": %.2f, " +
                                        "\"avgExchangeRate\": %.4f, " +
                                        "\"minAmount\": %.2f, " +
                                        "\"maxAmount\": %.2f }",
                                window.getStart(),
                                window.getEnd(),
                                totalCount,
                                successCount,
                                failedCount,
                                avgAmount,
                                avgRate,
                                minAmount,
                                maxAmount
                        );

                        out.collect(metricsJson);
                    }
                })
                .name("Tumbling Window Aggregation"); // FIX: .name() moved here

        // --- JDBC Sink Configuration ---
        final String dbUrl = System.getenv().getOrDefault("METRICS_DB_URL", "jdbc:postgresql://host.docker.internal:5432/remittance_db");
        final String dbUser = System.getenv().getOrDefault("METRICS_DB_USER", "postgres");
        final String dbPassword = System.getenv().getOrDefault("METRICS_DB_PASSWORD", "manish1234");

        final String insertSql =  "INSERT INTO remittance_metrics " +
                "(window_start, window_end, count, avg_amount, avg_exchange_rate, min_amount, max_amount,success_count,failure_count) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?,?,?)";

        // Convert JSON string → Metric POJO for JDBC sink
        DataStream<Metric> metricPojoStream = metricsStream.map(json ->
                new ObjectMapper().readValue(json, Metric.class)
        ).name("JSON to Metric POJO");

        metricPojoStream.addSink(
                JdbcSink.sink(
                        insertSql,
                        (PreparedStatement ps, Metric metric) -> {
                            ps.setLong(1, metric.getWindowStart());
                            ps.setLong(2, metric.getWindowEnd());
                            ps.setLong(3, metric.getCount());
                            ps.setDouble(4, metric.getAvgAmount());
                            ps.setDouble(5, metric.getAvgExchangeRate());
                            ps.setDouble(6, metric.getMinAmount());
                            ps.setDouble(7, metric.getMaxAmount());
                            ps.setInt(8,metric.getSuccessCount());
                            ps.setInt(9,metric.getFailureCount());
                        },
                        new JdbcExecutionOptions.Builder()
                                .withBatchSize(1)
                                .withBatchIntervalMs(200)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(dbUrl)
                                .withDriverName("org.postgresql.Driver")
                                .withUsername(dbUser)
                                .withPassword(dbPassword)
                                .build()
                )
        ).name("PostgreSQL Metrics Sink");

        // --- Output metrics ---
        metricsStream.print();

        env.execute("Remittance Metrics Job");
    }
}
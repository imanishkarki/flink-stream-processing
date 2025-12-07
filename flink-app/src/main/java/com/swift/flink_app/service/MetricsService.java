package com.swift.flink_app.service;

import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Spring Boot Service to fetch metrics from Prometheus and push them to the frontend
 * via WebSockets (STOMP).
 */
@Service
public class MetricsService {

    private final SimpMessagingTemplate template;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    // Configuration for the metrics to query (PromQL queries)
    // Using LinkedHashMap to preserve order for dashboard display
    private static final Map<String, String> METRICS_QUERIES = new LinkedHashMap<>();

    static {
        // Throughput Metrics
        METRICS_QUERIES.put("input_rate",
                "sum(rate(flink_taskmanager_job_task_operator_numRecordsIn[1m]))");
        METRICS_QUERIES.put("output_rate",
                "sum(rate(flink_taskmanager_job_task_operator_numRecordsOut[1m]))");

        // Latency Metrics
        METRICS_QUERIES.put("avg_latency",
                "avg(flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency)");
        METRICS_QUERIES.put("max_latency",
                "max(flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency)");

        // Checkpoint Metrics
        METRICS_QUERIES.put("checkpoint_size",
                "flink_jobmanager_job_lastCheckpointSize");
        METRICS_QUERIES.put("checkpoint_duration",
                "flink_jobmanager_job_lastCheckpointDuration");

        // Backpressure & Performance
        METRICS_QUERIES.put("backpressure",
                "avg(flink_taskmanager_job_task_buffers_outPoolUsage)");
        METRICS_QUERIES.put("cpu_usage",
                "avg(flink_taskmanager_Status_JVM_CPU_Load)");

        // Memory Usage
        METRICS_QUERIES.put("heap_memory_used",
                "avg(flink_taskmanager_Status_JVM_Memory_Heap_Used)");
        METRICS_QUERIES.put("heap_memory_max",
                "avg(flink_taskmanager_Status_JVM_Memory_Heap_Max)");
    }

    private static final String PROMETHEUS_URL = "http://localhost:9070/api/v1/query?query=";

    public MetricsService(SimpMessagingTemplate template) {
        this.template = template;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Executes every 5 seconds to query Prometheus and broadcast metrics via WebSocket.
     */
    @Scheduled(fixedRate = 5000)
    public void fetchAndBroadcastMetrics() {
        for (Map.Entry<String, String> entry : METRICS_QUERIES.entrySet()) {
            String metricId = entry.getKey();
            String query = entry.getValue();

            try {
                String resultJson = queryPrometheus(query);
                JsonNode root = objectMapper.readTree(resultJson);
                JsonNode result = root.at("/data/result/0");

                if (result.isMissingNode() || result.isEmpty()) {
                    System.out.println("No data returned for metric: " + metricId + ". Prometheus likely not receiving Flink data.");
                    continue;
                }

                String jobName = result.at("/metric/job_name").asText("N/A");
                double rawValue = result.at("/value/1").asDouble();
                String formattedValue = formatMetricValue(metricId, rawValue);

                Map<String, String> payload = Map.of(
                        "id", metricId,
                        "value", formattedValue,
                        "jobName", jobName
                );

                template.convertAndSend("/topic/metrics", payload);

            } catch (Exception e) {
                String errorMsg = e.getMessage() != null ? e.getMessage() :
                        e.getClass().getName() + " (Check if Prometheus is running on http://localhost:9070)";
                System.err.println("Error fetching metric " + metricId + ": " + errorMsg);
            }
        }
    }

    /**
     * Format metric values based on metric type
     */
    private String formatMetricValue(String metricId, double rawValue) {
        switch (metricId) {
            case "checkpoint_size":
            case "heap_memory_used":
            case "heap_memory_max":
                // Convert bytes to MB
                return String.format("%.2f", rawValue / (1024 * 1024));

            case "checkpoint_duration":
            case "avg_latency":
            case "max_latency":
                // Milliseconds - keep as is
                return String.format("%.2f", rawValue);

            case "cpu_usage":
            case "backpressure":
                // Percentage (0-1 to 0-100)
                return String.format("%.1f", rawValue * 100);

            default:
                // Default: 2 decimal places
                return String.format("%.2f", rawValue);
        }
    }

    /**
     * Executes the PromQL query against the Prometheus API.
     */
    private String queryPrometheus(String query) throws Exception {
        String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8.toString());
        URI uri = new URI(PROMETHEUS_URL + encodedQuery);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofSeconds(10))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("Prometheus query failed with status code: " +
                    response.statusCode() + " for query: " + query);
        }

        return response.body();
    }
}
//package com.swift.flink_app.controller;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import com.swift.flink_app.service.PrometheusService;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.messaging.simp.SimpMessagingTemplate;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Controller;
//
//    /**
//     * WebSocket controller that periodically queries Prometheus and sends metrics to clients.
//     */
//    @Controller
//    public class MetricsWebSocketController {
//
//        private static final Logger logger = LoggerFactory.getLogger(MetricsWebSocketController.class);
//
//        private final SimpMessagingTemplate messagingTemplate;
//        private final PrometheusService prometheusService;
//
//        @Autowired
//        public MetricsWebSocketController(SimpMessagingTemplate messagingTemplate, PrometheusService prometheusService) {
//            this.messagingTemplate = messagingTemplate;
//            this.prometheusService = prometheusService;
//        }
//
//        /**
//         * Periodically called method to fetch metrics and send over WebSocket.
//         * Requires scheduling to be enabled in the Spring application.
//         */
//        @Scheduled(fixedRate = 5000)
//        public void sendMetrics() {
//            try {
//                Map<String, Double> metricsMap = new HashMap<>();
//
//                // Example metrics queries (adjust PromQL as needed)
//                double recordsInRate = prometheusService.queryForDouble("increase(num_records_in_total[5m])");
//                metricsMap.put("recordsInRate", recordsInRate);
//
//                double memoryUsage = prometheusService.queryForDouble("sum(container_memory_usage_bytes)");
//                metricsMap.put("memoryUsageBytes", memoryUsage);
//
//                double runningJobs = prometheusService.queryForDouble("count(my_running_jobs)");
//                metricsMap.put("runningJobs", runningJobs);
//
//                // Add more metrics as needed...
//
//                // Send the metrics map to WebSocket topic (e.g., /topic/metrics)
//                messagingTemplate.convertAndSend("/topic/metrics", metricsMap);
//                logger.info("Sent metrics to WebSocket clients: {}", metricsMap);
//            } catch (Exception e) {
//                logger.error("Error while sending metrics to WebSocket: {}", e.getMessage(), e);
//            }
//        }
////    }

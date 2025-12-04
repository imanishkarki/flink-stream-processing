//package com.swift.flink_app.service;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.stereotype.Service;
//import org.springframework.web.client.HttpClientErrorException;
//import org.springframework.web.client.RestClientException;
//import org.springframework.web.client.RestTemplate;
//import org.springframework.web.util.UriComponentsBuilder;
//
//import java.net.URLEncoder;
//import java.nio.charset.StandardCharsets;
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//
//@Service
//public class PrometheusService {
//
//    private static final Logger logger = LoggerFactory.getLogger(PrometheusService.class);
//
//    // Base URL for the Prometheus server
//    private static final String PROMETHEUS_BASE_URL = "http://localhost:9070";
//
//    private final RestTemplate restTemplate = new RestTemplate();
//
//    /**
//     * Queries Prometheus using the HTTP API and returns a numeric result.
//     * Returns 0.0 if no result or on error.
//     *
//     * @param promql the PromQL query expression
//     * @return the numeric value of the query result (summing if multiple series), or 0.0 on failure
//     */
//    @SuppressWarnings("unchecked")
//    public double queryForDouble(String promql) {
//        if (promql == null || promql.isEmpty()) {
//            logger.warn("PromQL query is null or empty.");
//            return 0.0;
//        }
//        try {
//            // Encode the query and build the URL
//            String url = PROMETHEUS_BASE_URL + "/api/v1/query?query="
//                    + URLEncoder.encode(promql, StandardCharsets.UTF_8.name());
//            // Call Prometheus HTTP API
//            Map<String, Object> response = restTemplate.getForObject(url, Map.class);
//            if (response == null) {
//                logger.error("Empty response from Prometheus for query: {}", promql);
//                return 0.0;
//            }
//            // Check status
//            String status = (String) response.get("status");
//            if (!"success".equalsIgnoreCase(status)) {
//                Object errorType = response.get("errorType");
//                Object error = response.get("error");
//                logger.error("Prometheus query returned error. Type: {}, Error: {}", errorType, error);
//                return 0.0;
//            }
//            // Parse data
//            Map<String, Object> data = (Map<String, Object>) response.get("data");
//            if (data == null) {
//                logger.error("No data field in Prometheus response for query: {}", promql);
//                return 0.0;
//            }
//            String resultType = (String) data.get("resultType");
//            Object resultObj = data.get("result");
//
//            // Handle vector result (instant queries)
//            if ("vector".equals(resultType)) {
//                List<Map<String, Object>> resultList = (List<Map<String, Object>>) resultObj;
//                if (resultList == null || resultList.isEmpty()) {
//                    return 0.0;
//                }
//                double sum = 0.0;
//                for (Map<String, Object> series : resultList) {
//                    // Each series has a "value": [ <timestamp>, "<value>" ]
//                    Object valueObj = series.get("value");
//                    if (valueObj instanceof List) {
//                        List<Object> valueList = (List<Object>) valueObj;
//                        if (valueList.size() >= 2) {
//                            Object valStr = valueList.get(1);
//                            try {
//                                double val = Double.parseDouble(valStr.toString());
//                                sum += val;
//                            } catch (NumberFormatException nfe) {
//                                logger.error("Failed to parse numeric value from Prometheus result: {}", valStr, nfe);
//                            }
//                        }
//                    }
//                }
//                return sum;
//            }
//            // Handle matrix result (range queries) by taking last value of each series
//            else if ("matrix".equals(resultType)) {
//                List<Map<String, Object>> resultList = (List<Map<String, Object>>) resultObj;
//                if (resultList == null || resultList.isEmpty()) {
//                    return 0.0;
//                }
//                double sum = 0.0;
//                for (Map<String, Object> series : resultList) {
//                    // Each series has "values": [ [ <timestamp>, "<value>" ], ... ]
//                    Object valuesObj = series.get("values");
//                    if (valuesObj instanceof List) {
//                        List<List<Object>> valuesList = (List<List<Object>>) valuesObj;
//                        if (!valuesList.isEmpty()) {
//                            List<Object> lastValue = valuesList.get(valuesList.size() - 1);
//                            if (lastValue.size() >= 2) {
//                                Object valStr = lastValue.get(1);
//                                try {
//                                    double val = Double.parseDouble(valStr.toString());
//                                    sum += val;
//                                } catch (NumberFormatException nfe) {
//                                    logger.error("Failed to parse numeric value from Prometheus range result: {}", valStr, nfe);
//                                }
//                            }
//                        }
//                    }
//                }
//                return sum;
//            }
//            // Handle scalar result
//            else if ("scalar".equals(resultType)) {
//                List<Object> resultList = (List<Object>) resultObj;
//                if (resultList == null || resultList.size() < 2) {
//                    return 0.0;
//                }
//                Object valStr = resultList.get(1);
//                try {
//                    return Double.parseDouble(valStr.toString());
//                } catch (NumberFormatException nfe) {
//                    logger.error("Failed to parse numeric scalar value from Prometheus result: {}", valStr, nfe);
//                    return 0.0;
//                }
//            }
//            // Unsupported type
//            else {
//                logger.warn("Unsupported resultType '{}' in Prometheus response for query: {}", resultType, promql);
//                return 0.0;
//            }
//        } catch (Exception e) {
//            logger.error("Exception while querying Prometheus with query '{}': {}", promql, e.getMessage(), e);
//            return 0.0;
//        }
//    }
//}

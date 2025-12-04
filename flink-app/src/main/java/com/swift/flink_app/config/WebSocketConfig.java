package com.swift.flink_app.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * WebSocketConfig
 * This class configures the WebSocket message broker using STOMP.
 * It is required to enable the necessary infrastructure beans, including SimpMessagingTemplate,
 * which the MetricsService uses to push messages to the clients.
 */
@Configuration
@EnableWebSocketMessageBroker // Enables WebSocket message handling, backed by a message broker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    /**
     * Registers a STOMP endpoint to which clients can connect.
     * The /metrics-feed endpoint is used by the Canvas frontend (index.html) for initial connection.
     *
     * @param registry the StompEndpointRegistry
     */
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // Registers the endpoint /metrics-feed, enabling SockJS fallback options.
        // The 'setAllowedOriginPatterns("*")' is essential for local development
        // or when the frontend (index.html) is served from a different domain/port.
        registry.addEndpoint("/metrics-feed")
                .setAllowedOriginPatterns("*") // Allows connection from any origin (e.g., frontend served locally)
                .withSockJS(); // Enables SockJS fallback for browsers that don't support WebSockets
    }

    /**
     * Configures the message broker options.
     *
     * @param config the MessageBrokerRegistry
     */
    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        // Sets the prefix for destinations handled by the application (Controller methods).
        // (Not strictly necessary for a push-only service like MetricsService, but good practice.)
        config.setApplicationDestinationPrefixes("/app");

        // Enables a simple in-memory message broker and prefixes destinations for server-to-client messages.
        // The MetricsService uses '/topic/metrics' which starts with '/topic'.
        config.enableSimpleBroker("/topic");
    }
}
package com.tahomatracker.service.api;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/**
 * Base class for API Lambda handlers using Function URLs.
 * Provides common HTTP request/response handling functionality.
 */
@Slf4j
public abstract class BaseApiHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    protected String getHttpMethod(Map<String, Object> event) {
        @SuppressWarnings("unchecked")
        Map<String, Object> requestContext = (Map<String, Object>) event.get("requestContext");
        if (requestContext != null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> http = (Map<String, Object>) requestContext.get("http");
            if (http != null) {
                return (String) http.getOrDefault("method", "UNKNOWN");
            }
        }
        return "UNKNOWN";
    }

    protected String getPath(Map<String, Object> event) {
        @SuppressWarnings("unchecked")
        Map<String, Object> requestContext = (Map<String, Object>) event.get("requestContext");
        if (requestContext != null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> http = (Map<String, Object>) requestContext.get("http");
            if (http != null) {
                return (String) http.getOrDefault("path", "/");
            }
        }
        return "/";
    }

    protected String getOrigin(Map<String, Object> event) {
        Object headersObj = event.get("headers");
        if (headersObj instanceof Map<?, ?> headers) {
            Object origin = headers.get("origin");
            if (origin == null) {
                origin = headers.get("Origin");
            }
            return origin != null ? origin.toString() : null;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, String> getQueryParameters(Map<String, Object> event) {
        Object queryParams = event.get("queryStringParameters");
        if (queryParams instanceof Map<?, ?>) {
            return (Map<String, String>) queryParams;
        }
        return new HashMap<>();
    }

    protected String getBody(Map<String, Object> event) {
        Object body = event.get("body");
        if (body == null) {
            return null;
        }

        String bodyStr = body.toString();
        Boolean isBase64 = (Boolean) event.get("isBase64Encoded");
        if (Boolean.TRUE.equals(isBase64)) {
            bodyStr = new String(java.util.Base64.getDecoder().decode(bodyStr));
        }
        return bodyStr;
    }

    protected Map<String, Object> handleOptions(String origin) {
        return corsResponse(204, null, origin);
    }

    protected Map<String, Object> validateApiSecret(Map<String, Object> event, String expectedSecret, String origin) {
        if (expectedSecret == null || expectedSecret.isBlank()) {
            return null; // Secret not configured; skip validation to avoid blocking dev/local
        }

        String provided = getHeader(event, "x-api-secret");
        if (provided == null || !provided.equals(expectedSecret)) {
            log.warn("API secret validation failed");
            return errorResponse(403, "Forbidden", origin);
        }
        return null;
    }

    protected String getHeader(Map<String, Object> event, String headerName) {
        Object headersObj = event.get("headers");
        if (headersObj instanceof Map<?, ?> headers) {
            for (var entry : headers.entrySet()) {
                if (entry.getKey() != null && headerName.equalsIgnoreCase(entry.getKey().toString())) {
                    Object value = entry.getValue();
                    return value != null ? value.toString() : null;
                }
            }
        }
        return null;
    }

    protected Map<String, Object> jsonResponse(int statusCode, Object body, String origin) {
        try {
            String json = MAPPER.writeValueAsString(body);
            return corsResponse(statusCode, json, origin);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize response", e);
            return corsResponse(500, "{\"success\":false,\"error\":\"Internal error\"}", origin);
        }
    }

    protected Map<String, Object> errorResponse(int statusCode, String message, String origin) {
        Map<String, Object> error = Map.of("success", false, "error", message);
        return jsonResponse(statusCode, error, origin);
    }

    protected Map<String, Object> corsResponse(int statusCode, String body, String origin) {
        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", statusCode);
        response.put("headers", corsHeaders(origin));
        if (body != null) {
            response.put("body", body);
        }
        return response;
    }

    protected Map<String, String> corsHeaders(String requestOrigin) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        return headers;
    }
}

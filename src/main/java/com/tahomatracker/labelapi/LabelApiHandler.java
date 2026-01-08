package com.tahomatracker.labelapi;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tahomatracker.di.LabelApiModule;
import com.tahomatracker.labelapi.dto.LabelRequest;
import com.tahomatracker.labelapi.dto.LabelResponse;

import lombok.extern.slf4j.Slf4j;

/**
 * Lambda handler for the Label API.
 *
 * Exposes a Function URL endpoint for crowdsource label submissions.
 * Uses Dagger for dependency injection to keep connections warm.
 *
 * Request format (Function URL payload format 2.0):
 * - POST /: Submit a label vote
 * - OPTIONS /: CORS preflight
 */
@Slf4j
public class LabelApiHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Singleton component - initialized once, reused across invocations
    private static volatile LabelApiComponent component;

    private final LabelSubmissionService submissionService;
    private final LabelApiConfig config;

    /**
     * Production constructor - uses singleton Dagger component.
     */
    public LabelApiHandler() {
        LabelApiComponent comp = getOrCreateComponent();
        this.submissionService = comp.labelSubmissionService();
        this.config = comp.config();
    }

    /**
     * Test constructor for dependency injection.
     */
    public LabelApiHandler(LabelSubmissionService submissionService, LabelApiConfig config) {
        this.submissionService = submissionService;
        this.config = config;
    }

    private static LabelApiComponent getOrCreateComponent() {
        if (component == null) {
            synchronized (LabelApiHandler.class) {
                if (component == null) {
                    LabelApiConfig config = LabelApiConfig.fromEnvironment();
                    component = DaggerLabelApiComponent.builder()
                            .labelApiModule(new LabelApiModule(config))
                            .build();
                }
            }
        }
        return component;
    }

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        String method = getHttpMethod(event);
        String origin = getOrigin(event);
        log.info("Received {} request", method);

        return switch (method) {
            case "OPTIONS" -> handleOptions(origin);
            case "POST" -> handlePost(event, origin);
            default -> errorResponse(405, "Method not allowed");
        };
    }

    private Map<String, Object> handleOptions(String origin) {
        return corsResponse(204, null, origin);
    }

    private Map<String, Object> handlePost(Map<String, Object> event, String origin) {
        String body = getBody(event);
        if (body == null || body.isBlank()) {
            return errorResponse(400, "Request body is required", origin);
        }

        LabelRequest request;
        try {
            request = MAPPER.readValue(body, LabelRequest.class);
        } catch (JsonProcessingException e) {
            log.warn("Invalid JSON: {}", e.getMessage());
            return errorResponse(400, "Invalid JSON: " + e.getMessage(), origin);
        }

        LabelResponse response = submissionService.submitLabel(request);
        int statusCode = response.success() ? 200 : 400;
        return jsonResponse(statusCode, response, origin);
    }

    private String getHttpMethod(Map<String, Object> event) {
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

    private String getOrigin(Map<String, Object> event) {
        Object headersObj = event.get("headers");
        if (headersObj instanceof Map<?, ?> headers) {
            Object origin = headers.get("origin");
            if (origin == null) {
                origin = headers.get("Origin"); // fallback in case case is preserved
            }
            return origin != null ? origin.toString() : null;
        }
        return null;
    }

    private String getBody(Map<String, Object> event) {
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

    private Map<String, Object> jsonResponse(int statusCode, Object body, String origin) {
        try {
            String json = MAPPER.writeValueAsString(body);
            return corsResponse(statusCode, json, origin);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize response", e);
            return corsResponse(500, "{\"success\":false,\"error\":\"Internal error\"}", origin);
        }
    }

    private Map<String, Object> errorResponse(int statusCode, String message) {
        return errorResponse(statusCode, message, null);
    }

    private Map<String, Object> errorResponse(int statusCode, String message, String origin) {
        LabelResponse error = LabelResponse.error(message);
        return jsonResponse(statusCode, error, origin);
    }

    private Map<String, Object> corsResponse(int statusCode, String body, String origin) {
        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", statusCode);
        response.put("headers", corsHeaders(origin));
        if (body != null) {
            response.put("body", body);
        }
        return response;
    }

    private Map<String, String> corsHeaders(String requestOrigin) {
        String originHeader = resolveOrigin(requestOrigin);
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        if (originHeader != null) {
            headers.put("Access-Control-Allow-Origin", originHeader);
        }
        headers.put("Access-Control-Allow-Methods", "POST, OPTIONS");
        headers.put("Access-Control-Allow-Headers", "Content-Type, Authorization");
        return headers;
    }

    private String resolveOrigin(String requestOrigin) {
        if (config.allowedOrigins().contains("*")) {
            return "*";
        }
        if (requestOrigin != null && config.allowedOrigins().contains(requestOrigin)) {
            return requestOrigin;
        }
        return null;
    }
}

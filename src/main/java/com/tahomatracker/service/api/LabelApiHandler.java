package com.tahomatracker.service.api;

import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.tahomatracker.service.api.BaseApiHandler;
import com.tahomatracker.service.modules.LabelApiModule;
import com.tahomatracker.service.api.dto.LabelRequest;
import com.tahomatracker.service.api.dto.LabelResponse;

import lombok.extern.slf4j.Slf4j;

/**
 * Lambda handler for the Label API.
 * Extends BaseApiHandler for common HTTP functionality.
 */
@Slf4j
public class LabelApiHandler extends BaseApiHandler {

    private static volatile LabelApiComponent component;

    private final LabelSubmissionService submissionService;
    private final LabelApiConfig config;

    public LabelApiHandler() {
        LabelApiComponent comp = getOrCreateComponent();
        this.submissionService = comp.labelSubmissionService();
        this.config = comp.config();
    }

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
            default -> errorResponse(405, "Method not allowed", origin);
        };
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
}

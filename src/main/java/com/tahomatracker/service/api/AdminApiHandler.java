package com.tahomatracker.service.api;

import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.tahomatracker.service.api.dto.BatchLabelRequest;
import com.tahomatracker.service.api.dto.BatchLabelResponse;
import com.tahomatracker.service.api.dto.LabelsResponse;
import com.tahomatracker.service.modules.AdminApiModule;

import lombok.extern.slf4j.Slf4j;

/**
 * Lambda handler for the Admin API.
 * Extends BaseApiHandler for common HTTP functionality.
 */
@Slf4j
public class AdminApiHandler extends BaseApiHandler {

    private static volatile AdminApiComponent component;

    private final AdminLabelService adminLabelService;
    private final AdminApiConfig config;

    public AdminApiHandler() {
        AdminApiComponent comp = getOrCreateComponent();
        this.adminLabelService = comp.adminLabelService();
        this.config = comp.config();
    }

    public AdminApiHandler(AdminLabelService adminLabelService, AdminApiConfig config) {
        this.adminLabelService = adminLabelService;
        this.config = config;
    }

    private static AdminApiComponent getOrCreateComponent() {
        if (component == null) {
            synchronized (AdminApiHandler.class) {
                if (component == null) {
                    AdminApiConfig config = AdminApiConfig.fromEnvironment();
                    component = DaggerAdminApiComponent.builder()
                            .adminApiModule(new AdminApiModule(config))
                            .build();
                }
            }
        }
        return component;
    }

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        String method = getHttpMethod(event);
        String path = getPath(event);
        String origin = getOrigin(event);
        log.info("Received {} request to {}", method, path);

        return switch (method) {
            case "OPTIONS" -> handleOptions(origin);
            case "GET" -> handleGet(event, origin);
            case "POST" -> handlePost(event, path, origin);
            default -> errorResponse(405, "Method not allowed", origin);
        };
    }

    private Map<String, Object> handleGet(Map<String, Object> event, String origin) {
        Map<String, String> queryParams = getQueryParameters(event);
        
        String startDate = queryParams.get("startDate");
        String endDate = queryParams.get("endDate");
        
        if (startDate == null || endDate == null) {
            return errorResponse(400, "startDate and endDate parameters are required", origin);
        }

        String excludeLabeled = queryParams.get("excludeLabeled");
        String labelSource = queryParams.get("labelSource");

        try {
            LabelsResponse response = adminLabelService.getLabelsForDateRange(
                startDate, endDate, excludeLabeled, labelSource);
            return jsonResponse(200, response, origin);
        } catch (Exception e) {
            log.error("Failed to get labels: {}", e.getMessage(), e);
            return errorResponse(500, "Failed to retrieve labels", origin);
        }
    }

    private Map<String, Object> handlePost(Map<String, Object> event, String path, String origin) {
        if (!"/batch".equals(path) && !path.endsWith("/batch")) {
            return errorResponse(404, "Not found", origin);
        }

        String body = getBody(event);
        if (body == null || body.isBlank()) {
            return errorResponse(400, "Request body is required", origin);
        }

        BatchLabelRequest request;
        try {
            request = MAPPER.readValue(body, BatchLabelRequest.class);
        } catch (JsonProcessingException e) {
            log.warn("Invalid JSON: {}", e.getMessage());
            return errorResponse(400, "Invalid JSON: " + e.getMessage(), origin);
        }

        try {
            BatchLabelResponse response = adminLabelService.submitBatchLabels(request);
            int statusCode = response.success() ? 200 : 400;
            return jsonResponse(statusCode, response, origin);
        } catch (Exception e) {
            log.error("Failed to submit batch labels: {}", e.getMessage(), e);
            return errorResponse(500, "Failed to submit labels", origin);
        }
    }
}

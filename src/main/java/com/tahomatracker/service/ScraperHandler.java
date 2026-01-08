package com.tahomatracker.service;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.tahomatracker.di.ScraperModule;
import com.tahomatracker.service.process.ImageScrapingService;
import org.slf4j.MDC;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Lambda handler for Tahoma Tracker image scraping and processing.
 *
 * Uses Dagger for dependency injection to keep connections warm across invocations.
 */
@Slf4j
public class ScraperHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    // Singleton component - initialized once, reused across invocations
    private static volatile ScraperComponent component;

    private final ImageScrapingService scrapingService;

    /**
     * Production constructor - uses singleton Dagger component.
     */
    public ScraperHandler() {
        ScraperComponent comp = getOrCreateComponent();
        this.scrapingService = comp.imageScrapingService();
    }

    /**
     * Test constructor for dependency injection.
     */
    public ScraperHandler(ImageScrapingService scrapingService) {
        this.scrapingService = scrapingService;
    }

    private static ScraperComponent getOrCreateComponent() {
        if (component == null) {
            synchronized (ScraperHandler.class) {
                if (component == null) {
                    ScraperConfig config = new ScraperConfig();
                    component = DaggerScraperComponent.builder()
                            .scraperModule(new ScraperModule(config))
                            .build();
                }
            }
        }
        return component;
    }

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        String requestId = context != null ? context.getAwsRequestId() : null;
        if (requestId != null) {
            MDC.put("aws_request_id", requestId);
        }
        try {
            log.info("Starting image scraping");
            return scrapingService.run(context);

        } catch (IOException | InterruptedException ex) {
            log.error("I/O error: {}", ex.getMessage(), ex);
            return buildErrorResponse(ex.getMessage());
        } catch (Exception ex) {
            log.error("Scraping failed", ex);
            return buildErrorResponse(ex.getMessage());
        } finally {
            MDC.clear();
        }
    }

    private Map<String, Object> buildErrorResponse(String error) {
        Map<String, Object> response = new HashMap<>();
        response.put("timestamp", isoformat(Instant.now()));
        response.put("status", "error");
        response.put("error", error);
        return response;
    }

    private String isoformat(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC)
                .withNano(0)
                .toString()
                .replace("+00:00", "Z");
    }
}

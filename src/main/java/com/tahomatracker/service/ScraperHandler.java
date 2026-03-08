package com.tahomatracker.service;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.tahomatracker.service.modules.ScraperModule;
import com.tahomatracker.service.scraper.ImageScrapingService;
import org.slf4j.MDC;
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

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Scraping interrupted: {}", ex.getMessage(), ex);
            throw new RuntimeException("Scraping interrupted", ex);
        } catch (Exception ex) {
            log.error("Scraping failed: {}", ex.getMessage(), ex);
            throw new RuntimeException("Scraping failed: " + ex.getMessage(), ex);
        } finally {
            MDC.clear();
        }
    }

}

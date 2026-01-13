package com.tahomatracker.service;

import javax.inject.Singleton;

import com.tahomatracker.service.modules.AwsClientsModule;
import com.tahomatracker.service.modules.ScraperModule;
import com.tahomatracker.service.scraper.ImageScrapingService;

import dagger.Component;

/**
 * Dagger component for the Scraper Lambda.
 *
 * Combines the shared AWS clients module with Scraper specific bindings.
 * The component is singleton-scoped to ensure clients are reused across invocations.
 */
@Singleton
@Component(modules = {AwsClientsModule.class, ScraperModule.class})
public interface ScraperComponent {

    /**
     * Provides the ImageScrapingService with all dependencies injected.
     */
    ImageScrapingService imageScrapingService();

    /**
     * Provides the config.
     */
    ScraperConfig config();

    /**
     * Builder for the component, allowing module instances to be provided.
     */
    @Component.Builder
    interface Builder {
        Builder scraperModule(ScraperModule module);
        ScraperComponent build();
    }
}

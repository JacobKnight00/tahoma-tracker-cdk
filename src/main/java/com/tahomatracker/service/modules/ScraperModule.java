package com.tahomatracker.service.modules;

import javax.inject.Singleton;

import com.tahomatracker.backfill.FastSliceFetcher;
import com.tahomatracker.service.ScraperConfig;
import com.tahomatracker.service.classifier.FrameStateClassifier;
import com.tahomatracker.service.classifier.OnnxFrameStateClassifier;
import com.tahomatracker.service.classifier.OnnxModelLoader;
import com.tahomatracker.service.classifier.OnnxVisibilityClassifier;
import com.tahomatracker.service.classifier.VisibilityClassifier;
import com.tahomatracker.service.external.ObjectStorageClient;
import com.tahomatracker.service.external.S3ObjectStorageClient;
import com.tahomatracker.service.external.SliceFetcher;
import com.tahomatracker.service.scraper.AnalysisPersistenceService;
import com.tahomatracker.service.scraper.ImageAcquisitionService;
import com.tahomatracker.service.scraper.ImageClassificationService;
import com.tahomatracker.service.scraper.ImageScrapingService;
import com.tahomatracker.service.scraper.TimeWindowPlanner;

import dagger.Module;
import dagger.Provides;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Dagger module providing Scraper Lambda specific dependencies.
 *
 * Provides all services needed for image scraping pipeline:
 * classifiers, acquisition, persistence, and orchestration.
 */
@Module
public class ScraperModule {

    private static final int SLICE_FETCHER_THREADS = 4;

    private final ScraperConfig config;

    public ScraperModule(ScraperConfig config) {
        this.config = config;
    }

    @Provides
    @Singleton
    ScraperConfig provideConfig() {
        return config;
    }

    @Provides
    @Singleton
    ObjectStorageClient provideObjectStorageClient(S3Client s3Client) {
        return new S3ObjectStorageClient(s3Client, config.bucketName);
    }

    @Provides
    @Singleton
    SliceFetcher provideSliceFetcher() {
        return new FastSliceFetcher(config.cameraBaseUrl, SLICE_FETCHER_THREADS);
    }

    @Provides
    @Singleton
    OnnxModelLoader provideOnnxModelLoader(S3Client s3Client) {
        return new OnnxModelLoader(s3Client, config.bucketName);
    }

    @Provides
    @Singleton
    FrameStateClassifier provideFrameStateClassifier(OnnxModelLoader modelLoader) {
        return new OnnxFrameStateClassifier(
                modelLoader,
                config.frameStateModelKey,
                config.modelInputWidth,
                config.modelInputHeight,
                config.normalizationMean,
                config.normalizationStd
        );
    }

    @Provides
    @Singleton
    VisibilityClassifier provideVisibilityClassifier(OnnxModelLoader modelLoader) {
        return new OnnxVisibilityClassifier(
                modelLoader,
                config.visibilityModelKey,
                config.modelInputWidth,
                config.modelInputHeight,
                config.normalizationMean,
                config.normalizationStd
        );
    }

    @Provides
    @Singleton
    ImageAcquisitionService provideImageAcquisitionService(
            SliceFetcher fetcher,
            ObjectStorageClient storage) {
        return new ImageAcquisitionService(
                fetcher, storage, config.panosPrefix, config.croppedPrefix);
    }

    @Provides
    @Singleton
    ImageClassificationService provideImageClassificationService(
            FrameStateClassifier frameStateClassifier,
            VisibilityClassifier visibilityClassifier,
            ObjectStorageClient storage) {
        return new ImageClassificationService(frameStateClassifier, visibilityClassifier, storage);
    }

    @Provides
    @Singleton
    AnalysisPersistenceService provideAnalysisPersistenceService(ObjectStorageClient storage) {
        return new AnalysisPersistenceService(storage, config.analysisPrefix,
                config.manifestsPrefix, config.modelVersions, config.localTz);
    }

    @Provides
    @Singleton
    TimeWindowPlanner provideTimeWindowPlanner() {
        return new TimeWindowPlanner(config);
    }

    @Provides
    @Singleton
    ImageScrapingService provideImageScrapingService(
            TimeWindowPlanner timeWindow,
            ImageAcquisitionService imageAcquisition,
            ImageClassificationService classification,
            AnalysisPersistenceService persistence,
            ObjectStorageClient storage) {
        return new ImageScrapingService(
                config, timeWindow, imageAcquisition,
                classification, persistence, storage);
    }
}

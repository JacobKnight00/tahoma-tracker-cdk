package com.tahomatracker.service.process;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.tahomatracker.service.ScraperConfig;
import com.tahomatracker.service.domain.AcquisitionResult;
import com.tahomatracker.service.domain.ClassificationResult;
import com.tahomatracker.service.domain.ImageContext;
import com.tahomatracker.service.domain.ImageId;
import com.tahomatracker.service.enums.AcquisitionStatus;
import com.tahomatracker.service.external.ObjectStorageClient;
import java.awt.image.BufferedImage;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class ImageScrapingServiceTest {

    @Test
    void processSingle_skipsWhenAnalysisExists() throws Exception {
        ScraperConfig config = new ScraperConfig();
        TimeWindowPlanner planner = new TimeWindowPlanner(config);
        LatestImageService latest = mock(LatestImageService.class);
        ImageAcquisitionService acquisition = mock(ImageAcquisitionService.class);
        ImageClassificationService classification = mock(ImageClassificationService.class);
        AnalysisPersistenceService persistence = mock(AnalysisPersistenceService.class);
        ObjectStorageClient storage = mock(ObjectStorageClient.class);

        // analysisExists() now uses persistence.formatAnalysisKey() to build the key
        when(persistence.formatAnalysisKey(any(ImageId.class))).thenReturn("analysis/v1/2026/01/06/1200.json");
        when(storage.exists(anyString())).thenReturn(true);

        ImageScrapingService service = new ImageScrapingService(config, planner, latest, acquisition, classification, persistence, storage);

        ImageContext ctx = service.processSingle(ZonedDateTime.now(ZoneId.of("UTC")), true);

        assertNull(ctx);
        verifyNoInteractions(acquisition, classification, latest);
    }

    @Test
    void processSingle_processesAndPublishesWhenNew() throws Exception {
        ScraperConfig config = new ScraperConfig();
        TimeWindowPlanner planner = new TimeWindowPlanner(config);
        LatestImageService latest = mock(LatestImageService.class);
        ImageAcquisitionService acquisition = mock(ImageAcquisitionService.class);
        ImageClassificationService classification = mock(ImageClassificationService.class);
        AnalysisPersistenceService persistence = mock(AnalysisPersistenceService.class);
        ObjectStorageClient storage = mock(ObjectStorageClient.class);

        // analysisExists() uses persistence.formatAnalysisKey() to build the key
        when(persistence.formatAnalysisKey(any(ImageId.class))).thenReturn("analysis/v1/2026/01/06/1200.json");
        when(storage.exists(anyString())).thenReturn(false);
        BufferedImage image = new BufferedImage(10, 10, BufferedImage.TYPE_INT_RGB);
        when(acquisition.fetchAndStitchPano(anyString())).thenReturn(new com.tahomatracker.service.domain.PanoResult(image, AcquisitionStatus.OK));
        when(acquisition.createCrop(any(), any())).thenReturn(image);
        when(acquisition.uploadImages(any(), any(), anyString())).thenReturn(new AcquisitionResult("pano", "crop", AcquisitionStatus.OK));
        when(classification.classify(any())).thenReturn(
                new ClassificationResult(null, 0.0, null, null, 0.0, null)
        );
        when(persistence.persistAnalysis(any(), any(ImageId.class))).thenReturn("analysis/key");

        ImageScrapingService service = new ImageScrapingService(config, planner, latest, acquisition, classification, persistence, storage);

        ImageContext ctx = service.processSingle(ZonedDateTime.now(ZoneId.of("UTC")), true);

        assertNotNull(ctx);
        assertEquals(AcquisitionStatus.OK, ctx.getStatus());
        verify(latest, times(1)).publishIfNew(any());
    }
}

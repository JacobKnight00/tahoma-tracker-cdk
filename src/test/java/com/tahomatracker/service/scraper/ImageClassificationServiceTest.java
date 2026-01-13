package com.tahomatracker.service.scraper;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.tahomatracker.service.classifier.FrameStateClassifier;
import com.tahomatracker.service.classifier.VisibilityClassifier;
import com.tahomatracker.service.domain.ClassificationResult;
import com.tahomatracker.service.enums.FrameState;
import com.tahomatracker.service.enums.Visibility;
import com.tahomatracker.service.external.ObjectStorageClient;
import java.awt.image.BufferedImage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ImageClassificationServiceTest {

    @Test
    void classify_skipsVisibilityWhenFrameNotGood() {
        BufferedImage img = new BufferedImage(4, 4, BufferedImage.TYPE_INT_RGB);

        FrameStateClassifier frame = mock(FrameStateClassifier.class);
        when(frame.predict(any())).thenReturn(Map.of(
                "frame_state_probabilities", Map.of("dark", 1.0)
        ));

        VisibilityClassifier visibility = mock(VisibilityClassifier.class);

        ImageClassificationService svc = new ImageClassificationService(frame, visibility, mock(ObjectStorageClient.class));

        ClassificationResult result = svc.classify(img);

        assertEquals(FrameState.DARK, result.getFrameState());
        assertEquals(1.0, result.getFrameStateProb());
        assertNull(result.getVisibility());
        assertNull(result.getVisibilityProbabilities());
        verify(visibility, never()).predict(any());
    }

    @Test
    void classify_runsVisibilityWhenFrameGood() {
        BufferedImage img = new BufferedImage(4, 4, BufferedImage.TYPE_INT_RGB);

        FrameStateClassifier frame = mock(FrameStateClassifier.class);
        when(frame.predict(any())).thenReturn(Map.of(
                "frame_state_probabilities", Map.of("good", 0.8, "bad", 0.2)
        ));

        VisibilityClassifier visibility = mock(VisibilityClassifier.class);
        when(visibility.predict(any())).thenReturn(Map.of(
                "visibility_probabilities", Map.of("out", 0.7, "not_out", 0.3)
        ));

        ImageClassificationService svc = new ImageClassificationService(frame, visibility, mock(ObjectStorageClient.class));

        ClassificationResult result = svc.classify(img);

        assertEquals(FrameState.GOOD, result.getFrameState());
        assertEquals(0.8, result.getFrameStateProb(), 1e-6);
        assertEquals(Visibility.OUT, result.getVisibility());
        assertEquals(0.7, result.getVisibilityProb(), 1e-6);
        assertEquals(2, result.getVisibilityProbabilities().size());
    }

    @Test
    void classifyFromS3_readsImageBytes() throws Exception {
        Path sample = Path.of("src/test/resources/sample_crop_watermarked.jpg");
        byte[] bytes = Files.readAllBytes(sample);

        FrameStateClassifier frame = mock(FrameStateClassifier.class);
        when(frame.predict(any())).thenReturn(Map.of(
                "frame_state_probabilities", Map.of("good", 1.0)
        ));
        VisibilityClassifier visibility = mock(VisibilityClassifier.class);
        when(visibility.predict(any())).thenReturn(Map.of(
                "visibility_probabilities", Map.of("out", 1.0)
        ));

        ObjectStorageClient storage = mock(ObjectStorageClient.class);
        when(storage.getObjectBytes("test/key")).thenReturn(bytes);

        ImageClassificationService svc = new ImageClassificationService(frame, visibility, storage);

        ClassificationResult result = svc.classifyFromS3("test/key");

        assertEquals(FrameState.GOOD, result.getFrameState());
        assertEquals(Visibility.OUT, result.getVisibility());
        verify(storage).getObjectBytes("test/key");
    }
}

package com.tahomatracker.service.scraper;

import com.tahomatracker.service.classifier.FrameStateClassifier;
import com.tahomatracker.service.classifier.VisibilityClassifier;
import com.tahomatracker.service.domain.ClassificationResult;
import com.tahomatracker.service.enums.FrameState;
import com.tahomatracker.service.enums.Visibility;
import com.tahomatracker.service.external.ObjectStorageClient;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.LinkedHashMap;
import javax.imageio.ImageIO;

/**
 * Service for classifying images using ML models (frame state + visibility).
 */
public class ImageClassificationService {
    private final FrameStateClassifier frameStateClassifier;
    private final VisibilityClassifier visibilityClassifier;
    private final ObjectStorageClient s3Store;

    public ImageClassificationService(FrameStateClassifier frameStateClassifier,
                                     VisibilityClassifier visibilityClassifier,
                                     ObjectStorageClient s3Store) {
        this.frameStateClassifier = frameStateClassifier;
        this.visibilityClassifier = visibilityClassifier;
        this.s3Store = s3Store;
    }

    /**
     * Classifies an image for frame state and visibility.
     *
     * @param image The image to classify
     * @return ClassificationResult with frame state and visibility predictions
     */
    public ClassificationResult classify(BufferedImage image) {
        // Run frame state classifier
        Map<String, Object> frameResult = frameStateClassifier.predict(image);
        Map<FrameState, Double> frameProbs = extractFrameStateProbabilities(frameResult);
        FrameState frameState = topClass(frameProbs);
        double frameStateProb = frameState != null ? frameProbs.getOrDefault(frameState, 0.0) : 0.0;

        // Run visibility classifier
        Map<Visibility, Double> visibilityProbs = null;
        Visibility visibility = null;
        double visibilityProb = 0.0;
        if (frameState == FrameState.GOOD) {
            Map<String, Object> visResult = visibilityClassifier.predict(image);
            visibilityProbs = extractVisibilityProbabilities(visResult);
            visibility = topClass(visibilityProbs);
            if (visibility != null) {
                visibilityProb = visibilityProbs.getOrDefault(visibility, 0.0);
            }
        }

        return new ClassificationResult(frameState, frameStateProb, frameProbs, visibility, visibilityProb, visibilityProbs);
    }

    /**
     * Loads an image from S3 and classifies it.
     *
     * @param s3Key The S3 key of the image to classify
     * @return ClassificationResult with frame state and visibility predictions
     */
    public ClassificationResult classifyFromS3(String s3Key) throws IOException {
        byte[] bytes = s3Store.getObjectBytes(s3Key);
        BufferedImage image = ImageIO.read(new ByteArrayInputStream(bytes));
        return classify(image);
    }

    private FrameState extractFrameState(Object value) {
        if (value instanceof FrameState) {
            return (FrameState) value;
        }
        if (value instanceof String) {
            return FrameState.fromValue((String) value);
        }
        return null;
    }

    private Visibility extractVisibility(Object value) {
        if (value instanceof Visibility) {
            return (Visibility) value;
        }
        if (value instanceof Boolean) {
            return (Boolean) value ? Visibility.OUT : Visibility.NOT_OUT;
        }
        if (value instanceof String) {
            return Visibility.fromValue((String) value);
        }
        return null;
    }

    private Map<FrameState, Double> extractFrameStateProbabilities(Map<String, Object> result) {
        Map<FrameState, Double> probabilities = new LinkedHashMap<>();
        Object probs = result.get("frame_state_probabilities");
        if (probs instanceof Map<?, ?> raw) {
            raw.forEach((k, v) -> {
                FrameState state = extractFrameState(k);
                if (state != null) {
                    probabilities.put(state, toDouble(v));
                }
            });
        }

        if (!probabilities.isEmpty()) {
            return probabilities;
        }

        FrameState top = extractFrameState(result.get("frame_state"));
        double prob = toDouble(result.get("frame_state_prob"));
        if (top != null) {
            probabilities.put(top, prob);
        }
        return probabilities;
    }

    private Map<Visibility, Double> extractVisibilityProbabilities(Map<String, Object> result) {
        Map<Visibility, Double> probabilities = new LinkedHashMap<>();
        Object probs = result.get("visibility_probabilities");
        if (probs instanceof Map<?, ?> raw) {
            raw.forEach((k, v) -> {
                Visibility vis = extractVisibility(k);
                if (vis != null) {
                    probabilities.put(vis, toDouble(v));
                }
            });
        }

        if (!probabilities.isEmpty()) {
            return probabilities;
        }

        Object rawVisibility = result.containsKey("visibility")
                ? result.get("visibility")
                : result.get("out");
        Visibility visibility = extractVisibility(rawVisibility);
        double visibilityProb = result.containsKey("visibility_prob")
                ? toDouble(result.get("visibility_prob"))
                : toDouble(result.get("out_prob"));
        if (visibility != null) {
            probabilities.put(visibility, visibilityProb);
        }
        return probabilities;
    }

    private <T> T topClass(Map<T, Double> probabilities) {
        return probabilities.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
    }

    private double toDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return 0.0;
    }
}

package com.tahomatracker.service.domain;

import com.tahomatracker.service.enums.FrameState;
import com.tahomatracker.service.enums.Visibility;
import lombok.Value;
import java.util.Map;

/**
 * Result of image classification (frame state + visibility).
 */
@Value
public class ClassificationResult {
    FrameState frameState; // top prediction (for gating/logging)
    double frameStateProb;
    Map<FrameState, Double> frameStateProbabilities;

    Visibility visibility; // top prediction (null when frame not GOOD)
    double visibilityProb;
    Map<Visibility, Double> visibilityProbabilities;
}

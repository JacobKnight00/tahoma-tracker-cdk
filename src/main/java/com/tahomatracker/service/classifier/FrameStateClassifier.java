package com.tahomatracker.service.classifier;

import java.awt.image.BufferedImage;
import java.util.Map;

public interface FrameStateClassifier {
    Map<String, Object> predict(BufferedImage img);
}

package com.tahomatracker.service.classifier;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;
import com.tahomatracker.service.enums.Visibility;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.nio.FloatBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ONNX-backed visibility classifier.
 */
public class OnnxVisibilityClassifier implements VisibilityClassifier {
    private static final Visibility[] CLASSES = new Visibility[]{
            Visibility.OUT, Visibility.PARTIALLY_OUT, Visibility.NOT_OUT
    };

    private final OnnxModelLoader loader;
    private final String modelKey;
    private final int width;
    private final int height;
    private final float[] mean;
    private final float[] std;
    private final OrtEnvironment env;

    public OnnxVisibilityClassifier(OnnxModelLoader loader,
                                    String modelKey,
                                    int width,
                                    int height,
                                    float[] mean,
                                    float[] std) {
        this.loader = loader;
        this.modelKey = modelKey;
        this.width = width;
        this.height = height;
        this.mean = mean;
        this.std = std;
        this.env = OrtEnvironment.getEnvironment();
    }

    @Override
    public Map<String, Object> predict(BufferedImage img) {
        try {
            BufferedImage resized = resize(img, width, height);
            float[] input = toCHW(resized, mean, std);
            long[] shape = new long[]{1, 3, height, width};

            OrtSession session = loader.loadSession(modelKey);
            try (OnnxTensor tensor = OnnxTensor.createTensor(env, FloatBuffer.wrap(input), shape);
                 OrtSession.Result result = session.run(Map.of(session.getInputNames().iterator().next(), tensor))) {

                float[] logits = ((float[][]) result.get(0).getValue())[0];
                Map<Visibility, Double> probs = softmax(logits);
                Map<String, Object> out = new LinkedHashMap<>();
                Map<String, Double> asStrings = new LinkedHashMap<>();
                probs.forEach((k, v) -> asStrings.put(k.getValue(), v));
                out.put("visibility_probabilities", asStrings);
                return out;
            }
        } catch (Exception ex) {
            throw new RuntimeException("Visibility ONNX inference failed", ex);
        }
    }

    private Map<Visibility, Double> softmax(float[] logits) {
        double max = Double.NEGATIVE_INFINITY;
        for (float logit : logits) {
            if (logit > max) {
                max = logit;
            }
        }
        double sum = 0.0;
        for (float logit : logits) {
            sum += Math.exp(logit - max);
        }
        Map<Visibility, Double> probs = new LinkedHashMap<>();
        for (int i = 0; i < logits.length && i < CLASSES.length; i++) {
            probs.put(CLASSES[i], Math.exp(logits[i] - max) / sum);
        }
        return probs;
    }

    private BufferedImage resize(BufferedImage input, int w, int h) {
        BufferedImage output = new BufferedImage(w, h, BufferedImage.TYPE_3BYTE_BGR);
        Graphics2D g = output.createGraphics();
        g.drawImage(input, 0, 0, w, h, null);
        g.dispose();
        return output;
    }

    private float[] toCHW(BufferedImage img, float[] mean, float[] std) {
        float[] data = new float[3 * height * width];
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int rgb = img.getRGB(x, y);
                float r = ((rgb >> 16) & 0xff) / 255.0f;
                float g = ((rgb >> 8) & 0xff) / 255.0f;
                float b = (rgb & 0xff) / 255.0f;

                int base = y * width + x;
                data[base] = (r - mean[0]) / std[0];
                data[width * height + base] = (g - mean[1]) / std[1];
                data[2 * width * height + base] = (b - mean[2]) / std[2];
            }
        }
        return data;
    }
}

package com.tahomatracker.service.util;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;


import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.imageio.ImageIO;

@Disabled("Manual watermark verification only; skip in automated test runs to avoid headless/IO overhead.")
public class ScraperUtilsWatermarkTest {

    @Test
    void applyWatermark_onSample() throws Exception {
        // Place a sample image at src/test/resources/sample_crop.jpg
        try (InputStream in = getClass().getResourceAsStream("/sample_crop.jpg")) {
            Assumptions.assumeTrue(in != null, "Add sample_crop.jpg under src/test/resources/");

            BufferedImage img = ImageIO.read(in);
            Assumptions.assumeTrue(img != null, "Failed to read sample image");

            ScraperUtils.setWatermarkImage(null);
            ScraperUtils.applyWatermark(img);

            Path outDir = Paths.get("src", "test", "output", "watermark");
            Files.createDirectories(outDir);
            Path out = outDir.resolve("watermarked-sample.jpg");
            ImageIO.write(img, "jpg", out.toFile());
            System.out.println("Watermarked sample written to: " + out.toAbsolutePath());
        }
    }
}

package com.tahomatracker.service.util;

import com.tahomatracker.service.domain.CropBox;
import java.awt.image.BufferedImage;

public final class ScraperUtils {

    private ScraperUtils() {
    }

    public static CropBox parseCropBox(CropBox cropBox, int width, int height) {
        CropBox parsed = cropBox != null ? cropBox : CropBox.fullImage(width, height);
        return parsed.clampToImage(width, height);
    }
}

package com.tahomatracker.service.util;

import static org.junit.jupiter.api.Assertions.*;

import com.tahomatracker.service.domain.CropBox;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ScraperUtilsTest {

    @Test
    public void parseCropBox_null_returnsFullImage() {
        CropBox res = ScraperUtils.parseCropBox(null, 100, 50);
        assertEquals(0, res.x1());
        assertEquals(0, res.y1());
        assertEquals(100, res.x2());
        assertEquals(50, res.y2());
    }

    @Test
    public void parseCropBox_clampsToImageBounds() {
        CropBox res = ScraperUtils.parseCropBox(new CropBox(-10, -5, 200, 60), 100, 50);
        assertEquals(0, res.x1());
        assertEquals(0, res.y1());
        assertEquals(100, res.x2());
        assertEquals(50, res.y2());
    }

    @Test
    public void stitchHorizontal_combinesWidths() throws IOException {
        BufferedImage a = new BufferedImage(10, 5, BufferedImage.TYPE_INT_RGB);
        BufferedImage b = new BufferedImage(20, 5, BufferedImage.TYPE_INT_RGB);
        var pano = ScraperUtils.stitchHorizontal(List.of(a, b));
        assertEquals(30, pano.getWidth());
        assertEquals(5, pano.getHeight());
    }
}

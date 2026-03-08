package com.tahomatracker.service.util;

import static org.junit.jupiter.api.Assertions.*;

import com.tahomatracker.service.domain.CropBox;
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
}

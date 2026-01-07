package com.tahomatracker.service.domain;

import com.tahomatracker.service.enums.AcquisitionStatus;
import lombok.Value;
import java.awt.image.BufferedImage;

/**
 * Result of fetching and stitching panorama slices.
 */
@Value
public class PanoResult {
    BufferedImage pano;
    AcquisitionStatus status;
}

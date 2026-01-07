package com.tahomatracker.service.domain;

import com.tahomatracker.service.enums.AcquisitionStatus;
import lombok.Value;

/**
 * Result of complete image acquisition (fetch, crop, upload).
 */
@Value
public class AcquisitionResult {
    String panoS3Key;
    String croppedS3Key;
    AcquisitionStatus status;
}

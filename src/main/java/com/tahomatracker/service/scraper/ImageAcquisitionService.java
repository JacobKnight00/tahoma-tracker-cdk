package com.tahomatracker.service.scraper;

import com.tahomatracker.service.domain.CropBox;
import com.tahomatracker.service.domain.AcquisitionResult;
import com.tahomatracker.service.domain.PanoResult;
import com.tahomatracker.service.enums.AcquisitionStatus;
import com.tahomatracker.service.external.ObjectStorageClient;
import com.tahomatracker.service.external.ImageFetcher;
import com.tahomatracker.service.util.ScraperUtils;
import java.awt.image.BufferedImage;
import java.io.IOException;

/**
 * Service for acquiring webcam images: fetch panorama, crop, and upload.
 */
public class ImageAcquisitionService {
    private final ImageFetcher fetcher;
    private final ObjectStorageClient s3Store;
    private final String panosPrefix;
    private final String croppedPrefix;

    public ImageAcquisitionService(ImageFetcher fetcher, ObjectStorageClient s3Store,
                                   String panosPrefix, String croppedPrefix) {
        this.fetcher = fetcher;
        this.s3Store = s3Store;
        this.panosPrefix = panosPrefix;
        this.croppedPrefix = croppedPrefix;
    }

    /**
     * Fetches a full panorama from the camera source.
     *
     * @param cameraPath camera-specific path (e.g. Roundshot "YYYY-MM-DD/HH-mm-00")
     * @return PanoResult containing the panorama image and status
     */
    public PanoResult fetchAndStitchPano(String cameraPath) throws IOException, InterruptedException {
        BufferedImage pano = fetcher.fetchImage(cameraPath);

        if (pano == null) {
            return new PanoResult(null, AcquisitionStatus.IMAGES_NOT_FOUND);
        }

        return new PanoResult(pano, AcquisitionStatus.OK);
    }

    /**
     * Creates a cropped image from the panorama.
     *
     * @param pano The full panorama image
     * @param cropBox Crop coordinates
     * @return Cropped image
     */
    public BufferedImage createCrop(BufferedImage pano, CropBox cropBox) throws IOException {
        CropBox box = ScraperUtils.parseCropBox(cropBox, pano.getWidth(), pano.getHeight());

        int cropW = box.width();
        int cropH = box.height();
        BufferedImage cropped = new BufferedImage(cropW, cropH, BufferedImage.TYPE_INT_RGB);

        // Copy the cropped region
        cropped.createGraphics().drawImage(pano, -box.x1(), -box.y1(), null);

        return cropped;
    }

    /**
     * Uploads panorama and cropped images to S3.
     *
     * @param pano The full panorama image
     * @param cropped The cropped image
     * @param keyBase The S3 key base in format yyyy/MM/dd/HHmm
     * @return AcquisitionResult with S3 keys and status
     */
    public AcquisitionResult uploadImages(BufferedImage pano, BufferedImage cropped, String keyBase)
            throws IOException {

        String panoKey = panosPrefix + "/" + keyBase + ".jpg";
        String croppedKey = croppedPrefix + "/" + keyBase + ".jpg";

        s3Store.putImage(panoKey, pano);
        s3Store.putImage(croppedKey, cropped);

        return new AcquisitionResult(panoKey, croppedKey, AcquisitionStatus.OK);
    }
}

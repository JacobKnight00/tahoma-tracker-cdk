package com.tahomatracker.service.process;

import com.tahomatracker.service.domain.CropBox;
import com.tahomatracker.service.domain.AcquisitionResult;
import com.tahomatracker.service.domain.PanoResult;
import com.tahomatracker.service.enums.AcquisitionStatus;
import com.tahomatracker.service.external.ObjectStorageClient;
import com.tahomatracker.service.external.SliceFetcher;
import com.tahomatracker.service.util.ScraperUtils;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.List;

/**
 * Service for acquiring webcam images: fetch slices, stitch, crop, watermark, and upload.
 */
public class ImageAcquisitionService {
    private final SliceFetcher fetcher;
    private final ObjectStorageClient s3Store;
    private final String panosPrefix;
    private final String croppedPrefix;

    public ImageAcquisitionService(SliceFetcher fetcher, ObjectStorageClient s3Store,
                                   String panosPrefix, String croppedPrefix) {
        this.fetcher = fetcher;
        this.s3Store = s3Store;
        this.panosPrefix = panosPrefix;
        this.croppedPrefix = croppedPrefix;
    }

    /**
     * Fetches camera slices and stitches them into a panorama.
     *
     * @param folder The folder path in format yyyy/MM/dd/yyyy_MMdd_HHmmss
     * @return PanoResult containing the stitched panorama and status
     */
    public PanoResult fetchAndStitchPano(String folder) throws IOException, InterruptedException {
        List<BufferedImage> slices = fetcher.fetchSlices(folder);

        if (slices.isEmpty()) {
            return new PanoResult(null, AcquisitionStatus.IMAGES_NOT_FOUND);
        }

        BufferedImage pano = ScraperUtils.stitchHorizontal(slices);
        return new PanoResult(pano, AcquisitionStatus.OK);
    }

    /**
     * Creates a cropped and watermarked image from the panorama.
     *
     * @param pano The full panorama image
     * @param cropBox Crop coordinates
     * @return Cropped and watermarked image
     */
    public BufferedImage createCrop(BufferedImage pano, CropBox cropBox) throws IOException {
        CropBox box = ScraperUtils.parseCropBox(cropBox, pano.getWidth(), pano.getHeight());

        int cropW = box.width();
        int cropH = box.height();
        BufferedImage cropped = new BufferedImage(cropW, cropH, BufferedImage.TYPE_INT_RGB);

        // Copy the cropped region
        cropped.createGraphics().drawImage(pano, -box.x1(), -box.y1(), null);

        // Watermark intentionally disabled; re-enable if branding is required again.
        // ScraperUtils.applyWatermark(cropped);

        return cropped;
    }

    /**
     * Uploads panorama and cropped images to S3.
     *
     * @param pano The full panorama image
     * @param cropped The cropped and watermarked image
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

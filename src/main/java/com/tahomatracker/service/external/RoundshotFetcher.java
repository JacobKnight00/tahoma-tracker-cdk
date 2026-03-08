package com.tahomatracker.service.external;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Fetches full-resolution panorama images from the Roundshot Livecam API.
 *
 * <p>The Space Needle camera produces a single 15634×2048 JPEG per 10-minute
 * interval stored at a predictable URL. No slice stitching is required.
 *
 * <p>URL format:
 * <pre>
 *   {baseUrl}/{YYYY-MM-DD}/{HH-mm-00}/{YYYY-MM-DD-HH-mm-00}_full.jpg
 * </pre>
 *
 * Example:
 * <pre>
 *   https://storage.roundshot.com/544a1a9d451563.40343637/
 *       2026-03-01/09-10-00/2026-03-01-09-10-00_full.jpg
 * </pre>
 *
 * <p>The {@code cameraPath} parameter accepted by {@link #fetchImage} must be in
 * the format produced by {@link com.tahomatracker.service.scraper.TimeWindowPlanner#folderPath},
 * i.e. {@code "YYYY-MM-DD/HH-mm-00"}.
 */
public class RoundshotFetcher implements ImageFetcher {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(RoundshotFetcher.class);
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(60);

    private final HttpClient httpClient;
    private final String cameraBaseUrl;

    public RoundshotFetcher(String cameraBaseUrl) {
        this.cameraBaseUrl = cameraBaseUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(HTTP_TIMEOUT)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    /**
     * Builds the full Roundshot URL from a camera path.
     *
     * @param cameraPath path in format "YYYY-MM-DD/HH-mm-00"
     * @return full HTTPS URL for the image
     */
    String buildUrl(String cameraPath) {
        // "YYYY-MM-DD/HH-mm-00" → filename "YYYY-MM-DD-HH-mm-00_full.jpg"
        String filename = cameraPath.replace("/", "-") + "_full.jpg";
        return cameraBaseUrl + "/" + cameraPath + "/" + filename;
    }

    @Override
    public BufferedImage fetchImage(String cameraPath) throws IOException, InterruptedException {
        String url = buildUrl(cameraPath);
        log.debug("Fetching Roundshot image: {}", url);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(HTTP_TIMEOUT)
                .header("User-Agent", "TahomaTracker-scraper/0.1")
                .GET()
                .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        int status = response.statusCode();

        if (status == 404 || status == 403) {
            log.debug("Roundshot image not found for path {} (HTTP {})", cameraPath, status);
            return null;
        }
        if (status == 429 || status >= 500) {
            throw new RetryableException(status, url);
        }
        if (status < 200 || status >= 300) {
            throw new IOException("Unexpected HTTP " + status + " for " + url);
        }

        BufferedImage img = ImageIO.read(new ByteArrayInputStream(response.body()));
        if (img == null) {
            throw new IOException("Could not decode JPEG from Roundshot for path: " + cameraPath);
        }
        log.debug("Fetched Roundshot image for {}: {}x{}", cameraPath, img.getWidth(), img.getHeight());
        return img;
    }

    /**
     * Thrown on 429 or 5xx responses. The caller should stop and retry later.
     */
    public static class RetryableException extends IOException {
        private final int statusCode;

        public RetryableException(int statusCode, String url) {
            super("HTTP " + statusCode + " (retryable) for " + url);
            this.statusCode = statusCode;
        }

        public int getStatusCode() { return statusCode; }
    }
}

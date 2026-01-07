package com.tahomatracker.backfill;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;

public class FastSliceFetcher implements com.tahomatracker.service.external.SliceFetcher {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FastSliceFetcher.class);
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);
    private final HttpClient httpClient;
    private final String cameraBaseUrl;
    private final ExecutorService executor;
    private final int defaultBatchSize;

    public FastSliceFetcher(String cameraBaseUrl, int threadPoolSize) {
        this(cameraBaseUrl, threadPoolSize, 32);
    }

    public FastSliceFetcher(String cameraBaseUrl, int threadPoolSize, int batchSize) {
        this.executor = Executors.newFixedThreadPool(Math.max(2, threadPoolSize));
        this.httpClient = HttpClient.newBuilder()
                .executor(executor)
                .connectTimeout(HTTP_TIMEOUT)
                .build();
        this.cameraBaseUrl = cameraBaseUrl;
        this.defaultBatchSize = Math.max(1, batchSize);
    }


    /**
     * Fetch slices for a folder using chunked concurrent requests.
     * Keeps the original "missing streak" semantics while fetching each chunk in parallel.
     */
    @Override
    public List<BufferedImage> fetchSlices(String folder) throws IOException, InterruptedException {
        return fetchSlices(folder, defaultBatchSize);
    }

    public List<BufferedImage> fetchSlices(String folder, int batchSize) throws IOException, InterruptedException {
        log.debug("Fetching slices for folder {} with batchSize={}", folder, batchSize);
        List<BufferedImage> slices = new ArrayList<>();
        int missingStreak = 0;
        for (int base = 0; base <= 500; base += batchSize) {
            int end = Math.min(500, base + batchSize - 1);
            List<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>();
            for (int idx = base; idx <= end; idx++) {
                String url = cameraBaseUrl + "/" + folder + "/slice" + idx + ".jpg";
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(HTTP_TIMEOUT)
                        .header("User-Agent", "TahomaTracker-backfill/0.1")
                        .GET()
                        .build();
                futures.add(httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()));
            }

            // wait for the whole chunk
            List<HttpResponse<byte[]>> responses = futures.stream()
                    .map(CompletableFuture::join)
                    .toList();

            // process in-order to preserve missing-streak logic
            for (int i = 0; i < responses.size(); i++) {
                int idx = base + i;
                HttpResponse<byte[]> resp = responses.get(i);
                if (resp == null) {
                    missingStreak++;
                    if (missingStreak >= 3) {
                        log.debug("Missing streak >=3, stopping at index {}", idx);
                        return slices;
                    }
                    continue;
                }
                int status = resp.statusCode();
                if (status == 403 || status == 404) {
                    if (idx == 0) {
                        log.debug("Probe returned {} for first slice, returning empty", status);
                        return List.of();
                    }
                    missingStreak++;
                    if (missingStreak >= 3) {
                        log.debug("Missing streak >=3 after status {} at idx {}, stopping", status, idx);
                        return slices;
                    }
                } else if (status >= 200 && status < 300) {
                    missingStreak = 0;
                    BufferedImage img = ImageIO.read(new ByteArrayInputStream(resp.body()));
                    if (img != null) {
                        slices.add(img);
                    }
                } else {
                    log.warn("Unexpected status {} for {} slice {}", status, folder, idx);
                    throw new IOException("Unexpected status " + status + " for " + folder + " slice " + idx);
                }
            }
        }
        return slices;
    }
}

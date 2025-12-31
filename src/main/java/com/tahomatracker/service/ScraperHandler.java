package com.tahomatracker.service;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.imageio.ImageIO;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class ScraperHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter FOLDER_FORMAT =
            DateTimeFormatter.ofPattern("yyyy/MM/dd/yyyy_MMdd_HHmmss");
    private static final DateTimeFormatter KEY_FORMAT =
            DateTimeFormatter.ofPattern("yyyy/MM/dd/HHmm");
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(HTTP_TIMEOUT)
            .build();
    private final S3Client s3Client = S3Client.builder().build();

    private final String bucketName = env("BUCKET_NAME", "");
    private final String cameraBaseUrl = env("CAMERA_BASE_URL", "https://d3omclagh7m7mg.cloudfront.net/assets");
    private final String panosPrefix = env("PANOS_PREFIX", "needle-cam/panos");
    private final String croppedPrefix = env("CROPPED_PREFIX", "needle-cam/cropped-images");
    private final String analysisPrefix = env("ANALYSIS_PREFIX", "analysis");
    private final String latestKey = env("LATEST_KEY", "latest/latest.json");
    private final String cropBox = env("CROP_BOX", "");
    private final double outThreshold = Double.parseDouble(env("OUT_THRESHOLD", "0.85"));
    private final String modelVersion = env("MODEL_VERSION", "v1");
    private final ZoneId localTz = ZoneId.of(env("LOCAL_TZ", "America/Los_Angeles"));
    private final int windowStartHour = Integer.parseInt(env("WINDOW_START_HOUR", "4"));
    private final int windowEndHour = Integer.parseInt(env("WINDOW_END_HOUR", "23"));
    private final int stepMinutes = Integer.parseInt(env("STEP_MINUTES", "10"));

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        ZonedDateTime tsUtc = parseTs(event);
        try {
            return runPipeline(tsUtc, true);
        } catch (Exception ex) {
            context.getLogger().log("Pipeline failed: " + ex.getMessage());
            return buildError(tsUtc.toInstant(), ex.getMessage());
        }
    }

    private Map<String, Object> runPipeline(ZonedDateTime tsUtc, boolean publishLatest) throws IOException, InterruptedException {
        ZonedDateTime baseLocal = floorToStepLocal(tsUtc);
        baseLocal = clampToWindow(baseLocal);
        ZonedDateTime previousLocal = baseLocal.minusMinutes(stepMinutes);

        Map<String, Object> latest = null;
        if (withinWindow(previousLocal) && !analysisExists(previousLocal)) {
            latest = processBucket(previousLocal);
        }

        Map<String, Object> current = processBucket(baseLocal);
        if (current != null) {
            latest = current;
        }

        if (latest == null) {
            throw new RuntimeException("No slices fetched for current or previous 10-minute buckets");
        }

        if (publishLatest) {
            putJson(latestKey, latest);
        }

        return latest;
    }

    private Map<String, Object> processBucket(ZonedDateTime tsLocal) throws IOException, InterruptedException {
        String folder = FOLDER_FORMAT.format(tsLocal);
        String probeUrl = sliceUrl(folder, 0);
        HttpResponse<Void> head = httpHead(probeUrl);
        if (head.statusCode() == 403 || head.statusCode() == 404) {
            return null;
        }

        List<BufferedImage> slices = fetchSlices(folder);
        if (slices.isEmpty()) {
            return null;
        }

        BufferedImage pano = stitchHorizontal(slices);
        int[] crop = parseCropBox(cropBox, pano.getWidth(), pano.getHeight());
        BufferedImage cropped = pano.getSubimage(crop[0], crop[1], crop[2] - crop[0], crop[3] - crop[1]);

        ZonedDateTime tsUtc = tsLocal.withZoneSameInstant(ZoneOffset.UTC);
        String panoKey = formatKey(panosPrefix, tsLocal, "jpg");
        String croppedKey = formatKey(croppedPrefix, tsLocal, "jpg");
        String analysisKey = formatKey(analysisPrefix, tsLocal, "json");

        putImage(panoKey, pano);
        putImage(croppedKey, cropped);

        String updatedAt = isoformat(Instant.now());

        Map<String, Object> analysis = new HashMap<>();
        analysis.put("ts", isoformat(tsUtc.toInstant()));
        analysis.put("status", "unknown");
        analysis.put("frame_state", null);
        analysis.put("frame_state_prob", null);
        analysis.put("visibility", null);
        analysis.put("visibility_prob", null);
        analysis.put("out", null);
        analysis.put("threshold", outThreshold);
        analysis.put("model_version", modelVersion);
        analysis.put("slice_count", slices.size());
        analysis.put("pano_size", List.of(pano.getWidth(), pano.getHeight()));
        analysis.put("cropped_size", List.of(cropped.getWidth(), cropped.getHeight()));
        analysis.put("cropped_s3_key", croppedKey);
        analysis.put("pano_s3_key", panoKey);
        analysis.put("error", null);
        analysis.put("updated_at", updatedAt);

        putJson(analysisKey, analysis);

        Map<String, Object> latest = new HashMap<>();
        latest.put("ts", isoformat(tsUtc.toInstant()));
        latest.put("status", "unknown");
        latest.put("frame_state", null);
        latest.put("out", null);
        latest.put("out_prob", null);
        latest.put("threshold", outThreshold);
        latest.put("model_version", modelVersion);
        latest.put("cropped_s3_key", croppedKey);
        latest.put("analysis_s3_key", analysisKey);
        latest.put("updated_at", updatedAt);
        return latest;
    }

    private List<BufferedImage> fetchSlices(String folder) throws IOException, InterruptedException {
        List<BufferedImage> slices = new ArrayList<>();
        int missingStreak = 0;
        for (int idx = 0; idx <= 500; idx++) {
            String url = sliceUrl(folder, idx);
            HttpResponse<byte[]> resp = httpGet(url);
            if (resp.statusCode() == 403 || resp.statusCode() == 404) {
                if (idx == 0) {
                    return List.of();
                }
                missingStreak++;
                if (missingStreak >= 3) {
                    break;
                }
            } else if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                missingStreak = 0;
                BufferedImage img = ImageIO.read(new ByteArrayInputStream(resp.body()));
                if (img != null) {
                    slices.add(img);
                }
            } else {
                throw new IOException("Unexpected status " + resp.statusCode() + " for " + url);
            }
            sleepMillis(120);
        }
        return slices;
    }

    private BufferedImage stitchHorizontal(List<BufferedImage> slices) {
        int height = slices.stream().mapToInt(BufferedImage::getHeight).max().orElse(0);
        int width = slices.stream().mapToInt(BufferedImage::getWidth).sum();
        BufferedImage pano = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = pano.createGraphics();
        int x = 0;
        for (BufferedImage img : slices) {
            g.drawImage(img, x, 0, null);
            x += img.getWidth();
        }
        g.dispose();
        return pano;
    }

    private int[] parseCropBox(String cropBox, int width, int height) {
        if (cropBox == null || cropBox.isBlank()) {
            return new int[] {0, 0, width, height};
        }
        String[] parts = cropBox.split(",");
        if (parts.length != 4) {
            throw new IllegalArgumentException("CROP_BOX must be x1,y1,x2,y2");
        }
        int x1 = Math.max(0, Integer.parseInt(parts[0].trim()));
        int y1 = Math.max(0, Integer.parseInt(parts[1].trim()));
        int x2 = Math.min(width, Integer.parseInt(parts[2].trim()));
        int y2 = Math.min(height, Integer.parseInt(parts[3].trim()));
        if (x2 <= x1 || y2 <= y1) {
            throw new IllegalArgumentException("Invalid crop box after bounds check");
        }
        return new int[] {x1, y1, x2, y2};
    }

    private void putImage(String key, BufferedImage image) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (!ImageIO.write(image, "jpeg", out)) {
            throw new IOException("Failed to encode JPEG");
        }
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .contentType("image/jpeg")
                .build();
        s3Client.putObject(request, RequestBody.fromBytes(out.toByteArray()));
    }

    private void putJson(String key, Map<String, Object> payload) throws JsonProcessingException {
        String body = MAPPER.writeValueAsString(payload);
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .contentType("application/json")
                .build();
        s3Client.putObject(request, RequestBody.fromString(body));
    }

    private boolean analysisExists(ZonedDateTime tsLocal) {
        String key = formatKey(analysisPrefix, tsLocal, "json");
        try {
            s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build());
            return true;
        } catch (NoSuchKeyException ex) {
            return false;
        } catch (S3Exception ex) {
            if (ex.statusCode() == 404) {
                return false;
            }
            return false;
        }
    }

    private String formatKey(String prefix, ZonedDateTime tsLocal, String ext) {
        return prefix + "/" + KEY_FORMAT.format(tsLocal) + "." + ext;
    }

    private String sliceUrl(String folder, int idx) {
        return cameraBaseUrl + "/" + folder + "/slice" + idx + ".jpg";
    }

    private HttpResponse<byte[]> httpGet(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(HTTP_TIMEOUT)
                .header("User-Agent", "TahomaTracker/0.1 (personal project; gentle rate limits)")
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private HttpResponse<Void> httpHead(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(HTTP_TIMEOUT)
                .header("User-Agent", "TahomaTracker/0.1 (personal project; gentle rate limits)")
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.discarding());
    }

    private ZonedDateTime parseTs(Map<String, Object> event) {
        Object tsValue = event == null ? null : event.get("ts");
        if (tsValue instanceof String tsString && !tsString.isBlank()) {
            try {
                return OffsetDateTime.parse(tsString).toInstant().atZone(ZoneOffset.UTC);
            } catch (Exception ignored) {
            }
        }
        return Instant.now().atZone(ZoneOffset.UTC);
    }

    private ZonedDateTime floorToStepLocal(ZonedDateTime tsUtc) {
        ZonedDateTime local = tsUtc.withZoneSameInstant(localTz);
        int minute = (local.getMinute() / stepMinutes) * stepMinutes;
        return local.withMinute(minute).withSecond(0).withNano(0);
    }

    private ZonedDateTime clampToWindow(ZonedDateTime local) {
        if (local.getHour() < windowStartHour) {
            ZonedDateTime previousDay = local.minusDays(1);
            return previousDay.withHour(windowEndHour - 1)
                    .withMinute(60 - stepMinutes)
                    .withSecond(0)
                    .withNano(0);
        }
        if (local.getHour() >= windowEndHour) {
            return local.withHour(windowEndHour - 1)
                    .withMinute(60 - stepMinutes)
                    .withSecond(0)
                    .withNano(0);
        }
        return local;
    }

    private boolean withinWindow(ZonedDateTime local) {
        return local.getHour() >= windowStartHour && local.getHour() < windowEndHour;
    }

    private Map<String, Object> buildError(Instant tsUtc, String error) {
        Map<String, Object> response = new HashMap<>();
        response.put("ts", isoformat(tsUtc));
        response.put("status", "unknown");
        response.put("frame_state", null);
        response.put("out", null);
        response.put("out_prob", null);
        response.put("threshold", outThreshold);
        response.put("model_version", modelVersion);
        response.put("cropped_s3_key", null);
        response.put("analysis_s3_key", null);
        response.put("updated_at", isoformat(Instant.now()));
        response.put("error", error);
        return response;
    }

    private String isoformat(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC)
                .withNano(0)
                .toString()
                .replace("+00:00", "Z");
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private void sleepMillis(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}

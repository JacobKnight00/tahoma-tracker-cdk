package com.tahomatracker.service.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tahomatracker.service.domain.CropBox;
import java.awt.AlphaComposite;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import javax.imageio.ImageIO;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public final class ScraperUtils {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WATERMARK_SVG = "/space-needle-logo.svg";
    private static final String WATERMARK_TEXT = "Image courtesy of: SpaceNeedle.com/webcam";
    private static final int WATERMARK_MARGIN_PX = 25;
    private static final int WATERMARK_WIDTH_PX = 210;
    private static final int WATERMARK_TEXT_FONT_SIZE = 10;
    private static BufferedImage watermarkImage;

    private ScraperUtils() {
    }

    public static BufferedImage stitchHorizontal(List<BufferedImage> slices) {
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

    public static CropBox parseCropBox(CropBox cropBox, int width, int height) {
        CropBox parsed = cropBox != null ? cropBox : CropBox.fullImage(width, height);
        return parsed.clampToImage(width, height);
    }

    public static void applyWatermark(BufferedImage image) throws IOException {
        if (image == null) return;

        BufferedImage logo = loadWatermark(WATERMARK_WIDTH_PX);
        if (logo == null) return;

        Graphics2D g = image.createGraphics();
        try {
            g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
            g.setComposite(AlphaComposite.SrcOver);

            Font font = new Font("SansSerif", Font.PLAIN, WATERMARK_TEXT_FONT_SIZE);
            g.setFont(font);
            FontMetrics metrics = g.getFontMetrics();

            WatermarkLayout layout = calculateLayout(image, logo, metrics);
            drawLogo(g, logo, layout);
            drawCaption(g, metrics, layout);
        } finally {
            g.dispose();
        }
    }

    private static WatermarkLayout calculateLayout(BufferedImage image, BufferedImage logo, FontMetrics metrics) {
        int logoX = Math.max(0, image.getWidth() - WATERMARK_MARGIN_PX - logo.getWidth());
        int logoY = Math.max(0, image.getHeight() - WATERMARK_MARGIN_PX - logo.getHeight());

        // Padding and gap scale modestly with logo size to stay proportional.
        int paddingX = Math.max(4, logo.getWidth() / 40);
        int paddingY = Math.max(2, logo.getHeight() / 80);
        int gapBelowLogo = Math.max(4, logo.getHeight() / 40);

        int textWidth = metrics.stringWidth(WATERMARK_TEXT);
        int textRectW = textWidth + paddingX * 2;
        int textRectH = metrics.getHeight() + paddingY * 2;

        int captionX = logoX + (logo.getWidth() - textRectW) / 2;
        captionX = clamp(captionX, 0, image.getWidth() - textRectW);

        int captionY = logoY + logo.getHeight() + gapBelowLogo;
        captionY = Math.min(image.getHeight() - textRectH, captionY);

        return new WatermarkLayout(logoX, logoY, captionX, captionY, textRectW, textRectH, paddingX, paddingY);
    }

    private static void drawLogo(Graphics2D g, BufferedImage logo, WatermarkLayout layout) {
        g.drawImage(logo, layout.logoX, layout.logoY, null);
    }

    private static void drawCaption(Graphics2D g, FontMetrics metrics, WatermarkLayout layout) {
        int textX = layout.captionX + layout.paddingX;
        int textY = layout.captionY + layout.paddingY + metrics.getAscent();

        g.setColor(new Color(0, 0, 0, 120));
        g.fillRect(layout.captionX, layout.captionY, layout.captionW, layout.captionH);
        g.setColor(new Color(255, 255, 255, 220));
        g.drawString(WATERMARK_TEXT, textX, textY);
    }

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    private static final class WatermarkLayout {
        final int logoX;
        final int logoY;
        final int captionX;
        final int captionY;
        final int captionW;
        final int captionH;
        final int paddingX;
        final int paddingY;

        WatermarkLayout(int logoX, int logoY, int captionX, int captionY, int captionW, int captionH,
                        int paddingX, int paddingY) {
            this.logoX = logoX;
            this.logoY = logoY;
            this.captionX = captionX;
            this.captionY = captionY;
            this.captionW = captionW;
            this.captionH = captionH;
            this.paddingX = paddingX;
            this.paddingY = paddingY;
        }
    }

    private static BufferedImage loadWatermark(int targetWidth) throws IOException {
        if (watermarkImage != null && watermarkImage.getWidth() == targetWidth) {
            return watermarkImage;
        }

        try (InputStream s = ScraperUtils.class.getResourceAsStream(WATERMARK_SVG)) {
            if (s == null) return null;
            watermarkImage = svgToBufferedImage(s, targetWidth);
            return watermarkImage;
        }
    }

    // Render an SVG input stream to a BufferedImage at the requested width using Batik PNGTranscoder
    static BufferedImage svgToBufferedImage(InputStream svgStream, int targetWidth) throws IOException {
        try {
            byte[] bytes = svgStream.readAllBytes();
            float targetHeight = targetWidth * extractAspectRatio(bytes);
            var baos = new java.io.ByteArrayOutputStream();
            var input = new org.apache.batik.transcoder.TranscoderInput(new java.io.ByteArrayInputStream(bytes));
            var output = new org.apache.batik.transcoder.TranscoderOutput(baos);
            var transcoder = new org.apache.batik.transcoder.image.PNGTranscoder();
            transcoder.addTranscodingHint(org.apache.batik.transcoder.image.PNGTranscoder.KEY_WIDTH, (float) targetWidth);
            transcoder.addTranscodingHint(org.apache.batik.transcoder.image.PNGTranscoder.KEY_HEIGHT, targetHeight);
            transcoder.transcode(input, output);
            baos.flush();
            try (var in = new java.io.ByteArrayInputStream(baos.toByteArray())) {
                return ImageIO.read(in);
            }
        } catch (org.apache.batik.transcoder.TranscoderException e) {
            throw new IOException(e);
        }
    }

    private static float extractAspectRatio(byte[] svgBytes) {
        try {
            String content = new String(svgBytes, StandardCharsets.UTF_8);
            int idx = content.indexOf("viewBox");
            if (idx == -1) return 1.0f;
            int quoteStart = content.indexOf('"', idx);
            int quoteEnd = content.indexOf('"', quoteStart + 1);
            if (quoteStart == -1 || quoteEnd == -1) return 1.0f;
            String[] parts = content.substring(quoteStart + 1, quoteEnd).trim().split("\\s+");
            if (parts.length != 4) return 1.0f;
            float w = Float.parseFloat(parts[2]);
            float h = Float.parseFloat(parts[3]);
            if (w <= 0 || h <= 0) return 1.0f;
            return h / w;
        } catch (Exception e) {
            return 1.0f;
        }
    }

    // Visible for tests: inject a watermark image (bypasses resource loading)
    static void setWatermarkImage(BufferedImage img) {
        watermarkImage = img;
    }

    public static void putImage(S3Client s3Client, String bucketName, String key, BufferedImage image) throws IOException {
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

    public static void putJson(S3Client s3Client, String bucketName, String key, Map<String, Object> payload) throws JsonProcessingException {
        String body = MAPPER.writeValueAsString(payload);
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .contentType("application/json")
                .build();
        s3Client.putObject(request, RequestBody.fromString(body));
    }
}

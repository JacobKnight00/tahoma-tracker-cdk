package com.tahomatracker.service.external;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.List;

public interface SliceFetcher {
    List<BufferedImage> fetchSlices(String folder) throws IOException, InterruptedException;
} 

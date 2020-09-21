package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PersistableSlidingWindow extends SlidingWindow<SlidingWindowData> implements Closeable {
  private static final Logger LOG = LogManager.getLogger(PersistableSlidingWindow.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  protected static final String SEPARATOR = "\n";

  protected final long writePeriod;

  private Path path;
  private BufferedWriter writer;
  private long timestamp = -1;

  public PersistableSlidingWindow(int slidingWindowSize,
                                  long writePeriod,
                                  TimeUnit timeUnit,
                                  String filePath) {
    super(slidingWindowSize, timeUnit);
    this.writePeriod = writePeriod;
    try {
      this.path = Paths.get(filePath);
      if (Files.exists(path)) {
        loadFromFile(path);
      } else {
        Files.createFile(path);
      }
      this.writer = new BufferedWriter(new FileWriter(filePath, false));
    } catch (IOException e) {
      LOG.error("Couldn't create file {} to perform young generation tuning", path, e);
      throw new IllegalArgumentException("Couldn't create or read a file at " + filePath);
    }
  }

  @Override
  public void next(SlidingWindowData slidingWindowData) {
    if (timestamp == -1) {
      timestamp = slidingWindowData.getTimeStamp();
    }
    long currTimestamp = slidingWindowData.getTimeStamp();
    super.next(slidingWindowData);
    long timestampDiff = currTimestamp - timestamp;
    if (timeUnit.convert(timestampDiff, TimeUnit.MILLISECONDS)
        > timeUnit.convert(writePeriod, TimeUnit.MILLISECONDS)) {
      try {
        persist();
      } catch (IOException e) {
        LOG.error("Exception persisting sliding window data", e);
      }
    }
  }

  public void loadFromFile(Path path) throws IOException {
    LineIterator it = FileUtils.lineIterator(path.toFile(), "UTF-8");
    try {
      while (it.hasNext()) {
        String line = it.nextLine();
        SlidingWindowData data = objectMapper.readValue(line, SlidingWindowData.class);
        next(data);
      }
    } finally {
      LineIterator.closeQuietly(it);
    }
  }

  public void persist() throws IOException {
    for (SlidingWindowData data : windowDeque) {
      writer.write(objectMapper.writeValueAsString(data));
      writer.write(SEPARATOR);
    }
    writer.flush();
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }
}

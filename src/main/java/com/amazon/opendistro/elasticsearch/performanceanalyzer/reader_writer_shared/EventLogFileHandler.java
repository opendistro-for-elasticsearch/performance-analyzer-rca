package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.EventDispatcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.EventProcessor;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class EventLogFileHandler {
    private static final Logger LOG = LogManager.getLogger(EventLogFileHandler.class);

    private final EventLog eventLog;
    private final String metricsLocation;
    private static final int BUFFER_SIZE = 8192;
    private static final String TMP_FILE_EXT = ".tmp";
    private long lastProcessed;

    public EventLogFileHandler(EventLog eventLog, String metricsLocation) {
        this.eventLog = eventLog;
        this.metricsLocation = metricsLocation;
    }

    /**
     * This method writes all the metrics corresponding to an epoch to file.
     *
     * The regular case is, create a temporary file with the same path as the actual file but with an extension .tmp.
     * After the .tmp is successfully written, atomically rename it to remove the .tmp.
     *
     * However there are a few corner cases. It can happen that during two successive purges of the Blocking Queue of
     * the Plugin, a few metrics corresponding to an epoch were found. In this case the original file will already
     * exist and an atomic move in the end will overwrite its data. So we copy the file over if it exists, to the .tmp,
     * then we append the .tmp and we finally rename it. This time replacing it is not harmful as the new file has
     * the complete data.
     *
     * If any of the above steps fail, then the tmp file is not deleted from the filesystem. This is fine as the
     * MetricsPurgeActivity, will eventually clean it. The copies are atomic and therefore the reader never reads
     * incompletely written file.
     *
     * @param dataEntries The metrics to be written to file.
     * @param epoch The epoch all the metrics belong to.
     */
    public void writeTmpFile(List<Event> dataEntries, long epoch) {

        Path path = Paths.get(metricsLocation, String.valueOf(epoch));
        Path tmpPath = Paths.get(path.toString() + TMP_FILE_EXT);

        Event currEntry = null;
        try (OutputStream out = Files.newOutputStream(tmpPath,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            for (Event event: dataEntries) {
                currEntry = event;
                byte[] data = eventLog.write(event);
                writeInternal(out, data);
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Error writing entry '{}'. Cause: {}",
                    currEntry==null ? "NOT_INITIALIZED" : currEntry.key, e.getMessage());
        }
    }

    public void renameFromTmp(long epoch) {
        Path path = Paths.get(metricsLocation, String.valueOf(epoch));
        Path tmpPath = Paths.get(path.toString() + TMP_FILE_EXT);
        // This is done only when no exception is thrown.
        try {
            Files.move(tmpPath, path, REPLACE_EXISTING, ATOMIC_MOVE);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Error moving file {} to {}.", tmpPath.toString(), path.toString());
        }
    }

    public void read(long timestamp, EventDispatcher processor) {
        if (timestamp <= lastProcessed) {
            return;
        }

        String filename = String.valueOf(timestamp);
        Path pathToFile = Paths.get(metricsLocation, filename);
        File tempFile = new File(pathToFile.toString());
        if (!tempFile.exists()) {
            long mCurrT = System.currentTimeMillis();
            LOG.info("Didnt find {} at {}", filename, mCurrT);
            return;
        }
        readInternal(pathToFile, BUFFER_SIZE, processor);
        lastProcessed = timestamp;
        //LOG.info("PARSED - {} {}", filename, ret);
        eventLog.clear();
    }


    private void writeInternal(OutputStream stream, byte[] data) throws IOException {
        int len = data.length;
        int rem = len;
        while (rem > 0) {
            int n = Math.min(rem, BUFFER_SIZE);
            stream.write(data, (len-rem), n);
            rem -= n;
        }
    }

    private void readInternal(Path pathToFile, int bufferSize,
            EventDispatcher processor) {
        try (SeekableByteChannel channel = Files.newByteChannel(pathToFile, StandardOpenOption.READ)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);

            while (channel.read(byteBuffer) > 0) {
                ((Buffer)byteBuffer).flip();
                //LOG.info(" MAP {}", byteBuffer);
                eventLog.read(byteBuffer, processor);
                ((Buffer)byteBuffer).clear();
                //TODO: Handle edge case where buffer is too small.
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}

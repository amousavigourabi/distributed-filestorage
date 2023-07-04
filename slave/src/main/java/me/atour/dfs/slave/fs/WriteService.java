package me.atour.dfs.slave.fs;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Write service, manages block writes.
 */
@Slf4j
public class WriteService {

  private static final int STANDARD_BLOCK_SIZE = 4096;
  private static final String BASE_PATH = "blocks" + File.separator;

  private final Queue<ScheduledWrite> scheduledWriteQueue;

  @Getter
  private final Map<String, FileLocation> tagLocations;

  /**
   * Constructs a write queue.
   *
   * @param locations {@link Map} of the locations of the tags
   */
  public WriteService(Map<String, FileLocation> locations) {
    scheduledWriteQueue = new ConcurrentLinkedDeque<>();
    tagLocations = locations;
  }

  /**
   * Adds content to the write queue.
   *
   * @param handle the handle of the body to add to the queue
   * @param body the body of content to add to the queue
   */
  public synchronized void add(String handle, String body) {
    scheduledWriteQueue.add(new ScheduledWrite(handle, body));
    if (scheduledWriteQueue.size() > STANDARD_BLOCK_SIZE) {
      tagLocations.putAll(writeBlock(STANDARD_BLOCK_SIZE));
    }
  }

  /**
   * Writes a block from the write queue.
   *
   * @param blockSizeHint block size hint
   * @return {@link Map} of file locations in the block
   */
  public synchronized Map<String, FileLocation> writeBlock(int blockSizeHint) {
    int blockSize = Math.min(blockSizeHint, scheduledWriteQueue.size());
    Map<String, FileLocation> addresses = new HashMap<>(blockSize);
    String fileName = getRandomFilename();
    try (OutputStream out = new BufferedOutputStream(new FileOutputStream(fileName), blockSize)) {
      for (int i = 0; i < blockSize; i++) {
        ScheduledWrite write = scheduledWriteQueue.remove();
        String formattedMessage = String.format("%-65536s", write.getBody());
        out.write(formattedMessage.getBytes());
        addresses.put(write.getHandle(), new FileLocation(fileName, i));
      }
    } catch (IOException e) {
      log.error("Could not write to block of size {}.", blockSize);
    }
    return addresses;
  }

  /**
   * Flushes the buffer.
   */
  public void flush() {
    tagLocations.putAll(writeBlock(STANDARD_BLOCK_SIZE));
  }

  /**
   * Generates a pseudo-random filename to avoid collisions.
   *
   * @return a pseudo-random filename, should be good enough to avoid collisions
   */
  public String getRandomFilename() {
    return BASE_PATH + UUID.randomUUID() + ".block";
  }
}

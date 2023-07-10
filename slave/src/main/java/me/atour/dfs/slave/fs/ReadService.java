package me.atour.dfs.slave.fs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReadService {

  private final Map<String, FileLocation> tagLocations;

  public ReadService(Map<String, FileLocation> tags) {
    tagLocations = tags;
  }

  /**
   * Read a file into memory from its tag.
   *
   * @param tag the tag of the file to read
   * @return the file contents
   */
  public byte[] read(String tag) {
    if (!tagLocations.containsKey(tag)) {
      throw new RuntimeException("womp womp");
    }
    FileLocation fileLocation = tagLocations.get(tag);
    byte[] buffer = new byte[65536];
    try (RandomAccessFile file = new RandomAccessFile(fileLocation.getFileName(), "r")) {
      file.readFully(buffer, fileLocation.getOffset(), 65536);
    } catch (IOException e) {
      log.error("Could not read file with tag {} because {}.", tag, e.getMessage());
      throw new CouldNotOpenFileException();
    }
    return buffer;
  }
}

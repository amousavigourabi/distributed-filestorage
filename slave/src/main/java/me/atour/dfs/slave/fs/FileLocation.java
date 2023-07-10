package me.atour.dfs.slave.fs;

import lombok.Data;

@Data
public class FileLocation {
  private final String fileName;
  private final int offset;
}

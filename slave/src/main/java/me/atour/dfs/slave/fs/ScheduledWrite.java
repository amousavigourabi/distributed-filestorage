package me.atour.dfs.slave.fs;

import lombok.Data;

@Data
public class ScheduledWrite {
  private final String handle;
  private final String body;
}

package me.atour.dfs.master.fs;

import java.net.InetSocketAddress;
import lombok.Data;

@Data
public class DistributedLocation {
  private final InetSocketAddress slave;
  private final String tag;

  public String toString() {
    return tag + '@' + slave.getHostString() + ':' + slave.getPort();
  }
}

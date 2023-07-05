package me.atour.dfs.master.net;

import java.net.SocketException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import me.atour.dfs.master.fs.DistributedLocation;

/**
 * Master server container.
 */
public class MasterServer {

  private final MasterSlaveService masterSlaveService;
  private final MasterClientService masterClientService;

  /**
   * Sets up the master server.
   *
   * @param clientPort port to communicate with clients
   * @param slavePort port to communicate with slaves
   * @param heartbeatPort port to receive slave heartbeats
   */
  public MasterServer(int clientPort, int slavePort, int heartbeatPort) throws SocketException {
    Map<String, DistributedLocation> locations = new ConcurrentHashMap<>();
    masterSlaveService = new MasterSlaveService(slavePort, heartbeatPort, locations);
    masterClientService = new MasterClientService(clientPort, masterSlaveService, locations);
  }

  /**
   * Shuts down the server.
   */
  public void shutdown() {
    masterSlaveService.shutdown();
    masterClientService.shutdown();
  }
}

package me.atour.dfs.slave.net;

import java.net.InetAddress;
import java.net.SocketException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Slave server container.
 */
@Slf4j
public class SlaveServer {

  private final SlaveMasterService masterService;
  private final SlaveClientService clientService;

  /**
   * Sets up the slave server.
   *
   * @param masterAddress address of the master
   * @param toMasterPort the master's port used to talk to the slaves
   * @param fromMasterPort port used to talk to the master
   * @param fromClientPort port used to talk to clients submitting data
   * @param fetchPort port used for data fetch requests
   * @throws SocketException when sockets cannot be opened
   */
  public SlaveServer(InetAddress masterAddress, int toMasterPort, int fromMasterPort, int fromClientPort,
                     int fetchPort) throws SocketException {
    Map<InetAddress, Long> reservations = new ConcurrentHashMap<>();
    masterService = new SlaveMasterService(masterAddress, fromMasterPort, toMasterPort, fromClientPort,
        reservations);
    clientService = new SlaveClientService(fromClientPort, fetchPort, reservations);
  }

  /**
   * Shuts down the server.
   */
  public void shutdown() {
    masterService.shutdown();
    clientService.shutdown();
  }
}

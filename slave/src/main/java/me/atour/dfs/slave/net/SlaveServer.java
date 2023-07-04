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
   * @param registrationPort port used to register slaves at the master
   * @param fetchPort port used for data fetch requests
   * @param memory the amount of memory available on the machine
   * @throws SocketException when sockets cannot be opened
   */
  public SlaveServer(InetAddress masterAddress, int toMasterPort, int fromMasterPort, int fromClientPort,
                     int registrationPort, int fetchPort,
                     long memory) throws SocketException {
    Map<InetAddress, Long> reservations = new ConcurrentHashMap<>();
    masterService = new SlaveMasterService(masterAddress, fromMasterPort, toMasterPort, fromClientPort,
        registrationPort, memory, reservations);
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

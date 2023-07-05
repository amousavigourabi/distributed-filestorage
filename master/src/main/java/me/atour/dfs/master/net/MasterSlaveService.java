package me.atour.dfs.master.net;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import me.atour.dfs.master.fs.DistributedLocation;
import me.atour.dfs.master.fs.PathAlreadyExistsException;

/**
 * Service responsible for master-slave communication.
 */
@Slf4j
public class MasterSlaveService {

  private final DatagramSocket slaveSocket;
  private final Map<String, DistributedLocation> fileLocations;
  private final List<InetSocketAddress> slaves;
  private final Set<InetSocketAddress> recentHeartbeats;

  private final Thread registrationThread;

  private final ThreadLocalRandom generator;

  private final ScheduledExecutorService slaveCleanupExecutorService = Executors.newSingleThreadScheduledExecutor();

  /**
   * Constructs the master-slave service.
   * Used for communicating with the slave from the master.
   *
   * @param slaveFacingPort the port on the master with which the slaves communicate
   * @param locations a {@link ConcurrentMap} maintaining the files that are stored in the system
   */
  public MasterSlaveService(int slaveFacingPort, ConcurrentMap<String, DistributedLocation> locations) throws SocketException {
    slaves = new ArrayList<>();
    generator = ThreadLocalRandom.current();
    recentHeartbeats = new HashSet<>();
    fileLocations = locations;
    slaveSocket = new DatagramSocket(slaveFacingPort);
    registrationThread = new Thread(this::registerSlaveListener);
    registrationThread.start();
    slaveCleanupExecutorService.scheduleAtFixedRate(this::cleanupSlaves, 5, 5, TimeUnit.MINUTES);
  }

  /**
   * Cleans up the slave registrations.
   * Removes registrations for which there have been no recent heartbeats.
   */
  public void cleanupSlaves() {
    slaves.retainAll(recentHeartbeats);
    recentHeartbeats.clear();
  }

  /**
   * Listens for slave registrations.
   */
  public void registerSlaveListener() {
    byte[] buf = new byte[65536];
    while (true) {
      try {
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        slaveSocket.receive(packet);
        InetSocketAddress sender = (InetSocketAddress) packet.getSocketAddress();
        int port = Integer.parseInt(new String(buf, 1, buf.length - 1));
        InetSocketAddress slaveRef = new InetSocketAddress(sender.getAddress(), port);
        if (buf[0] == 'r') {
          recentHeartbeats.add(slaveRef);
          slaves.add(slaveRef); // todo register memory size
          DatagramPacket response = new DatagramPacket(buf, buf.length, sender);
          slaveSocket.send(response);
        } else if (buf[0] == 'h') {
          recentHeartbeats.add(slaveRef);
        } else {
          log.info("Could not recognize message {}.", new String(buf));
        }
      } catch (IOException e) {
        log.error("Could not deal with a slave registration request because {}.", e.getMessage());
      }
    }
  }

  /**
   * Allocates a location for the file.
   *
   * @param path the path the file has in the file system
   * @return the allocated location
   */
  public DistributedLocation submit(String path) {
    if (fileLocations.containsKey(path)) {
      throw new PathAlreadyExistsException();
    }
    int randomIndex = generator.nextInt();
    InetSocketAddress address = slaves.get(randomIndex);
    String tag = UUID.randomUUID().toString();
    DistributedLocation location = new DistributedLocation(address, tag);
    fileLocations.put(path, location);
    return location;
  }

  /**
   * Shuts down the master-slave service.
   */
  public void shutdown() {
    registrationThread.interrupt();
    slaveSocket.close();
  }
}

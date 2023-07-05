package me.atour.dfs.master.net;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
  private final Set<InetSocketAddress> slaves;
  private final Map<InetSocketAddress, Date> heartbeats;

  private final Thread registrationThread;

  /**
   * Constructs the master-slave service.
   * Used for communicating with the slave from the master.
   *
   * @param slaveFacingPort the port on the master with which the slaves communicate
   * @param locations a {@link ConcurrentMap} maintaining the files that are stored in the system
   */
  public MasterSlaveService(int slaveFacingPort, ConcurrentMap<String, DistributedLocation> locations) throws SocketException {
    slaves = new HashSet<>();
    heartbeats = new ConcurrentHashMap<>();
    fileLocations = locations;
    slaveSocket = new DatagramSocket(slaveFacingPort);
    registrationThread = new Thread(this::registerSlaveListener);
    registrationThread.start();
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
        if (buf[0] == 'r') {
          slaves.add(sender); // todo register memory size
        } else if (buf[0] == 'h') {
          heartbeats.put(sender, new Date());
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
    Set<InetSocketAddress> tempSlaves = new HashSet<>(slaves);
    int randomIndex = new Random().nextInt(tempSlaves.size());
    Iterator<InetSocketAddress> iterator = tempSlaves.iterator();
    for (int i = 0; i < randomIndex; i++) {
      iterator.next();
    }
    InetSocketAddress address = iterator.next();
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

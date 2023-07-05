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
import lombok.extern.slf4j.Slf4j;
import me.atour.dfs.master.fs.DistributedLocation;
import me.atour.dfs.master.fs.PathAlreadyExistsException;

/**
 * Service responsible for master-slave communication.
 */
@Slf4j
public class MasterSlaveService {

  private final DatagramSocket registrationSocket;
  private final DatagramSocket heartbeatSocket;
  private final Map<String, DistributedLocation> fileLocations;
  private final Set<InetSocketAddress> slaves;
  private final Map<InetSocketAddress, Date> heartbeats;

  private final Thread registrationThread;
  private final Thread heartbeatThread;

  /**
   * Constructs the master-slave service.
   * Used for communicating with the slave from the master.
   *
   * @param registrationPort the port on the master the slaves register at
   * @param heartbeatPort the port on the master the slaves send heartbeats to
   */
  public MasterSlaveService(int registrationPort, int heartbeatPort, Map<String, DistributedLocation> locations) throws SocketException {
    slaves = new HashSet<>();
    heartbeats = new ConcurrentHashMap<>();
    fileLocations = locations;
    registrationSocket = new DatagramSocket(registrationPort);
    heartbeatSocket = new DatagramSocket(heartbeatPort);
    registrationThread = new Thread(this::registerSlaveListener);
    heartbeatThread = new Thread(this::heartbeatListener);
    registrationThread.start();
    heartbeatThread.start();
  }

  /**
   * Listens for slave registrations.
   */
  public void registerSlaveListener() {
    byte[] buf = new byte[65536];
    while (true) {
      try {
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        registrationSocket.receive(packet);
        InetSocketAddress sender = (InetSocketAddress) packet.getSocketAddress();
        slaves.add(sender);
      } catch (IOException e) {
        log.error("Could not deal with a slave registration request because {}.", e.getMessage());
      }
    }
  }

  /**
   * Listens for heartbeats.
   */
  public void heartbeatListener() {
    byte[] buf = new byte[65536];
    while (true) {
      try {
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        heartbeatSocket.receive(packet);
        InetSocketAddress sender = (InetSocketAddress) packet.getSocketAddress();
        heartbeats.put(sender, new Date());
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
    heartbeatThread.interrupt();
    registrationSocket.close();
    heartbeatSocket.close();
  }
}

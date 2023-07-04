package me.atour.dfs.slave.net;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Service responsible for slave-master communication.
 */
@Slf4j
public class SlaveMasterService {

  private final InetAddress master;
  private final int toMasterPort;
  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  private final DatagramSocket heartbeatSocket;
  private final Thread thread;
  private final Map<InetAddress, Long> clients;

  /**
   * Constructs the slave-master service.
   * Used for communicating with the master from the slave.
   *
   * @param masterAddress the address of the master
   * @param slavePort the port the slave uses for master communication
   * @param masterPort the port the master uses for slave communication
   * @param clientPort the port the slave uses for client communication
   * @param registrationPort the port the master uses for slave registration
   * @param memory the amount of memory available on this slave
   * @param reservations {@link Map} of the resources reserved/available for a given client
   * @throws SocketException when something goes wrong with the {@link DatagramSocket}s
   */
  public SlaveMasterService(InetAddress masterAddress, int slavePort, int masterPort, int clientPort,
                            int registrationPort, long memory,
                            Map<InetAddress, Long> reservations) throws SocketException {
    master = masterAddress;
    toMasterPort = masterPort;
    clients = reservations;
    heartbeatSocket = new DatagramSocket(slavePort);
    String slaveId = register(registrationPort, clientPort, memory);
    scheduledExecutorService.scheduleAtFixedRate(() -> heartbeat(slaveId), 0, 5, TimeUnit.SECONDS);
    thread = new Thread(this::listenForOrchestration);
    thread.start();
  }

  /**
   * Register the slave node
   *
   * @param registrationPort the port the master uses for registration requests
   * @param clientPort the port the slave uses for client communication
   * @param memory the memory available, in bytes
   * @return the slave id
   */
  public String register(int registrationPort, int clientPort, long memory) {
    try (DatagramSocket registrationSocket = new DatagramSocket(registrationPort)) {
      String message = memory + "\\" + clientPort;
      byte[] buf = message.getBytes();
      DatagramPacket packet = new DatagramPacket(buf, buf.length, master, toMasterPort);
      registrationSocket.send(packet);
      byte[] replyBuf = new byte[512];
      DatagramPacket reply = new DatagramPacket(replyBuf, replyBuf.length);
      registrationSocket.receive(reply);
      return new String(reply.getData(), reply.getOffset(), reply.getLength());
    } catch (IOException e) {
      log.error("Could not register slave with {} bytes of memory because of {}.", memory, e.getMessage());
      throw new CouldNotRegisterSlaveException();
    }
  }

  /**
   * Sends a heartbeat to the master
   *
   * @param slaveId the slave id
   */
  public void heartbeat(String slaveId) {
    try {
      byte[] buf = slaveId.getBytes();
      DatagramPacket packet = new DatagramPacket(buf, buf.length, master, toMasterPort);
      heartbeatSocket.send(packet);
    } catch (IOException e) {
      log.error("Could not send heartbeat to master at {}:{} because of {}.", master, toMasterPort, e.getMessage());
    }
  }

  /**
   * Listens for orchestration from the master
   */
  public void listenForOrchestration() {
    byte[] buf = new byte[512];
    while (true) {
      try {
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        heartbeatSocket.receive(packet);
        if (!master.equals(((InetSocketAddress) packet.getSocketAddress()).getAddress())) {
          log.debug("Received packet from unknown host {}.", packet.getAddress());
          continue;
        }
        String message = new String(packet.getData(), packet.getOffset(), packet.getLength());
        String[] splits = message.split(";");
        for (String split : splits) {
          InetAddress clientName = InetAddress.getByName(split);
          synchronized (clients) {
            long newAmount = clients.getOrDefault(clientName, 0L) + 1;
            clients.put(clientName, newAmount);
          }
        }
      } catch (IOException e) {
        log.error("Could not receive communications from the master because of {}.", e.getMessage());
      }
    }
  }

  /**
   * Shuts down the slave-master communications
   */
  public void shutdown() {
    thread.interrupt();
    scheduledExecutorService.shutdownNow();
    heartbeatSocket.close();
  }
}

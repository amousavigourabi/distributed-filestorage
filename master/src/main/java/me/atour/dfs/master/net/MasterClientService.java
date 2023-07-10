package me.atour.dfs.master.net;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import me.atour.dfs.master.fs.DistributedLocation;

@Slf4j
public class MasterClientService {

  private final DatagramSocket clientSocket;
  private final Map<String, DistributedLocation> fileLocations;
  private final MasterSlaveService masterSlaveService;

  private final Thread submissionThread;
  private final Thread fetchThread;

  public MasterClientService(int clientPort, MasterSlaveService slaveService,
                             Map<String, DistributedLocation> locations) throws SocketException {
    masterSlaveService = slaveService;
    fileLocations = locations;
    clientSocket = new DatagramSocket(clientPort);
    submissionThread = new Thread(this::listenForClientSubmits);
    fetchThread = new Thread(this::listenForClientFetches);
    submissionThread.start();
    fetchThread.start();
  }

  public void listenForClientSubmits() {
    byte[] buf = new byte[65536];
    while (true) {
      try {
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        clientSocket.receive(packet);
        String path = new String(packet.getData(), packet.getOffset(), packet.getLength());
        DistributedLocation allocatedLocation = masterSlaveService.submit(path);
        byte[] responseBuf = allocatedLocation.toString().getBytes();
        InetSocketAddress sender = (InetSocketAddress) packet.getSocketAddress();
        DatagramPacket response = new DatagramPacket(responseBuf, responseBuf.length, sender.getAddress(),
            sender.getPort());
        clientSocket.send(response);
      } catch (IOException e) {
        log.error("Could not deal with a client's submission request because of {}.", e.getMessage());
      }
    }
  }

  public void listenForClientFetches() {
    byte[] buf = new byte[65536];
    while (true) {
      try {
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        clientSocket.receive(packet);
        String path = new String(packet.getData(), packet.getOffset(), packet.getLength());
        DistributedLocation fetchLocation = fileLocations.get(path);
        byte[] responseBuf = fetchLocation.toString().getBytes();
        InetSocketAddress sender = (InetSocketAddress) packet.getSocketAddress();
        DatagramPacket response = new DatagramPacket(responseBuf, responseBuf.length, sender.getAddress(),
            sender.getPort());
        clientSocket.send(response);
      } catch (IOException e) {
        log.error("Could not deal with a client's submission request because of {}.", e.getMessage());
      }
    }
  }

  /**
   * Shuts down the master-client service.
   */
  public void shutdown() {
    submissionThread.interrupt();
    fetchThread.interrupt();
    clientSocket.close();
  }
}

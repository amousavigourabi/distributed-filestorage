package me.atour.dfs.slave.net;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import me.atour.dfs.slave.fs.FileLocation;
import me.atour.dfs.slave.fs.ReadService;
import me.atour.dfs.slave.fs.WriteService;

/**
 * Service to deal with slave-client communications.
 */
@Slf4j
public class SlaveClientService {

  private final DatagramSocket clientSocket;
  private final DatagramSocket fetchSocket;
  private final Thread thread;
  private final Thread fetchThread;
  private final Map<InetAddress, Long> clients;
  private final WriteService writeService;
  private final ReadService readService;

  /**
   * Constructs service responsible for slave-client communication.
   *
   * @param clientPort port for the client to submit files
   * @param fetchPort port for the client to fetch files
   * @param reservations bookkeeping of the reservations for the clients
   * @throws SocketException when sockets cannot be opened
   */
  public SlaveClientService(int clientPort, int fetchPort, Map<InetAddress, Long> reservations) throws SocketException {
    Map<String, FileLocation> tagLocations = new ConcurrentHashMap<>();
    writeService = new WriteService(tagLocations);
    readService = new ReadService(tagLocations);
    clientSocket = new DatagramSocket(clientPort);
    fetchSocket = new DatagramSocket(fetchPort);
    clients = reservations;
    thread = new Thread(this::listenForClients);
    thread.start();
    fetchThread = new Thread(this::fetchForClients);
    fetchThread.start();
  }

  /**
   * Listens for clients submitting files.
   */
  public void listenForClients() {
    byte[] buf = new byte[65536];
    while (true) {
      try {
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        clientSocket.receive(packet);
        InetAddress clientAddress = ((InetSocketAddress) packet.getSocketAddress()).getAddress();
        if (clients.containsKey(clientAddress)) {
          synchronized (clients) {
            long newValue = clients.get(clientAddress) - 1;
            clients.put(clientAddress, newValue);
          }
        }
        String message = new String(packet.getData(), packet.getOffset(), packet.getLength());
        String[] splits = message.split("\\\\");
        String handle = splits[0];
        String body = splits[1];
        writeService.add(handle, body);
      } catch (IOException e) {
        log.error("Could not receive communications from the master because of {}.", e.getMessage());
      }
    }
  }

  /**
   * Listens for clients fetching files.
   */
  public void fetchForClients() {
    byte[] buf = new byte[65536];
    while (true) {
      try {
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        fetchSocket.receive(packet);
        String handle = new String(packet.getData(), packet.getOffset(), packet.getLength());
        byte[] contents = readService.read(handle);
        InetSocketAddress clientAddress = (InetSocketAddress) packet.getSocketAddress();
        DatagramPacket response = new DatagramPacket(contents, contents.length, clientAddress.getAddress(),
            clientAddress.getPort());
        fetchSocket.send(response);
      } catch (IOException e) {
        log.error("Could not receive communications from the master because of {}.", e.getMessage());
      }
    }
  }

  /**
   * Shuts down the slave-client communication.
   */
  public void shutdown() {
    // todo: persist the tag map
    writeService.flush();
    thread.interrupt();
    fetchThread.interrupt();
    clientSocket.close();
  }
}

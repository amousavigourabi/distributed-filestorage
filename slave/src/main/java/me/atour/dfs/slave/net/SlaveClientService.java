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
  private final Thread thread;
  private final Map<InetAddress, Long> clients;
  private final WriteService writeService;
  private final ReadService readService;

  /**
   * Constructs service responsible for slave-client communication.
   *
   * @param clientPort port for the client to submit files
   * @param reservations bookkeeping of the reservations for the clients
   * @throws SocketException when sockets cannot be opened
   */
  public SlaveClientService(int clientPort, Map<InetAddress, Long> reservations) throws SocketException {
    Map<String, FileLocation> tagLocations = new ConcurrentHashMap<>();
    writeService = new WriteService(tagLocations);
    readService = new ReadService(tagLocations);
    clientSocket = new DatagramSocket(clientPort);
    clients = reservations;
    thread = new Thread(this::listenForClients);
    thread.start();
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
        char[] data = new String(packet.getData(), packet.getOffset(), packet.getLength()).toCharArray();
        InetSocketAddress clientSocketAddress = (InetSocketAddress) packet.getSocketAddress();
        InetAddress clientAddress = clientSocketAddress.getAddress();
        String message = new String(data, 1, data.length - 1);
        if (data[0] == 'f') {
          byte[] contents = readService.read(message);
          DatagramPacket response = new DatagramPacket(contents, contents.length, clientAddress,
              clientSocketAddress.getPort());
          clientSocket.send(response);
        } else if (data[0] == 's') {
          if (clients.containsKey(clientAddress) && clients.get(clientAddress) > 0) {
            synchronized (clients) {
              long newValue = clients.get(clientAddress) - 1;
              clients.put(clientAddress, newValue);
            }
          } else {
            log.info("Client {} tried to submit without reservation.", clientAddress);
          }
          String[] splits = message.split("\\\\");
          String handle = splits[0];
          String body = splits[1];
          writeService.add(handle, body);
        } else {
          log.info("Client {} tried to communicate using prefix {}.", clientAddress, data[0]);
        }
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
    clientSocket.close();
  }
}

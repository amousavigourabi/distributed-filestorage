package me.atour.dfs.slave;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import lombok.NonNull;
import me.atour.dfs.slave.net.SlaveServer;

public class SlaveApplication {

  /**
   * Starts the application
   *
   * @param args the CLI arguments, cannot be null
   *             first is the master address,
   *             second is the master port for communicating with the slaves,
   *             third is the slave port for communicating with the master,
   *             fourth is the port for communicating with clients to submit data,
   *             fifth is the port for data fetches,
   *             sixth is the port for slave registration at the master,
   *             seventh is the memory available in bytes
   */
  public static void main(@NonNull String... args) throws SocketException, UnknownHostException {
    if (args == null || args.length < 7) {
      throw new IllegalArgumentException();
    }
    SlaveServer server = new SlaveServer(InetAddress.getByName(args[0]), Integer.parseInt(args[1]),
        Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]), Integer.parseInt(args[5]),
        Long.parseLong(args[6]));
    while (true) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
        String command = reader.readLine();
        if (command != null && command.equals("exit")) {
          server.shutdown();
          break;
        }
      } catch (IOException e) {
        System.out.println("womp womp");
      }
    }
  }
}

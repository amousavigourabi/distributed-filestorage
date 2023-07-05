package me.atour.dfs.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketException;
import lombok.extern.slf4j.Slf4j;
import me.atour.dfs.master.net.MasterServer;

@Slf4j
public class MasterApplication {

  /**
   * Starts the application.
   *
   * @param args the CLI arguments, cannot be null
   *             the first is the master-client port,
   *             the second is the master-slave port
   * @throws SocketException when sockets cannot be opened or used
   */
  public static void main(String... args) throws SocketException {
    if (args == null || args.length < 2) {
      throw new IllegalArgumentException();
    }
    MasterServer server = new MasterServer(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
    while (true) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
        String command = reader.readLine();
        if (command != null && command.equals("exit")) {
          server.shutdown();
          break;
        }
      } catch (IOException e) {
        log.info("Cannot read from the command line because {}.", e.getMessage());
      }
    }
  }
}

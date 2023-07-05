package me.atour.dfs.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketException;
import me.atour.dfs.master.net.MasterServer;

public class MasterApplication {

  /**
   *
   * @param args the CLI arguments, cannot be null
   *             the first is the master-client port,
   *             the second is the master-slave port,
   *             the third is the heartbeat port
   * @throws SocketException
   */
  public static void main(String... args) throws SocketException {
    if (args == null || args.length < 3) {
      throw new IllegalArgumentException();
    }
    MasterServer server = new MasterServer(Integer.parseInt(args[0]), Integer.parseInt(args[1]),
        Integer.parseInt(args[2]));
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

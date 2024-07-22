package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import it.unitn.ds1.old_actors.LegacyClient;
import it.unitn.ds1.old_actors.LegacyCoordinator;
import it.unitn.ds1.old_actors.LegacyReplica;
import it.unitn.ds1.utils.Functions;
import it.unitn.ds1.utils.LegacyMessages.StartMessage;

/**
 * The Main class is the entry point for the distributed system simulation.
 * It initializes the actor system, creates the coordinator, replicas, and
 * clients, and sends initial messages.
 */
public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);
  final static int N_REPLICAS = 3; // Number of replicas
  final static int N_CLIENTS = 2; // Number of clients

  public static void main(String[] args) {
    final ActorSystem system = ActorSystem.create("main");

    try {
      // Create the coordinator
      ActorRef coordinator = system.actorOf(LegacyCoordinator.props(), "coordinator");

      // Create replicas and put them into a list
      List<ActorRef> replicas = new ArrayList<>();
      for (int i = 0; i < N_REPLICAS; i++) {
        replicas.add(system.actorOf(LegacyReplica.props(i, coordinator), "replica" + i));
      }

      // Create clients and put them into a list
      List<ActorRef> clients = new ArrayList<>();
      for (int i = 0; i < N_CLIENTS; i++) {
        clients.add(system.actorOf(LegacyClient.props(i), "client" + i));
      }

      // Send start message to coordinator and clients
      StartMessage start = new StartMessage(replicas);
      coordinator.tell(start, ActorRef.noSender());
      // Functions.multicast(start, clients, ActorRef.noSender());

      // Wait for user input to terminate
      logger.info(">>> Press ENTER to exit <<<");
      System.in.read();
    } catch (IOException ioe) {
      logger.error("IOException occurred during input reading", ioe);
    } catch (Exception e) {
      logger.error("Unexpected error occurred", e);
    } finally {
      system.terminate();
    }
  }
}

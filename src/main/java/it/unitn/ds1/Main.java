package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;

import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.Coordinator;
import it.unitn.ds1.actors.Replica;

/**
 * The Main class is the entry point for the distributed system simulation.
 * It initializes the actor system, creates the coordinator, replicas, and
 * clients, and sends initial messages.
 */
public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);
  final static int N_REPLICAS = 2; // Number of replicas
  final static int N_CLIENTS = 1; // Number of clients

  /**
   * StartMessage is sent to initialize the actors with the list of participants.
   */
  public static class StartMessage implements Serializable {
    public final List<ActorRef> group;

    public StartMessage(List<ActorRef> group) {
      this.group = Collections.unmodifiableList(new ArrayList<>(group));
    }
  }

  public static void main(String[] args) {
    // Create the actor system
    final ActorSystem system = ActorSystem.create("main");

    // Create the coordinator
    ActorRef coordinator = system.actorOf(Coordinator.props(), "coordinator");

    // Create replicas and put them into a list
    List<ActorRef> replicas = new ArrayList<>();
    for (int i = 0; i < N_REPLICAS; i++) {
      replicas.add(system.actorOf(Replica.props(i, coordinator), "replica" + i));
    }

    // Create clients and put them into a list
    List<ActorRef> clients = new ArrayList<>();
    for (int i = 0; i < N_CLIENTS; i++) {
      clients.add(system.actorOf(Client.props(i), "client" + i));
    }

    // Send start message to coordinator and clients
    StartMessage start = new StartMessage(replicas);
    coordinator.tell(start, ActorRef.noSender());

    for (ActorRef peer : clients) {
      peer.tell(start, ActorRef.noSender());
    }

    // Wait for user input to terminate
    try {
      logger.info(">>> Press ENTER to exit <<<");
      System.in.read();
    } catch (IOException ioe) {
      logger.error("IOException occurred", ioe);
    }
    system.terminate();
  }
}

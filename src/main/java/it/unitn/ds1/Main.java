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

public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);
  final static int N_REPLICAS = 2;
  final static int N_CLIENTS = 1;

  // Start message that sends the list of participants to everyone
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

    StartMessage start = new StartMessage(replicas);
    coordinator.tell(start, ActorRef.noSender());

    // Send join messages to the banks to inform them of all the replicas
    for (ActorRef peer : clients) {
      peer.tell(start, ActorRef.noSender());
    }

    try {
      logger.info(">>> Press ENTER to exit <<<");
      System.in.read();
    } catch (IOException ioe) {
      logger.error("IOException occurred", ioe);
    }
    system.terminate();
  }
}

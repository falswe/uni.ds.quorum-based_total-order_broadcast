package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.Replica;
import it.unitn.ds1.utils.Messages.*;
import it.unitn.ds1.actors.Replica.CrashType;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.io.IOException;

public class QuorumBasedTotalOrderBroadcast {
  final static int N_REPLICAS = 6;
  final static int N_CLIENTS = 4;

  // needed for our logging framework
  private static final Logger logger = LoggerFactory.getLogger(QuorumBasedTotalOrderBroadcast.class);

  public static void main(String[] args) {

    // Create the actor system
    final ActorSystem system = ActorSystem.create("QBTOBSystem");

    // Create nodes and put them to a list
    List<ActorRef> replicas = new ArrayList<>();

    // Create a coordinator of the system
    ActorRef coordinator = system.actorOf(Replica.props(), "replica" + 0);
    replicas.add(coordinator);

    for (int i = 1; i < N_REPLICAS; i++) {
      replicas.add(system.actorOf(Replica.props(coordinator), "replica" + i));
    }

    // Create nodes and put them to a list
    List<ActorRef> clients = new ArrayList<>();
    for (int i = 0; i < N_CLIENTS; i++) {
      clients.add(system.actorOf(Client.props(), "client" + i));
    }

    // Send join messages to the manager and the nodes to inform them of the whole
    // group
    JoinGroupMsg start = new JoinGroupMsg(replicas);
    for (ActorRef client : clients) {
      client.tell(start, ActorRef.noSender());
    }
    for (ActorRef replica : replicas) {
      replica.tell(start, ActorRef.noSender());
    }

    inputContinue();

    logger.info("Initiating some Reads and Writes");
    clients.get(1).tell(new ClientRead(), ActorRef.noSender());
    clients.get(2).tell(new ClientWrite(), ActorRef.noSender());
    clients.get(3).tell(new ClientWrite(), ActorRef.noSender());
    clients.get(0).tell(new ClientRead(), ActorRef.noSender());

    inputContinue();

    logger.info("Crashing Replica 0 (coordinator)");
    replicas.get(0).tell(new CrashMsg(CrashType.NotResponding), ActorRef.noSender());

    inputContinue();

    logger.info("Calling a Write Request from client 2");
    clients.get(2).tell(new ClientWrite(), ActorRef.noSender());
    logger.info("Crashing Replica 5 (coordinator) while sending Write Ok");
    logger.info("Crashing Replica 2 during the election phase");
    logger.info("Crashing Replica 3 while choosing the Coordinator");
    replicas.get(2).tell(new CrashMsg(CrashType.WhileElection), ActorRef.noSender());
    replicas.get(3).tell(new CrashMsg(CrashType.WhileChoosingCoordinator), ActorRef.noSender());
    replicas.get(5).tell(new CrashMsg(CrashType.WhileSendingWriteOk), ActorRef.noSender());

    inputContinue();
  }

  public static void inputContinue() {
    try {
      System.out.println(">>> Press ENTER to continue <<<");
      while (System.in.read() != '\n') {
        // Consume all characters until newline
      }
    } catch (IOException ignored) {
    }
  }
}

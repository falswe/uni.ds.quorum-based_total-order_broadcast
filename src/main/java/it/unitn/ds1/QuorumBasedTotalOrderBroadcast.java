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
  final static int N_REPLICAS = 9;
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
    clients.get(2).tell(new ClientRead(1), ActorRef.noSender());
    clients.get(1).tell(new ClientRead(0), ActorRef.noSender());
    clients.get(2).tell(new ClientWrite(2), ActorRef.noSender());
    clients.get(3).tell(new ClientWrite(3), ActorRef.noSender());
    clients.get(0).tell(new ClientRead(2), ActorRef.noSender());

    inputContinue();

    //replicas Alive = 8 (0,1,2,3,4,5,6,7) [0 coordinator]

    logger.info("Crashing Replica 0 (coordinator)");
    replicas.get(0).tell(new CrashMsg(CrashType.NotResponding), ActorRef.noSender());

    //replica Alive = 8 (1,2,3,4,5,6,7,8) [8 coordinator]

    inputContinue();

    //replica Alive = 8 (1,2,3,4,5,6,7,8) [8 coordinator]

    logger.info("Calling a Write Request from client 0");
    clients.get(0).tell(new ClientWrite(3), ActorRef.noSender());
    logger.info("Crashing Replica 8 (coordinator) while sending Update");
    replicas.get(8).tell(new CrashMsg(CrashType.WhileSendingUpdate), ActorRef.noSender());
    logger.info("Crashing Replica 2 during the election phase");
    replicas.get(2).tell(new CrashMsg(CrashType.WhileElection), ActorRef.noSender());

    //replica Alive = 6 (1,3,4,5,6,7) [7 coordinator]

    inputContinue();

    //replica Alive = 6 (1,3,4,5,6,7) [7 coordinator]

    logger.info("Calling a Write Request from client 2");
    clients.get(2).tell(new ClientWrite(1), ActorRef.noSender());
    logger.info("Crashing Replica 1 after receiving Update");
    replicas.get(1).tell(new CrashMsg(CrashType.AfterReceivingUpdate), ActorRef.noSender());
    logger.info("Crashing Replica 7 (coordinator) while sending Write Ok");
    replicas.get(7).tell(new CrashMsg(CrashType.WhileSendingWriteOk), ActorRef.noSender());
    logger.info("Crashing Replica 3 while choosing the Coordinator");
    replicas.get(3).tell(new CrashMsg(CrashType.WhileChoosingCoordinator), ActorRef.noSender());

    //replica Alive = 3 (4,5,6) [4 coordinator]

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

package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.Replica;
import it.unitn.ds1.utils.Messages;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuorumBasedTotalOrderBroadcast {
  private static final int N_REPLICAS = 9;
  private static final int N_CLIENTS = 4;
  private static final Logger logger = LoggerFactory.getLogger(QuorumBasedTotalOrderBroadcast.class);

  public static void main(String[] args) {
    final ActorSystem system = ActorSystem.create("QBTOBSystem");

    List<ActorRef> replicas = createReplicas(system);
    List<ActorRef> clients = createClients(system);

    initializeSystem(replicas, clients);

    runSimulation(replicas, clients);

    system.terminate();
  }

  private static List<ActorRef> createReplicas(ActorSystem system) {
    List<ActorRef> replicas = new ArrayList<>();
    ActorRef coordinator = system.actorOf(Replica.props(), "replica0");
    replicas.add(coordinator);

    for (int i = 1; i < N_REPLICAS; i++) {
      replicas.add(system.actorOf(Replica.props(coordinator), "replica" + i));
    }
    return replicas;
  }

  private static List<ActorRef> createClients(ActorSystem system) {
    List<ActorRef> clients = new ArrayList<>();
    for (int i = 0; i < N_CLIENTS; i++) {
      clients.add(system.actorOf(Client.props(), "client" + i));
    }
    return clients;
  }

  private static void initializeSystem(List<ActorRef> replicas, List<ActorRef> clients) {
    Messages.JoinGroupMsg start = new Messages.JoinGroupMsg(replicas);
    for (ActorRef client : clients) {
      client.tell(start, ActorRef.noSender());
    }
    for (ActorRef replica : replicas) {
      replica.tell(start, ActorRef.noSender());
    }
  }

  private static void runSimulation(List<ActorRef> replicas, List<ActorRef> clients) {
    waitForUserInput();

    simulateReadsAndWrites(clients);
    waitForUserInput();

    // replicas Alive = 9 (0,1,2,3,4,5,6,7,8) [0 coordinator]
    simulateCoordinatorCrash(replicas, clients);
    waitForUserInput();

    // replicas Alive = 8 (1,2,3,4,5,6,7,8) [8 coordinator]
    simulateElectionCrashes(replicas, clients);
    waitForUserInput();

    // replica Alive = 5 (1,4,5,6,7) [7 coordinator]
    simulateUpdateCrash(replicas, clients);
    waitForUserInput();

    // replica Alive = 4 (1,4,5,6) [6 coordinator]
    simulateWriteOkCrashes(replicas, clients);
    waitForUserInput();

    // replica Alive = 2 (4,5) [4 coordinator]
  }

  private static void simulateReadsAndWrites(List<ActorRef> clients) {
    logger.info("Initiating some Reads and Writes");
    clients.get(2).tell(new Messages.Client.Read(1), ActorRef.noSender());
    clients.get(1).tell(new Messages.Client.Read(0), ActorRef.noSender());
    clients.get(2).tell(new Messages.Client.Write(2), ActorRef.noSender());
    clients.get(3).tell(new Messages.Client.Write(3), ActorRef.noSender());
    clients.get(0).tell(new Messages.Client.Read(2), ActorRef.noSender());
  }

  private static void simulateCoordinatorCrash(List<ActorRef> replicas, List<ActorRef> clients) {
    logger.info("Crashing Replica 0 (coordinator)");
    replicas.get(0).tell(new Messages.CrashMsg(Messages.CrashType.NotResponding), ActorRef.noSender());
  }

  private static void simulateElectionCrashes(List<ActorRef> replicas, List<ActorRef> clients) {
    logger.info("Crashing Replica 2 during the election phase");
    replicas.get(2).tell(new Messages.CrashMsg(Messages.CrashType.WhileElection), ActorRef.noSender());
    logger.info("Crashing Replica 3 while choosing the Coordinator");
    replicas.get(3).tell(new Messages.CrashMsg(Messages.CrashType.WhileChoosingCoordinator), ActorRef.noSender());

    logger.info("Crashing Replica 8 (coordinator)");
    replicas.get(8).tell(new Messages.CrashMsg(Messages.CrashType.NotResponding), ActorRef.noSender());
  }

  private static void simulateUpdateCrash(List<ActorRef> replicas, List<ActorRef> clients) {
    logger.info("Crashing Replica 7 (coordinator) while sending Update");
    replicas.get(7).tell(new Messages.CrashMsg(Messages.CrashType.WhileSendingUpdate), ActorRef.noSender());

    logger.info("Calling a Write Request from client 0");
    clients.get(0).tell(new Messages.Client.Write(4), ActorRef.noSender());
  }

  private static void simulateWriteOkCrashes(List<ActorRef> replicas, List<ActorRef> clients) {
    logger.info("Crashing Replica 1 after receiving Update");
    replicas.get(1).tell(new Messages.CrashMsg(Messages.CrashType.AfterReceivingUpdate), ActorRef.noSender());
    logger.info("Crashing Replica 6 (coordinator) while sending Write Ok");
    replicas.get(6).tell(new Messages.CrashMsg(Messages.CrashType.WhileSendingWriteOk), ActorRef.noSender());

    logger.info("Calling a Write Request from client 2");
    clients.get(2).tell(new Messages.Client.Write(1), ActorRef.noSender());
  }

  private static void waitForUserInput() {
    System.out.println(">>> Press ENTER to continue <<<");
    try {
      System.in.read();
    } catch (IOException ignored) {
      // Handle or log the exception if needed
    }
  }
}

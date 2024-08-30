package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.Replica;
import it.unitn.ds1.utils.Message;
import it.unitn.ds1.utils.Message.CrashType;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QBTOB {
  private static final int N_REPLICAS = 9;
  private static final int N_CLIENTS = 4;
  private static final Logger logger = LoggerFactory.getLogger(QBTOB.class);

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
    ActorRef coordinator = system.actorOf(Replica.props(), "Replica0");
    replicas.add(coordinator);

    for (int i = 1; i < N_REPLICAS; i++) {
      replicas.add(system.actorOf(Replica.props(coordinator), "Replica" + i));
    }
    return replicas;
  }

  private static List<ActorRef> createClients(ActorSystem system) {
    List<ActorRef> clients = new ArrayList<>();
    for (int i = 0; i < N_CLIENTS; i++) {
      clients.add(system.actorOf(Client.props(), "Client" + i));
    }
    return clients;
  }

  private static void initializeSystem(List<ActorRef> replicas, List<ActorRef> clients) {
    Message.System.JoinGroup joinGroupMessage = new Message.System.JoinGroup(replicas);
    for (ActorRef client : clients) {
      client.tell(joinGroupMessage, ActorRef.noSender());
    }
    for (ActorRef replica : replicas) {
      replica.tell(joinGroupMessage, ActorRef.noSender());
    }
  }

  private static void runSimulation(List<ActorRef> replicas, List<ActorRef> clients) {
    logger.info("Starting simulation with {} replicas and {} clients", N_REPLICAS, N_CLIENTS);
    waitForUserInput();

    simulateReadsAndWrites(clients);
    waitForUserInput();

    // replicas Alive = 9 (0,1,2,3,4,5,6,7,8) [0 coordinator]
    simulateCoordinatorCrash(replicas, clients);
    waitForUserInput();

    // replicas Alive = 8 (1,2,3,4,5,6,7,8) [8 coordinator]
    simulateElectionCrashes(replicas, clients);
    waitForUserInput();

    // replicas Alive = 5 (1,4,5,6,7) [7 coordinator]
    simulateUpdateCrash(replicas, clients);
    waitForUserInput();

    // replicas Alive = 4 (1,4,5,6) [6 coordinator]
    simulateWriteOkCrashes(replicas, clients);
    waitForUserInput();

    // replicas Alive = 2 (4,5) [4 coordinator]
  }

  private static void simulateReadsAndWrites(List<ActorRef> clients) {
    logger.info("Initiating read and write operations");
    clients.get(2).tell(new Message.Client.Read(1), ActorRef.noSender());
    clients.get(1).tell(new Message.Client.Read(0), ActorRef.noSender());
    clients.get(2).tell(new Message.Client.Write(2), ActorRef.noSender());
    clients.get(3).tell(new Message.Client.Write(3), ActorRef.noSender());
    clients.get(0).tell(new Message.Client.Read(2), ActorRef.noSender());
  }

  private static void simulateCoordinatorCrash(List<ActorRef> replicas, List<ActorRef> clients) {
    logger.info("Crashing Replica 0 (coordinator)");
    replicas.get(0).tell(new Message.System.Crash(CrashType.NOT_RESPONDING), ActorRef.noSender());
  }

  private static void simulateElectionCrashes(List<ActorRef> replicas, List<ActorRef> clients) {
    logger.info("Crashing Replica 2 during the election phase");
    replicas.get(2).tell(new Message.System.Crash(CrashType.WHILE_ELECTION), ActorRef.noSender());
    logger.info("Crashing Replica 3 while choosing the coordinator");
    replicas.get(3).tell(new Message.System.Crash(CrashType.WHILE_CHOOSING_COORDINATOR), ActorRef.noSender());

    logger.info("Crashing Replica 8 (coordinator)");
    replicas.get(8).tell(new Message.System.Crash(CrashType.NOT_RESPONDING), ActorRef.noSender());
  }

  private static void simulateUpdateCrash(List<ActorRef> replicas, List<ActorRef> clients) {
    logger.info("Crashing Replica 7 (coordinator) while sending Update");
    replicas.get(7).tell(new Message.System.Crash(CrashType.WHILE_SENDING_UPDATE), ActorRef.noSender());

    logger.info("Calling a Write Request from Client 0");
    clients.get(0).tell(new Message.Client.Write(4), ActorRef.noSender());
  }

  private static void simulateWriteOkCrashes(List<ActorRef> replicas, List<ActorRef> clients) {
    logger.info("Crashing Replica 1 after receiving Update");
    replicas.get(1).tell(new Message.System.Crash(CrashType.AFTER_RECEIVING_UPDATE), ActorRef.noSender());
    logger.info("Crashing Replica 6 (coordinator) while sending Write Ok");
    replicas.get(6).tell(new Message.System.Crash(CrashType.WHILE_SENDING_WRITE_OK), ActorRef.noSender());

    logger.info("Calling a Write Request from Client 2");
    clients.get(2).tell(new Message.Client.Write(1), ActorRef.noSender());
  }

  private static void waitForUserInput() {
    System.out.println(">>> Press ENTER to continue <<<");
    try {
      System.in.read();
      while (System.in.available() > 0) {
        System.in.read();
      }
    } catch (IOException e) {
      logger.error("Error reading user input", e);
    }
  }
}

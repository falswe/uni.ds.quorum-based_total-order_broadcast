package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.Coordinator;
import it.unitn.ds1.actors.Replica;
import it.unitn.ds1.actors.Replica.JoinGroupMsg;
import it.unitn.ds1.actors.Replica.CrashMsg;
import it.unitn.ds1.actors.Replica.CrashType;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class QuorumBasedTotalOrderBroadcast {
  final static int N_REPLICAS = 2;
  final static int N_CLIENTS = 1;

  public static void main(String[] args) {

    // Create the actor system
    final ActorSystem system = ActorSystem.create("QBTOBSystem");

    // Create a coordinator of the system
    ActorRef coordinator = system.actorOf(Coordinator.props(), "coordinator");

    // Create nodes and put them to a list
    List<ActorRef> replicas = new ArrayList<>();
    for (int i = 0; i < N_REPLICAS; i++) {
      replicas.add(system.actorOf(Replica.props(coordinator, false), "replica" + i));
    }

    // Create nodes and put them to a list
    List<ActorRef> clients = new ArrayList<>();
    for (int i = 0; i < N_CLIENTS; i++) {
      clients.add(system.actorOf(Client.props(), "client" + i));
    }

    // Send join messages to the manager and the nodes to inform them of the whole
    // group
    JoinGroupMsg start = new JoinGroupMsg(replicas);
    coordinator.tell(start, ActorRef.noSender());
    for (ActorRef client : clients) {
      client.tell(start, ActorRef.noSender());
    }
    for (ActorRef replica : replicas) {
      replica.tell(start, ActorRef.noSender());
    }

    inputContinue();

    replicas.get(1).tell(new CrashMsg(CrashType.NotResponding, 2), ActorRef.noSender());

    inputContinue();

    /*
     * 
     * // Create new nodes and make them join the existing group
     * ActorRef joiningFirst = system.actorOf(VirtualSynchActor.props(manager,
     * true), "vsnodeJ0");
     * 
     * inputContinue();
     * 
     * ActorRef joiningSecond = system.actorOf(VirtualSynchActor.props(manager,
     * true), "vsnodeJ1");
     * 
     * inputContinue();
     * 
     * // Make one of the new nodes crash while sending stabilization,
     * // and the other while sending the flush (which will occur due to the first
     * crash);
     * // nextCrashAfter in CrashMsg controls how many messages are correctly sent
     * before crashing
     * //joiningFirst.tell(new CrashMsg(CrashType.StableChatMsg, 2),
     * ActorRef.noSender());
     * joiningFirst.tell(new CrashMsg(CrashType.StableChatMsg, 2),
     * ActorRef.noSender());
     * 
     * inputContinue();
     * 
     * joiningSecond.tell(new CrashMsg(CrashType.ViewFlushMsg, 0),
     * ActorRef.noSender());
     * 
     * inputContinue();
     * 
     * // Restart nodes (they will join again through the manager)
     * joiningFirst.tell(new RecoveryMsg(), ActorRef.noSender());
     * 
     * inputContinue();
     * 
     * joiningSecond.tell(new RecoveryMsg(), ActorRef.noSender());
     * 
     * inputContinue();
     * 
     * // Make one of the new nodes crash while sending stabilization,
     * // and the other while sending the flush (which will occur due to the first
     * crash);
     * // nextCrashAfter in CrashMsg controls how many messages are correctly sent
     * before crashing
     * joiningSecond.tell(new CrashMsg(CrashType.ViewFlushMsg, 1),
     * ActorRef.noSender());
     * 
     * inputContinue();
     * 
     * joiningFirst.tell(new CrashMsg(CrashType.ChatMsg, 1), ActorRef.noSender());
     * 
     * inputContinue();
     * 
     * // system shutdown
     * system.terminate();
     */
  }

  public static void inputContinue() {
    try {
      System.out.println(">>> Press ENTER to continue <<<");
      System.in.read();
    } catch (IOException ignored) {
    }
  }
}

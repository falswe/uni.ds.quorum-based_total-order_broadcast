package it.unitn.ds1.actors;

import akka.actor.*;
import it.unitn.ds1.actors.Client.WrRqMsg;
import it.unitn.ds1.actors.Replica.AckMsg;
import it.unitn.ds1.actors.Replica.UpRqMsg;
import it.unitn.ds1.actors.Replica.JoinGroupMsg;
import it.unitn.ds1.actors.Replica.ViewChangeMsg;
import it.unitn.ds1.utils.Functions;

import java.io.Serializable;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator extends AbstractActor {
  // needed for our logging framework
  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

  // participants (initial group, current and proposed views)
  private final List<ActorRef> replicas;
  private final Set<ActorRef> view;
  private int viewId;

  // message sequence number for identification
  private int epoch;
  private int seqno;

  private final Map<Integer, Integer> seqnoAckCounter;

  /*-- Actor constructors --------------------------------------------------- */
  public Coordinator() {
    replicas = new ArrayList<>();
    view = new HashSet<>(replicas);
    seqnoAckCounter = new HashMap<>();
    viewId = 0;
    this.seqno = 0;
    this.epoch = 0;
  }

  static public Props props() {
    return Props.create(Coordinator.class, Coordinator::new);
  }

  /*-- Message classes ------------------------------------------------------ */

  public static class CrashReportMsg implements Serializable {
    public final Set<ActorRef> crashedMembers;

    public CrashReportMsg(Set<ActorRef> crashedMembers) {
      this.crashedMembers = Collections.unmodifiableSet(crashedMembers);
    }
  }

  public static class JoinNodeMsg implements Serializable {
  }

  /*-- Actor logic ---------------------------------------------------------- */

  @Override
  public void preStart() {
  }

  private void onJoinGroupMsg(JoinGroupMsg msg) {

    // initialize group
    for (ActorRef r : msg.group) {
      if (!r.equals(getSelf())) {
        this.replicas.add(r);
      }
    }

    // at the beginning, the view includes all nodes in the group
    view.addAll(replicas);
    // System.out.println(getSelf().path().name() + " initial view " + view);
  }

  private void onWrRqMsg(WrRqMsg msg) {
    logger.info("Coordinator received write request from replica {} with value {}", Functions.getId(getSender()), msg.new_value);
    Functions.multicast(new UpRqMsg(epoch, ++seqno, getSender(), msg.new_value), replicas, getSelf());
  }

  private void onAck(AckMsg msg) {
    seqnoAckCounter.put(msg.seqno, seqnoAckCounter.getOrDefault(msg.seqno, 0) + 1);
    logger.info("Coordinator received {} ack(s) from replica {} with seqno {}", seqnoAckCounter.get(msg.seqno), Functions.getId(getSender()), msg.seqno);
    int Q = (replicas.size() / 2) + 1;

    if (seqnoAckCounter.get(msg.seqno) == Q) {
      logger.info("Coordinator confirm the update to all the replica");
      Functions.multicast(msg, replicas, getSelf());
    }
  }

  private void onCrashReportMsg(CrashReportMsg msg) {

    // remove the crashed node from view;
    // if the view changed, update view ID and notify nodes
    boolean viewChange = false;
    for (ActorRef crashed : msg.crashedMembers) {
      if (view.remove(crashed)) {
        viewChange = true;
      }
    }
    if (viewChange) {
      viewId++;
      ViewChangeMsg m = new ViewChangeMsg(viewId, view);
      System.out.println(
          getSelf().path().name() + " view " + m.viewId
              + " of " + m.proposedView.size() + " nodes "
              + " (" + msg.crashedMembers + " crashed - reported by "
              + getSender().path().name() + ") " + m.proposedView);
      Functions.multicast(m, replicas, getSelf());
    }
  }

  private void onJoinNodeMsg(JoinNodeMsg msg) {

    // add node to view;
    // if the view changed, update view ID and notify nodes
    if (view.add(getSender())) {
      viewId++;
      ViewChangeMsg m = new ViewChangeMsg(viewId, view);
      System.out.println(
          getSelf().path().name() + " view " + m.viewId
              + " of " + m.proposedView.size() + " nodes "
              + " (" + getSender() + " joining) " + m.proposedView);
      Functions.multicast(m, replicas, getSelf());
    }
  }

  // Here we define the mapping between the received message types
  // and our actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(JoinGroupMsg.class, this::onJoinGroupMsg)
        .match(WrRqMsg.class, this::onWrRqMsg)
        .match(AckMsg.class, this::onAck)
        .match(JoinNodeMsg.class, this::onJoinNodeMsg)
        .match(CrashReportMsg.class, this::onCrashReportMsg)
        .build();
  }
}

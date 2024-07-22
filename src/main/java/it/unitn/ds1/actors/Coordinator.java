package it.unitn.ds1.actors;

import akka.actor.*;
import it.unitn.ds1.actors.Client.RdRqMsg;
import it.unitn.ds1.actors.Client.WrRqMsg;
import it.unitn.ds1.utils.Functions;

import java.io.Serializable;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator extends Replica {
  // needed for our logging framework
  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

  // timeouts
  private final static int HEARTBEAT_PERIOD = 1000;
  private final static int HEARTBEAT_TIMEOUT = 1000;

  // participants (initial group, current and proposed views)
  private Set<ActorRef> replicasAlive;
  private final Set<ActorRef> view;
  private int viewId;

  // message sequence number for identification
  private int epoch;
  private int seqno;

  private final Map<Integer, Integer> seqnoAckCounter;

  /*-- Actor constructors --------------------------------------------------- */
  public Coordinator() {
    super(null);
    coordinator = getSelf();
    replicasAlive = new HashSet<>();
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

  public static class CoordinatorHeartbeatPeriod implements Serializable {
  }

  public static class HeartbeatTimeout implements Serializable {
  }

  public static class CoordinatorHeartbeatMsg implements Serializable {
  }

  public static class HeartbeatPeriod implements Serializable {
  }

  public static class HeartbeatMsg implements Serializable {
  }

  public static class CrashReportMsg implements Serializable {
    public final Set<ActorRef> crashedMembers;

    public CrashReportMsg(Set<ActorRef> crashedMembers) {
      this.crashedMembers = Collections.unmodifiableSet(crashedMembers);
    }
  }

  public static class JoinNodeMsg implements Serializable {
  }

  public static class WrOk implements Serializable {
    public final int epoch;
    public final ActorRef sender;
    public final int seqno;

    public WrOk(int epoch, int seqno, ActorRef sender) {
      this.epoch = epoch;
      this.sender = sender;
      this.seqno = seqno;
    }
  }

  /*-- Actor logic ---------------------------------------------------------- */

  @Override
  public void preStart() {
    Functions.setTimeout(getContext(), HEARTBEAT_PERIOD, getSelf(), new CoordinatorHeartbeatPeriod());
  }

  public void onCoordinatorHeartbeatPeriod(CoordinatorHeartbeatPeriod msg) {
    logger.debug("Coordinator sent out a heartbeat message to all replicas");
    Functions.setTimeout(getContext(), HEARTBEAT_PERIOD, getSelf(), new CoordinatorHeartbeatPeriod());
    CoordinatorHeartbeatMsg confAlive = new CoordinatorHeartbeatMsg();
    Functions.multicast(confAlive, replicas, getSelf());
    Functions.setTimeout(getContext(), HEARTBEAT_TIMEOUT, getSelf(), new HeartbeatTimeout());
  }

  public void onHeartbeatMsg(HeartbeatMsg msg) {
    logger.debug("Coordinator got a heartbeat message from {}", Functions.getName(getSender()));
    replicasAlive.add(getSender());
  }

  public void onHeartbeatTimeout(HeartbeatTimeout msg) {
    logger.debug("Coordinator reached it's heartbeat timeout");
    if (replicas.size() != replicasAlive.size()) {
      logger.warn("Some replicas did not respond. Initiating failure handling.");

      replicas.clear();
      replicas.addAll(replicasAlive);
    }
    replicasAlive.clear();
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
    logger.info("Coordinator received write request from {} with value {}", Functions.getName(getSender()),
        msg.new_value);
    Functions.multicast(new UpdRqMsg(msg.c_snd, epoch, ++seqno, msg.op_cnt, msg.new_value), replicas, getSelf());
  }

  private void onAck(AckMsg msg) {
    seqnoAckCounter.put(msg.seqno, seqnoAckCounter.getOrDefault(msg.seqno, 0) + 1);
    logger.info("Coordinator received {} ack(s) from {} with seqno {}", seqnoAckCounter.get(msg.seqno),
        Functions.getName(getSender()), msg.seqno);
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

  private void onCrashMsg(CrashMsg msg) {
    if (msg.nextCrash == CrashType.NotResponding) {
      crash();
    } else {
      nextCrash = msg.nextCrash;
    }
  }

  // Here we define the mapping between the received message types
  // and our actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(JoinGroupMsg.class, this::onJoinGroupMsg)
        .match(RdRqMsg.class, this::onRdRqMsg)
        .match(WrRqMsg.class, this::onWrRqMsg)
        .match(UpdRqMsg.class, this::onUpdRqMsg)
        .match(UpdTimeout.class, this::onUpdTimeout)
        .match(AckTimeout.class, this::onAckTimeout)
        .match(HeartbeatMsg.class, this::onHeartbeatMsg)
        .match(CoordinatorHeartbeatPeriod.class, this::onCoordinatorHeartbeatPeriod)
        .match(CoordinatorHeartbeatMsg.class, this::onCoordinatorHeartbeatMsg)
        .match(WrOk.class, this::onWrOk)
        .match(AckMsg.class, this::onAck)
        .match(HeartbeatPeriod.class, this::onHeartbeatPeriod)
        .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
        .match(HeartbeatMsg.class, this::onHeartbeatMsg)
        .match(JoinNodeMsg.class, this::onJoinNodeMsg)
        .match(CrashMsg.class, this::onCrashMsg)
        .match(CrashReportMsg.class, this::onCrashReportMsg)
        .build();
  }
}

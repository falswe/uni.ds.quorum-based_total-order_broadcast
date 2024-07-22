package it.unitn.ds1.actors;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unitn.ds1.actors.Client.RdRqMsg;
import it.unitn.ds1.actors.Client.WrRqMsg;
import it.unitn.ds1.utils.Functions;
import it.unitn.ds1.utils.Messages.*;

public class Replica extends AbstractActor {

  // needed for our logging framework
  private static final Logger logger = LoggerFactory.getLogger(Replica.class);

  // holding the actual current value of the replica
  protected int value = 5;
  // message sequence number for identification
  private int epoch;
  private int seqno;

  // timeouts
  private final static int UPD_TIMEOUT = 1000; // Timeout for the update from coordinator, ms
  private final static int WRITEOK_TIMEOUT = 1000; // Timeout for the writeok from coordinator, ms
  private final static int REPLICA_HEARTBEAT_TIMEOUT = 1200; // Timeout for the heartbeat from coordinator, ms
  // coordinator timeouts
  private final static int COORDINATOR_HEARTBEAT_PERIOD = 1000;
  private final static int COORDINATOR_HEARTBEAT_TIMEOUT = 1000;

  // used to start the hearbeat period timeout
  static boolean firstHeartbeatReceived = false;
  private boolean heartbeatReceived = false;

  // replica coordinator manager
  protected ActorRef coordinator;
  // the variable would be true if the replica is the coordinator, false otherwise
  private boolean iscoordinator;

  // participants (initial group, current and proposed views)
  protected List<ActorRef> replicas;
  private List<ActorRef> replicasAlive; // used by the coordinator

  // counters for timeouts
  private final Map<Map<ActorRef, Integer>, Boolean> membersUpdRcvd;
  private final Map<Integer, Boolean> AckRcvd;

  // coordinator counters for acknowledgement received
  private final Map<Integer, Integer> seqnoAckCounter;

  // list of sequence number related to the value communicated from the
  // coordinator
  private final Map<Integer, Integer> seqnoValue;

  // type of the next simulated crash
  public enum CrashType {
    NONE,
    NotResponding, // the replica crashes, and does not respond
    WhileSendingUpdate,
    AfterReceivingUpdate,
    WhileSendingWriteOk,
    WhileElection,
    ChatMsg,
    StableChatMsg,
    ViewFlushMsg
  }

  protected CrashType nextCrash;

  // other variables
  private final Set<ActorRef> currentView;
  private final Map<Integer, Set<ActorRef>> proposedView;

  // last sequence number for each node message (to avoid delivering duplicates)
  private final Map<ActorRef, Integer> membersSeqno;

  // unstable messages
  private final Set<ChatMsg> unstableMsgSet;

  // deferred messages (of a future view)
  private final Set<ChatMsg> deferredMsgSet;

  // group view flushes
  private final Map<Integer, Set<ActorRef>> flushes;

  /*-- Actor constructors --------------------------------------------------- */
  public Replica() {
    this.seqno = 0;
    this.epoch = 0;
    this.coordinator = getSelf();
    this.iscoordinator = true;
    this.replicas = new ArrayList<>();
    this.replicasAlive = new ArrayList<>();
    this.membersUpdRcvd = new HashMap<>();
    this.AckRcvd = new HashMap<>();
    this.seqnoAckCounter = new HashMap<>();
    this.seqnoValue = new HashMap<>();
    this.nextCrash = CrashType.NONE;
    this.currentView = new HashSet<>();
    this.proposedView = new HashMap<>();
    this.unstableMsgSet = new HashSet<>();
    this.deferredMsgSet = new HashSet<>();
    this.flushes = new HashMap<>();
    this.membersSeqno = new HashMap<>();
  }

  public Replica(ActorRef coordinator) {
    this.seqno = 0;
    this.epoch = 0;
    this.coordinator = coordinator;
    this.iscoordinator = false;
    this.replicas = new ArrayList<>();
    this.replicasAlive = new ArrayList<>();
    this.membersUpdRcvd = new HashMap<>();
    this.AckRcvd = new HashMap<>();
    this.seqnoAckCounter = new HashMap<>();
    this.seqnoValue = new HashMap<>();
    this.nextCrash = CrashType.NONE;
    this.currentView = new HashSet<>();
    this.proposedView = new HashMap<>();
    this.unstableMsgSet = new HashSet<>();
    this.deferredMsgSet = new HashSet<>();
    this.flushes = new HashMap<>();
    this.membersSeqno = new HashMap<>();
  }

  static public Props props(ActorRef coordinator) {
    return Props.create(Replica.class, () -> new Replica(coordinator));
  }

  static public Props props() {
    return Props.create(Replica.class, () -> new Replica());
  }

  /*-- Actor start logic ---------------------------------------------------------- */

  @Override
  public void preStart() {
    if (iscoordinator) {
      Functions.setTimeout(getContext(), COORDINATOR_HEARTBEAT_PERIOD, getSelf(), new CoordinatorHeartbeatPeriod());
    }
  }

  /*-- Helper methods ---------------------------------------------------------- */

  private int multicast(Serializable m, Set<ActorRef> multicastGroup) {
    int i = 0;
    Random rnd = new Random();

    for (ActorRef r : multicastGroup) {

      // send m to r (except to self)
      if (!r.equals(getSelf())) {

        // model a random network/processing delay
        try {
          Thread.sleep(rnd.nextInt(10));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        r.tell(m, getSelf());
        i++;
      }
    }

    return i;
  }

  // TODO (solution) create void putInFlushes and boolean isViewFlushed methods

  private void putInFlushes(int viewId, ActorRef flushSender) {
    Set<ActorRef> flushed = flushes.getOrDefault(viewId, new HashSet<>());
    flushed.add(flushSender);
    flushes.put(viewId, flushed);
  }

  private boolean isViewFlushed(int viewId) {
    if (flushes.containsKey(viewId) && proposedView.containsKey(viewId + 1)) {
      return flushes.get(viewId).containsAll(proposedView.get(viewId + 1));
    }
    return false;
  }

  private boolean isViewChanging() {

    // TODO (solution) implement effective view change status check

    // in this implementation, a node adds a flush for itself upon receiving a
    // ViewChangeMsg,
    // and old flushes are removed when a view is installed;
    // thus, checking if there is an ongoing view change is trivial:
    // if there is any flush message, there is a view change that has not completed
    // yet.
    return !flushes.isEmpty();
  }

  private void deliver(ChatMsg m, boolean deferred) {
    if (membersSeqno.getOrDefault(m.sender, 0) < m.seqno) {
      membersSeqno.put(m.sender, m.seqno);
      System.out.println(
          getSelf().path().name() + " delivers " + m.seqno
              + " from " + m.sender.path().name() + " in view " + (deferred ? m.viewId : this.epoch)
              + (deferred ? " (deferred)" : ""));
    }
  }

  private boolean canDeliver(int viewId) {
    return this.epoch == viewId;
  }

  private void deferredDeliver(int prevViewId, int nextViewId) {

    // due to multiple crashes, some views may not have been installed;
    // make sure you deliver all pending messages between views
    for (int i = prevViewId; i <= nextViewId; i++) {
      for (ChatMsg m : deferredMsgSet) {
        if (m.viewId == i) {
          deliver(m, true);
        }
      }
    }
  }

  private void installView(int viewId) {

    // check if there are messages waiting to be delivered in the new view
    deferredDeliver(this.epoch, viewId);

    // update view ID
    this.epoch = viewId;

    // System.out.println(getSelf().path().name() + " flushes before view change " +
    // this.viewId + " " + flushes);

    // remove flushes, unstable and deferred messages of the old views
    flushes.entrySet().removeIf(entry -> entry.getKey() < this.epoch);
    unstableMsgSet.removeIf(unstableMsg -> unstableMsg.viewId < this.epoch);
    deferredMsgSet.removeIf(deferredMsg -> deferredMsg.viewId <= this.epoch);

    // System.out.println(getSelf().path().name() + " flushes after view change " +
    // this.viewId + " " + flushes);

    // update current view
    currentView.clear();
    currentView.addAll(proposedView.get(this.epoch));

    // remove proposed view entry as it is not needed anymore
    proposedView.entrySet().removeIf(entry -> entry.getKey() <= this.epoch);

    System.out.println(
        getSelf().path().name() + " installs view " + this.epoch + " " + currentView
            + " with updated proposedView " + proposedView);
  }

  /*-- Actor message handlers ---------------------------------------------------------- */

  protected void onRdRqMsg(RdRqMsg msg) {
    logger.info("{} received read request from client {}", Functions.getName(getSelf()),
        Functions.getId(getSender()));
    getSender().tell(new RdRspMsg(value), getSelf());
  }

  private void onWrRqMsg(WrRqMsg msg) {
    if (!iscoordinator) {
      logger.info("{} received write request from client {} with value {}", Functions.getName(getSelf()),
          Functions.getId(getSender()),
          msg.new_value);
      coordinator.tell(msg, getSelf());

      Functions.setTimeout(getContext(), UPD_TIMEOUT, getSelf(), new UpdTimeout(msg.c_snd, msg.op_cnt));
    } else {
      logger.info("Coordinator received write request from {} with value {}", Functions.getName(getSender()),
          msg.new_value);
      Functions.multicast(new UpdRqMsg(msg.c_snd, epoch, ++seqno, msg.op_cnt, msg.new_value), replicas, getSelf());
    }
  }

  protected void onUpdRqMsg(UpdRqMsg msg) {
    if (nextCrash == CrashType.WhileSendingUpdate)
      crash();

    logger.info("{} received UPDATE message from coordinator with value {} with seqno {}",
        Functions.getName(getSelf()), msg.value, msg.seqno);
    seqnoValue.put(msg.seqno, msg.value);

    Map m = Map.of(msg.sender, msg.op_cnt);
    membersUpdRcvd.put(m, true);

    coordinator.tell(new AckMsg(msg.epoch, msg.seqno, msg.sender), getSelf());

    Functions.setTimeout(getContext(), WRITEOK_TIMEOUT, getSelf(), new AckTimeout(msg.seqno));
  }

  protected void onWrOk(WrOk msg) {
    if (nextCrash == CrashType.WhileSendingWriteOk)
      crash();

    logger.info("{} changed the value from {} to {} with seqno {}", Functions.getName(getSelf()), value,
        seqnoValue.get(msg.seqno), msg.seqno);
    value = seqnoValue.get(msg.seqno);

    AckRcvd.put(msg.seqno, true);
  }

  protected void onUpdTimeout(UpdTimeout msg) {
    logger.debug("{} reached it's update timeout", Functions.getName(getSelf()));

    Map m = Map.of(msg.c_snd, msg.op_cnt);
    if (!membersUpdRcvd.getOrDefault(m, false)) {
      logger.error("Replica {} did not receive update in time. Coordinator might have crashed.",
          Functions.getId(getSelf()));

      List<ActorRef> candidates = new ArrayList<ActorRef>();
      coordinatorCrashRecovery(candidates);
    }
  }

  protected void onAckTimeout(AckTimeout msg) {
    logger.debug("{} reached it's write_ok timeout", Functions.getName(getSelf()));

    if (!AckRcvd.getOrDefault(msg.seqno, false)) {
      logger.error("Replica {} did not receive write acknowledgment in time. Coordinator might have crashed.",
          Functions.getId(getSelf()));

      List<ActorRef> candidates = new ArrayList<ActorRef>();
      coordinatorCrashRecovery(candidates);
    }
  }

  protected void onCoordinatorHeartbeatMsg(CoordinatorHeartbeatMsg msg) {
    if (!firstHeartbeatReceived) {
      firstHeartbeatReceived = true;
      Functions.setTimeout(getContext(), REPLICA_HEARTBEAT_TIMEOUT, getSelf(), new HeartbeatPeriod());
    }

    logger.debug("{} received a heartbeat message from the coordinator", Functions.getName(getSelf()));
    heartbeatReceived = true;

    coordinator.tell(new HeartbeatMsg(), getSelf());
  }

  protected void onHeartbeatPeriod(HeartbeatPeriod msg) {
    logger.debug("{} reached it's heartbeat timeout", Functions.getName(getSelf()));

    if (!heartbeatReceived) {
      logger.error("{} did not heartbeat in time. Coordinator might have crashed.", Functions.getName(getSelf()));

      List<ActorRef> candidates = new ArrayList<ActorRef>();
      coordinatorCrashRecovery(candidates);
    }
    heartbeatReceived = false;
    Functions.setTimeout(getContext(), REPLICA_HEARTBEAT_TIMEOUT, getSelf(), new HeartbeatPeriod());
  }

  private void onJoinGroupMsg(JoinGroupMsg msg) {
    // initialize group
    replicas.addAll(msg.group);

    // at the beginning, the view includes all nodes in the group
    currentView.addAll(replicas);
  }

  private void coordinatorCrashRecovery(List<ActorRef> candidates) {
    int selfIndex = replicas.indexOf(getSelf());
    int nextIndex = (selfIndex + 1) % replicas.size();

    replicas.remove(coordinator);

    candidates.add(getSelf());
    logger.info("{} sending an election message to {}", Functions.getName(getSelf()), Functions.getName(getSender()));
    replicas.get(nextIndex).tell(new ElectionMsg(candidates), getSelf());
  }

  private void electCoordinator(List<ActorRef> candidates) {
    int selfIndex = replicas.indexOf(getSelf());
    int nextIndex = (selfIndex + 1) % replicas.size();
    int maxId = 0;
    ActorRef replicaMaxId = null;

    for (ActorRef c : candidates) {
      if (Functions.getId(c) >= maxId) {
        maxId = Functions.getId(c);
        replicaMaxId = c;
      }
    }

    if (replicaMaxId == getSelf()) {
      // TO DO: become coordinator
      iscoordinator = true;
      epoch++;
      Functions.setTimeout(getContext(), COORDINATOR_HEARTBEAT_PERIOD, getSelf(), new CoordinatorHeartbeatPeriod());
      logger.info("{}: the new coordinator is me", Functions.getName(getSelf()));
    } else {
      logger.info("{}: the new coordinator is {}", Functions.getName(getSelf()), Functions.getName(replicaMaxId));
      coordinator = replicaMaxId;
    }

    replicas = candidates;
    replicas.get(nextIndex).tell(new CoordinatorMsg(candidates), getSelf());
  }

  private void onElectionMsg(ElectionMsg msg) {
    logger.info("{} receiving an election message from {}", Functions.getName(getSelf()),
        Functions.getName(getSender()));
    if (msg.coordinatorCandidates.contains(getSelf())) {
      // if we are on the coordinator candidates list, send coordinator message
      logger.info("{} ");
      electCoordinator(msg.coordinatorCandidates);
    } else {
      // otherwise keep passing the election message
      coordinatorCrashRecovery(msg.coordinatorCandidates);
    }
  }

  private void onCoordinatorMsg(CoordinatorMsg msg) {
    electCoordinator(msg.coordinatorCandidates);
  }

  private void onSendChatMsg(SendChatMsg msg) {
    Random rnd = new Random();

    // schedule next ChatMsg
    getContext().system().scheduler().scheduleOnce(
        Duration.create(rnd.nextInt(1000) + 300, TimeUnit.MILLISECONDS),
        getSelf(), // destination actor reference
        new SendChatMsg(), // the message to send
        getContext().system().dispatcher(), // system dispatcher
        getSelf() // source of the message (myself)
    );

    // avoid transmitting during view changes
    if (isViewChanging())
      return;

    // prepare chat message and add it to the unstable set
    String content = "Message " + seqno + " in view " + currentView;
    ChatMsg m = new ChatMsg(epoch, getSelf(), seqno, content);
    unstableMsgSet.add(m);

    // send message to the group
    int numSent = multicast(m, currentView);
    System.out.println(
        getSelf().path().name() + " multicasts " + m.seqno
            + " in view " + this.epoch + " to " + (currentView.size() - 1) + " nodes"
            + " (" + numSent + ", " + currentView + ")");

    // increase local sequence number (for packet identification)
    seqno++;

    // check if the node should crash
    if (nextCrash.name().equals(CrashType.ChatMsg.name())) {
      crash();
      return;
    }

    // after the message has been sent, it is stable for the sender
    unstableMsgSet.remove(m);

    // broadcast stabilization message
    multicast(new StableChatMsg(m), currentView);

    // check if the node should crash
    if (nextCrash.name().equals(CrashType.StableChatMsg.name())) {
      crash();
    }
  }

  private void onChatMsg(ChatMsg msg) {

    // ignore own messages (may have been sent during flush protocol)
    if (getSelf().equals(msg.sender))
      return;

    // the node will deliver the message, but it will also be kept in the unstable
    // set;
    // if the initiator crashes, we can retransmit the unstable message
    unstableMsgSet.add(msg);

    // deliver immediately or add to deferred to deliver in a future view
    if (canDeliver(msg.viewId)) {
      deliver(msg, false);
    } else {
      System.out.println(getSelf().path().name() + " deferred " + msg.seqno + " from " + msg.sender.path().name());
      deferredMsgSet.add(msg);
    }

    // send message to self in order to timeout while waiting stabilization;
    // schedule timeout only if the message was sent by the original initiator,
    // this way we prevent setting a timeout during flush protocol
    if (getSender().equals(msg.sender)) {
      getContext().system().scheduler().scheduleOnce(
          Duration.create(1000, TimeUnit.MILLISECONDS), // how frequently generate them
          getSelf(), // destination actor reference
          new StableTimeoutMsg(msg, getSender()), // the message to send
          getContext().system().dispatcher(), // system dispatcher
          getSelf() // source of the message (myself)
      );
    }
  }

  private void onStableChatMsg(StableChatMsg msg) {
    unstableMsgSet.remove(msg.stableMsg);
  }

  private void onStableTimeoutMsg(StableTimeoutMsg msg) {

    // check if the message is still unstable
    if (!unstableMsgSet.contains(msg.unstableMsg))
      return;

    // alert the manager about the crashed node
    Set<ActorRef> crashed = new HashSet<>();
    crashed.add(msg.unstableMsg.sender);
    // coordinator.tell(new CrashReportMsg(crashed), getSelf());
  }

  private void onCrashedChatMsg(ChatMsg msg) {

    // deliver immediately or ignore the message;
    // used to debug virtual synchrony correctness
    if (msg.viewId >= this.epoch) {
      if (membersSeqno.getOrDefault(msg.sender, 0) < msg.seqno) {
        membersSeqno.put(msg.sender, msg.seqno);
        System.out.println(
            "(crashed) " +
                getSelf().path().name() + " delivers " + msg.seqno
                + " from " + msg.sender.path().name() + " in view " + msg.viewId);
      }
    }
  }

  // TODO (solution) implement view flushing

  private void onViewChangeMsg(ViewChangeMsg msg) {

    System.out.println(
        getSelf().path().name() + " initiates view change " + this.epoch + "->" + msg.viewId
            + " " + this.proposedView + "->" + msg.proposedView);

    // check whether the node is in the view;
    // the message may have been caused by another node joining
    // and this one may not be part of the view yet
    if (!msg.proposedView.contains(getSelf()))
      return;

    // store the proposed view to begin transition
    proposedView.put(msg.viewId, new HashSet<>(msg.proposedView));

    // first, send all unstable messages (to the nodes in the new view)
    for (ChatMsg unstableMsg : unstableMsgSet) {
      boolean resend = unstableMsg.viewId == this.epoch;
      System.out.println(getSelf().path().name() + " may resend (" + resend + ") " + unstableMsg.seqno + " from "
          + unstableMsg.sender.path().name());
      if (resend) {
        multicast(unstableMsg, proposedView.get(msg.viewId));
      }
    }

    // then, multicast flush messages
    multicast(new ViewFlushMsg(msg.viewId - 1), proposedView.get(msg.viewId));

    // check if the node should crash
    if (nextCrash.name().equals(CrashType.ViewFlushMsg.name())) {
      crash();
    }

    // add self to already flushed for the previous view
    putInFlushes(msg.viewId - 1, getSelf());

    // check if all flushes were already received;
    // (ViewChangeMsg from the manager could be delayed wrt flushes)
    if (isViewFlushed(msg.viewId - 1)) {
      installView(msg.viewId);
    } else {

      // send message to self in order to timeout while waiting flushes
      getContext().system().scheduler().scheduleOnce(
          Duration.create(500, TimeUnit.MILLISECONDS), // how frequently generate them
          getSelf(), // destination actor reference
          new FlushTimeoutMsg(this.epoch), // the message to send
          getContext().system().dispatcher(), // system dispatcher
          getSelf() // source of the message (myself)
      );
    }
  }

  // TODO (solution) use the suggested flush messages (below) for view change

  private void onViewFlushMsg(ViewFlushMsg msg) {
    System.out
        .println(getSelf().path().name() + " adds flush of view " + msg.viewId + " from " + getSender().path().name());

    // add sender to flush map at the corresponding view ID
    putInFlushes(msg.viewId, getSender());

    // check if all flushed were received
    if (isViewFlushed(msg.viewId)) {
      installView(msg.viewId + 1);
    }
  }

  private void onFlushTimeoutMsg(FlushTimeoutMsg msg) {
    // System.out.println(getSelf().path().name() + " timeouts with current flushes
    // " + flushes);

    // check if there still are missing flushes
    if (!flushes.containsKey(msg.viewId))
      return;

    // find all nodes whose flush has not been received
    Set<ActorRef> crashed = new HashSet<>(proposedView.get(msg.viewId + 1));
    crashed.removeAll(flushes.get(msg.viewId));
    // coordinator.tell(new CrashReportMsg(crashed), getSelf());
  }

  private void onCrashMsg(CrashMsg msg) {
    if (msg.nextCrash == CrashType.NotResponding) {
      crash();
    } else {
      nextCrash = msg.nextCrash;
    }
  }

  private void onChangeReplicaSet(ChangeReplicaSet msg) {
    // initialize group
    replicas.clear();
    replicas.addAll(msg.group);

    // at the beginning, the view includes all nodes in the group
    currentView.addAll(replicas);
  }

  private void onRecoveryMsgBeforeCrash(RecoveryMsg msg) {
    System.out.println(getSelf().path().name() + " will recover immediately");
  }

  public void onCoordinatorHeartbeatPeriod(CoordinatorHeartbeatPeriod msg) {
    if (iscoordinator) {
      logger.debug("Coordinator sent out a heartbeat message to all replicas");
      Functions.setTimeout(getContext(), COORDINATOR_HEARTBEAT_PERIOD, getSelf(), new CoordinatorHeartbeatPeriod());
      CoordinatorHeartbeatMsg confAlive = new CoordinatorHeartbeatMsg();
      Functions.multicast(confAlive, replicas, getSelf());
      Functions.setTimeout(getContext(), COORDINATOR_HEARTBEAT_TIMEOUT, getSelf(), new HeartbeatTimeout());
    }
  }

  public void onHeartbeatMsg(HeartbeatMsg msg) {
    if (iscoordinator) {
      logger.debug("Coordinator got a heartbeat message from {}", Functions.getName(getSender()));
      replicasAlive.add(getSender());
    }
  }

  public void onHeartbeatTimeout(HeartbeatTimeout msg) {
    if (iscoordinator) {
      logger.debug("Coordinator reached it's heartbeat timeout");
      if (replicas.size() != replicasAlive.size()) {
        logger.warn("Some replicas did not respond. Initiating failure handling.");

        replicas.clear();
        replicas.addAll(replicasAlive);

        // contact all replicas with the new replicas Set of alive replicas
        Functions.multicast(new ChangeReplicaSet(replicas), replicas, coordinator);
      }
      replicasAlive.clear();
    }
  }

  private void onAck(AckMsg msg) {
    if (iscoordinator) {
      seqnoAckCounter.put(msg.seqno, seqnoAckCounter.getOrDefault(msg.seqno, 0) + 1);
      logger.info("Coordinator received {} ack(s) from {} with seqno {}", seqnoAckCounter.get(msg.seqno),
          Functions.getName(getSender()), msg.seqno);
      int Q = (replicas.size() / 2) + 1;

      if (seqnoAckCounter.get(msg.seqno) == Q) {
        logger.info("Coordinator confirm the update to all the replica");
        Functions.multicast(new WrOk(epoch, seqno, getSelf()), replicas, getSelf());
      }
    }
  }

  protected void crash() {
    logger.error("{} crashed", Functions.getName(getSelf()));
    getContext().become(crashed());
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
        .match(CoordinatorHeartbeatMsg.class, this::onCoordinatorHeartbeatMsg)
        .match(HeartbeatPeriod.class, this::onHeartbeatPeriod)
        .match(SendChatMsg.class, this::onSendChatMsg)
        .match(ChatMsg.class, this::onChatMsg)
        .match(WrOk.class, this::onWrOk)
        .match(StableChatMsg.class, this::onStableChatMsg)
        .match(StableTimeoutMsg.class, this::onStableTimeoutMsg)
        .match(ViewChangeMsg.class, this::onViewChangeMsg)
        .match(ViewFlushMsg.class, this::onViewFlushMsg)
        .match(FlushTimeoutMsg.class, this::onFlushTimeoutMsg)
        .match(CrashMsg.class, this::onCrashMsg)
        .match(ChangeReplicaSet.class, this::onChangeReplicaSet)
        .match(RecoveryMsg.class, this::onRecoveryMsgBeforeCrash)
        .match(CoordinatorHeartbeatPeriod.class, this::onCoordinatorHeartbeatPeriod)
        .match(HeartbeatMsg.class, this::onHeartbeatMsg)
        .match(AckMsg.class, this::onAck)
        .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
        .match(ElectionMsg.class, this::onElectionMsg)
        .match(CoordinatorMsg.class, this::onCoordinatorMsg)
        // .match(RecoveryMsg.class, msg -> System.out.println(getSelf().path().name() +
        // " ignoring RecoveryMsg"))
        .build();
  }

  final AbstractActor.Receive crashed() {
    return receiveBuilder()
        .match(ChatMsg.class, this::onCrashedChatMsg)
        .matchAny(msg -> {
        })
        // .matchAny(msg -> System.out.println(getSelf().path().name() + " ignoring " +
        // msg.getClass().getSimpleName() + " (crashed)"))
        .build();
  }
}

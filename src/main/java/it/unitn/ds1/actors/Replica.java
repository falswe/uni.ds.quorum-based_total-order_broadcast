package it.unitn.ds1.actors;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unitn.ds1.utils.Functions;
import it.unitn.ds1.utils.Functions.EpochSeqno;
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
  private final static int REPLICA_HEARTBEAT_TIMEOUT = Functions.DELAYTIME + 1000; // Timeout for the heartbeat from
                                                                                   // coordinator, ms
  private final static int EL_TIMEOUT = Functions.DELAYTIME * 2 + 100; // Timeout for the update from coordinator, ms
  private final static int EL_COORDINATOR_TIMEOUT = Functions.DELAYTIME * 2 + 100; // Timeout for the update from
                                                                                   // coordinator, ms
  // coordinator timeouts
  private final static int COORDINATOR_HEARTBEAT_PERIOD = 1000; // Time of the timeout for telling the replicas that the
                                                                // coordinator is alive
  private final static int COORDINATOR_HEARTBEAT_TIMEOUT = Functions.DELAYTIME + 100; // Time of the timeout for the
                                                                                      // response of the replicas

  // cancellable timeouts
  private static Cancellable cUpdateTimeout;
  private static Cancellable cWriteOkTimeout;
  private static Cancellable cReplicaHeartbeatTimeout;
  private static Cancellable cCoordinatorHearbeatPeriod;
  private static Cancellable cCoordinatorHearbeatTimeout;

  // used to start the hearbeat period timeout
  private boolean firstHeartbeatReceived;
  private boolean heartbeatReceived;

  private int electionAcksReceived;
  private int electionCoordinatorAcksReceived;

  // replica coordinator manager
  protected ActorRef coordinator;
  // the variable would be true if the replica is the coordinator, false otherwise
  private boolean iscoordinator;

  // participants (initial group, current and proposed views)
  protected List<ActorRef> replicas;
  private List<ActorRef> replicasAlive; // used by the coordinator

  // counters for timeouts
  private final Map<Map<ActorRef, Integer>, Boolean> membersUpdRcvd;
  private final Map<Map<Integer, Integer>, Boolean> AckRcvd;

  // coordinator counters for acknowledgement received
  private final Map<Integer, Integer> seqnoAckCounter;

  // list of epoch and sequence number related to the value communicated from the
  // coordinator
  private final Map<Map<Integer, Integer>, Integer> epochSeqnoValue;

  // type of the next simulated crash
  public enum CrashType {
    NONE,
    NotResponding, // the replica crashes, and does not respond
    WhileSendingUpdate,
    AfterReceivingUpdate,
    WhileSendingWriteOk,
    WhileElection,
    WhileChoosingCoordinator,
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
    this.firstHeartbeatReceived = false;
    this.heartbeatReceived = false;
    this.electionAcksReceived = 0;
    this.electionCoordinatorAcksReceived = 0;
    this.coordinator = getSelf();
    this.iscoordinator = true;
    this.replicas = new ArrayList<>();
    this.replicasAlive = new ArrayList<>();
    this.membersUpdRcvd = new HashMap<>();
    this.AckRcvd = new HashMap<>();
    this.seqnoAckCounter = new HashMap<>();
    this.epochSeqnoValue = new HashMap<>();
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
    this.firstHeartbeatReceived = false;
    this.heartbeatReceived = false;
    this.electionAcksReceived = 0;
    this.electionCoordinatorAcksReceived = 0;
    this.coordinator = coordinator;
    this.iscoordinator = false;
    this.replicas = new ArrayList<>();
    this.replicasAlive = new ArrayList<>();
    this.membersUpdRcvd = new HashMap<>();
    this.AckRcvd = new HashMap<>();
    this.seqnoAckCounter = new HashMap<>();
    this.epochSeqnoValue = new HashMap<>();
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

  protected void onRdRqMsg(RdRqMsg msg) {
    logger.info("{} received read request from client {}", Functions.getName(getSelf()),
        Functions.getId(getSender()));
    Functions.tellDelay(new RdRspMsg(value), getSelf(), getSender());
    // getSender().tell(new RdRspMsg(value), getSelf());
  }

  private void onWrRqMsg(WrRqMsg msg) {
    if (!iscoordinator) {
      logger.info("{} received write request from client {} with value {}", Functions.getName(getSelf()),
          Functions.getId(getSender()),
          msg.new_value);
      Functions.tellDelay(msg, getSelf(), coordinator);
      // coordinator.tell(msg, getSelf());

      cUpdateTimeout = Functions.setTimeout(getContext(), UPD_TIMEOUT, getSelf(),
          new UpdTimeout(msg.c_snd, msg.op_cnt));
    } else {
      logger.info("{} received write request from {} with value {}", Functions.getName(getSelf()),
          Functions.getName(getSender()),
          msg.new_value);
      if (nextCrash == CrashType.WhileSendingUpdate) {
        Random rnd = new Random();
        // TODO: This is forced, cover other cases as well.
        // int start = rnd.nextInt(replicas.size() - 1);
        // int end = start + rnd.nextInt(replicas.size() - start - 1);
        Functions.multicast(new UpdRqMsg(msg.c_snd, epoch, ++seqno, msg.op_cnt, msg.new_value),
            replicas.subList(0, 2), getSelf());
        crash();
      } else {
        Functions.multicast(new UpdRqMsg(msg.c_snd, epoch, ++seqno, msg.op_cnt, msg.new_value), replicas, getSelf());
      }
    }
  }

  protected void onUpdRqMsg(UpdRqMsg msg) {
    logger.info("{} received UPDATE message from coordinator with value {} with seqno {}",
        Functions.getName(getSelf()), msg.value, msg.seqno);
    Map<Integer, Integer> epochSeqno = new HashMap<>(msg.epoch, msg.seqno);
    epochSeqnoValue.put(epochSeqno, msg.value);

    Map<ActorRef, Integer> m = Map.of(msg.sender, msg.op_cnt);
    membersUpdRcvd.put(m, true);

    Functions.tellDelay(new AckMsg(msg.epoch, msg.seqno, msg.sender), getSelf(), coordinator);
    // coordinator.tell(new AckMsg(msg.epoch, msg.seqno, msg.sender), getSelf());

    cWriteOkTimeout = Functions.setTimeout(getContext(), WRITEOK_TIMEOUT, getSelf(),
        new AckTimeout(msg.epoch, msg.seqno));
  }

  protected void onWrOk(WrOk msg) {
    this.epoch = msg.epoch;
    this.seqno = msg.seqno;
    Map<Integer, Integer> epochSeqno = new HashMap<>(msg.epoch, msg.seqno);
    value = epochSeqnoValue.get(epochSeqno);
    AckRcvd.put(epochSeqno, true);

    logger.info("Replica {} update {}:{} {}", Functions.getId(getSelf()), epoch, seqno, value);
  }

  protected void onUpdTimeout(UpdTimeout msg) {
    logger.debug("{} reached it's update timeout", Functions.getName(getSelf()));

    Map<ActorRef, Integer> m = Map.of(msg.c_snd, msg.op_cnt);
    if (!membersUpdRcvd.getOrDefault(m, false)) {
      logger.error("Replica {} did not receive update in time. Coordinator might have crashed.",
          Functions.getId(getSelf()));

      Map<ActorRef, EpochSeqno> candidates = new HashMap<ActorRef, EpochSeqno>();
      coordinatorCrashRecovery(coordinator, candidates);
    }
  }

  protected void onAckTimeout(AckTimeout msg) {
    logger.debug("{} reached it's write_ok timeout", Functions.getName(getSelf()));

    Map<Integer, Integer> epochSeqno = new HashMap<>(msg.epoch, msg.seqno);
    if (!AckRcvd.getOrDefault(epochSeqno, false)) {
      logger.error("Replica {} did not receive write acknowledgment in time. Coordinator might have crashed.",
          Functions.getId(getSelf()));

      Map<ActorRef, EpochSeqno> candidates = new HashMap<ActorRef, EpochSeqno>();
      coordinatorCrashRecovery(coordinator, candidates);
    }
  }

  protected void onCoordinatorHeartbeatMsg(CoordinatorHeartbeatMsg msg) {
    if (!firstHeartbeatReceived) {
      firstHeartbeatReceived = true;
      cReplicaHeartbeatTimeout = Functions.setTimeout(getContext(), REPLICA_HEARTBEAT_TIMEOUT, getSelf(),
          new HeartbeatPeriod());
    }

    logger.debug("{} received a heartbeat message from the coordinator", Functions.getName(getSelf()));
    heartbeatReceived = true;

    Functions.tellDelay(new HeartbeatMsg(), getSelf(), getSender());
    // getSender().tell(new HeartbeatMsg(), getSelf());
  }

  protected void onHeartbeatPeriod(HeartbeatPeriod msg) {
    logger.debug("{} reached it's heartbeat timeout", Functions.getName(getSelf()));

    if (!heartbeatReceived) {
      logger.error("{} did not heartbeat in time. Coordinator ({}) might have crashed.", Functions.getName(getSelf()),
          Functions.getName(coordinator));

      Map<ActorRef, EpochSeqno> candidates = new HashMap<ActorRef, EpochSeqno>();
      coordinatorCrashRecovery(coordinator, candidates);
    }
    heartbeatReceived = false;
    cReplicaHeartbeatTimeout = Functions.setTimeout(getContext(), REPLICA_HEARTBEAT_TIMEOUT, getSelf(),
        new HeartbeatPeriod());
  }

  private void onJoinGroupMsg(JoinGroupMsg msg) {
    // initialize group
    replicas.addAll(msg.group);

    // at the beginning, the view includes all nodes in the group
    currentView.addAll(replicas);
  }

  private void coordinatorCrashRecovery(ActorRef crashed_c, Map<ActorRef, EpochSeqno> candidates) {
    replicas.remove(crashed_c);

    // if (cReplicaHeartbeatTimeout != null)
    //   cReplicaHeartbeatTimeout.cancel();
    // if (cUpdateTimeout != null)
    //   cUpdateTimeout.cancel();
    // if (cWriteOkTimeout != null)
    //   cWriteOkTimeout.cancel();

    int selfIndex = replicas.indexOf(getSelf());
    int nextIndex = (selfIndex + 1) % replicas.size();

    EpochSeqno actorEpochSeqno = new EpochSeqno(epoch, seqno);
    candidates.put(getSelf(), actorEpochSeqno);
    logger.info("{} sending an election message to {}, {} was removed", Functions.getName(getSelf()),
        Functions.getName(replicas.get(nextIndex)), Functions.getName(coordinator));
    Functions.tellDelay(new ElectionMsg(crashed_c, candidates), getSelf(), replicas.get(nextIndex));
    Functions.setTimeout(getContext(), EL_TIMEOUT, getSelf(),
        new ElectionAckTimeout(replicas.get(nextIndex), coordinator, candidates));
    // replicas.get(nextIndex).tell(new ElectionMsg(candidates), getSelf());
  }

  private void electCoordinator(Map<ActorRef, EpochSeqno> candidates) {
    int maxEpoch = epoch;
    int maxSeqno = seqno;
    int maxId = 0;

    boolean incompleteBroadcast = false;

    for (ActorRef actor : candidates.keySet()) {
      if (candidates.get(actor).epoch > maxEpoch) {
        coordinator = actor;

        maxEpoch = candidates.get(actor).epoch;
        maxSeqno = candidates.get(actor).seqno;
        maxId = Functions.getId(actor);

        incompleteBroadcast = true;
      } else if (candidates.get(actor).epoch == maxEpoch) {
        if (candidates.get(actor).seqno > maxSeqno) {
          coordinator = actor;

          maxSeqno = candidates.get(actor).seqno;
          maxId = Functions.getId(actor);
        } else if (candidates.get(actor).seqno == maxSeqno) {
          if (Functions.getId(actor) > maxId) {
            coordinator = actor;

            maxId = Functions.getId(actor);
          }
        } else {
          incompleteBroadcast = true;
        }
      } else {
        incompleteBroadcast = true;
      }
    }

    if (coordinator == getSelf()) {
      // TO DO: become coordinator
      iscoordinator = true;
      epoch++;
      onCoordinatorHeartbeatPeriod(new CoordinatorHeartbeatPeriod());
      logger.info("{} [e: {}, sn: {}]: the new coordinator is me, replica-size {}", Functions.getName(getSelf()), epoch,
          seqno, replicas.size());

      if (incompleteBroadcast)
        Functions.multicast(new UpdRqMsg(getSelf(), epoch, ++seqno, 0, value), replicas, getSelf());
    } else {
      logger.info("{} [e: {}, sn: {}]: the new coordinator is {}, replica-size {}", Functions.getName(getSelf()), epoch,
          seqno, Functions.getName(coordinator), replicas.size());
    }

    int selfIndex = replicas.indexOf(getSelf());
    int nextIndex = (selfIndex + 1) % replicas.size();
    logger.info("{} sending coordinator message to {}", Functions.getName(getSelf()),
        Functions.getName(replicas.get(nextIndex)));
    Functions.tellDelay(new CoordinatorMsg(candidates), getSelf(), replicas.get(nextIndex));
    Functions.setTimeout(getContext(), EL_COORDINATOR_TIMEOUT, getSelf(),
        new CoordinatorAckTimeout(replicas.get(nextIndex), candidates));
    // replicas.get(nextIndex).tell(new CoordinatorMsg(candidates), getSelf());
  }

  private void onElectionMsg(ElectionMsg msg) {
    if (nextCrash == CrashType.WhileElection) {
      crash();
      return;
    }
    logger.info("{} receiving an election message from {}", Functions.getName(getSelf()),
        Functions.getName(getSender()));
    Functions.tellDelay(new ElectionMsgAck(), getSelf(), getSender());
    if (msg.coordinatorCandidates.containsKey(getSelf())) {
      // if we are on the coordinator candidates list, send coordinator message
      electCoordinator(msg.coordinatorCandidates);
    } else {
      // otherwise keep passing the election message
      coordinatorCrashRecovery(msg.crashedCoordinator, msg.coordinatorCandidates);
    }
  }

  private void onElectionMsgAck(ElectionMsgAck msg) {
    electionAcksReceived++;
    logger.debug("{} received election msg ACK from {}", Functions.getName(getSelf()), Functions.getName(getSender()));
  }

  private void onCoordinatorMsg(CoordinatorMsg msg) {
    if (nextCrash == CrashType.WhileChoosingCoordinator) {
      crash();
      return;
    }
    Functions.tellDelay(new CoordinatorMsgAck(), getSelf(), getSender());
    if (!replicas.contains(coordinator))
      electCoordinator(msg.coordinatorCandidates);
  }

  private void onCoordinatorMsgAck(CoordinatorMsgAck msg) {
    electionCoordinatorAcksReceived++;
  }

  private void onElectionAckTimeout(ElectionAckTimeout msg) {
    if (electionAcksReceived == 0) {
      logger.debug("{} has not received election msg ACK, remove {}", Functions.getName(getSelf()),
          Functions.getName(msg.nextReplica));
      replicas.remove(msg.nextReplica);

      int selfIndex = replicas.indexOf(getSelf());
      int nextIndex = (selfIndex + 1) % replicas.size();
      logger.info("{} sending an election message to {}, {} was removed", Functions.getName(getSelf()),
          Functions.getName(replicas.get(nextIndex)), Functions.getName(coordinator));
      Functions.tellDelay(new ElectionMsg(msg.crashed_c, msg.coordinatorCandidates), getSelf(),
          replicas.get(nextIndex));
      Functions.setTimeout(getContext(), EL_TIMEOUT, getSelf(),
          new ElectionAckTimeout(replicas.get(nextIndex), coordinator, msg.coordinatorCandidates));
    } else {
      electionAcksReceived--;
    }
  }

  private void onCoordinatorAckTimeout(CoordinatorAckTimeout msg) {
    if (electionCoordinatorAcksReceived == 0) {
      replicas.remove(msg.nextReplica);

      int selfIndex = replicas.indexOf(getSelf());
      int nextIndex = (selfIndex + 1) % replicas.size();
      logger.info("{} sending coordinator message to {}", Functions.getName(getSelf()),
          Functions.getName(replicas.get(nextIndex)));
      Functions.tellDelay(new CoordinatorMsg(msg.coordinatorCandidates), getSelf(), replicas.get(nextIndex));
      Functions.setTimeout(getContext(), EL_COORDINATOR_TIMEOUT, getSelf(),
          new CoordinatorAckTimeout(replicas.get(nextIndex), msg.coordinatorCandidates));
    } else {
      electionCoordinatorAcksReceived--;
    }
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

  public void onCoordinatorHeartbeatPeriod(CoordinatorHeartbeatPeriod msg) {
    if (iscoordinator) {
      logger.debug("{} sent out a heartbeat message to all replicas", Functions.getName(getSelf()));
      Functions.setTimeout(getContext(), COORDINATOR_HEARTBEAT_PERIOD, getSelf(), new CoordinatorHeartbeatPeriod());
      CoordinatorHeartbeatMsg confAlive = new CoordinatorHeartbeatMsg();
      Functions.multicast(confAlive, replicas, getSelf());
      Functions.setTimeout(getContext(), COORDINATOR_HEARTBEAT_TIMEOUT, getSelf(), new HeartbeatTimeout());
    }
  }

  public void onHeartbeatMsg(HeartbeatMsg msg) {
    if (iscoordinator) {
      logger.debug("{} got a heartbeat message from {}", Functions.getName(getSelf()), Functions.getName(getSender()));
      replicasAlive.add(getSender());
    }
  }

  public void onHeartbeatTimeout(HeartbeatTimeout msg) {
    if (iscoordinator) {
      logger.debug("{} reached it's heartbeat timeout", Functions.getName(getSelf()));
      if (replicas.size() != replicasAlive.size()) {
        logger.warn("replicas did not respond. Initiating failure handling.");
        for (ActorRef r : replicasAlive)
          logger.warn("{} is alive", Functions.getName(r));

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
      logger.info("{} received {} ack(s) from {} with seqno {}", Functions.getName(getSelf()),
          seqnoAckCounter.get(msg.seqno),
          Functions.getName(getSender()), msg.seqno);
      int Q = (replicas.size() / 2) + 1;

      if (seqnoAckCounter.get(msg.seqno) == Q) {
        logger.info("{} confirm the update to all the replica", Functions.getName(getSelf()));
        if (nextCrash == CrashType.WhileSendingWriteOk) {
          Random rnd = new Random();
          // TODO: This is forced, cover other cases as well.
          // int start = rnd.nextInt(replicas.size() - 1);
          // int end = start + rnd.nextInt(replicas.size() - start - 1);
          Functions.multicast(new WrOk(msg.epoch, msg.seqno, getSelf()), replicas.subList(0, 2), getSelf());
          crash();
        } else {
          Functions.multicast(new WrOk(msg.epoch, msg.seqno, getSelf()), replicas, getSelf());
        }
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
        .match(WrOk.class, this::onWrOk)
        .match(CrashMsg.class, this::onCrashMsg)
        .match(ChangeReplicaSet.class, this::onChangeReplicaSet)
        .match(CoordinatorHeartbeatPeriod.class, this::onCoordinatorHeartbeatPeriod)
        .match(HeartbeatMsg.class, this::onHeartbeatMsg)
        .match(AckMsg.class, this::onAck)
        .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
        .match(ElectionMsg.class, this::onElectionMsg)
        .match(ElectionMsgAck.class, this::onElectionMsgAck)
        .match(ElectionAckTimeout.class, this::onElectionAckTimeout)
        .match(CoordinatorMsg.class, this::onCoordinatorMsg)
        .match(CoordinatorMsgAck.class, this::onCoordinatorMsgAck)
        .match(CoordinatorAckTimeout.class, this::onCoordinatorAckTimeout)
        .build();
  }

  final AbstractActor.Receive crashed() {
    return receiveBuilder()
        .matchAny(msg -> {
        })
        .build();
  }
}

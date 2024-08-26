package it.unitn.ds1.actors;

import akka.actor.*;

import java.util.*;

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
  private final static int UPD_TIMEOUT = 1000;                                     // Timeout for the update from coordinator, ms
  private final static int WRITEOK_TIMEOUT = 1000;                                 // Timeout for the writeok from coordinator, ms
  private final static int REPLICA_HEARTBEAT_PERIOD = Functions.DELAYTIME + 2000;  // Timeout for the heartbeat from
                                                                                   // coordinator, ms
  private final static int EL_TIMEOUT = Functions.DELAYTIME * 2 + 100;             // Timeout for the update from coordinator, ms
  private final static int EL_COORDINATOR_TIMEOUT = Functions.DELAYTIME * 2 + 100; // Timeout for the update from
                                                                                   // coordinator, ms
  private final static int RESTART_REPLICA_HEARTBEAT_PERIOD = 5000;
  
  // coordinator timeouts
  private final static int COORDINATOR_HEARTBEAT_PERIOD = 1000;                       // Time of the timeout for telling the replicas that the
                                                                                      // coordinator is alive
  private final static int COORDINATOR_HEARTBEAT_TIMEOUT = Functions.DELAYTIME + 500; // Time of the timeout for the
                                                                                      // response of the replicas

  // cancellable timeouts
  private Cancellable cReplicaHeartbeatPeriod;

  // used to start the hearbeat period timeout
  private boolean firstHeartbeatReceived;
  private boolean heartbeatReceived;

  private Map<ActorRef, Integer> electionAcksReceived;
  private Map<ActorRef, Integer> electionCoordinatorAcksReceived;

  // replica coordinator manager
  protected ActorRef coordinator;
  // the variable would be true if the replica is the coordinator, false otherwise
  private boolean iscoordinator;

  // participants (initial group, current and proposed views)
  protected List<ActorRef> replicas;
  private List<ActorRef> replicasAlive; // used by the coordinator

  // counters for timeouts
  private final Map<Map<ActorRef, Integer>, Boolean> membersUpdRcvd;
  private final Map<EpochSeqno, Boolean> AckRcvd;

  // coordinator counters for acknowledgement received
  private final Map<EpochSeqno, Integer> epochSeqnoAckCounter;
  private final Map<EpochSeqno, Boolean> epochSeqnoConfirmUpdate;

  // list of epoch and sequence number related to the value communicated from the
  // coordinator
  private final Map<EpochSeqno, Integer> epochSeqnoValue;

  // type of the next simulated crash
  public enum CrashType {
    NONE,
    NotResponding, // the replica crashes, and does not respond
    WhileSendingUpdate,
    AfterReceivingUpdate,
    WhileSendingWriteOk,
    WhileElection,
    WhileChoosingCoordinator
  }

  protected CrashType nextCrash;

  /*-- Actor constructors --------------------------------------------------- */
  public Replica() {
    this.seqno = 0;
    this.epoch = 0;
    this.firstHeartbeatReceived = false;
    this.heartbeatReceived = false;
    this.electionAcksReceived = new HashMap<>();
    this.electionCoordinatorAcksReceived = new HashMap<>();
    this.coordinator = getSelf();
    this.iscoordinator = true;
    this.replicas = new ArrayList<>();
    this.replicasAlive = new ArrayList<>();
    this.membersUpdRcvd = new HashMap<>();
    this.AckRcvd = new HashMap<>();
    this.epochSeqnoAckCounter = new HashMap<>();
    this.epochSeqnoConfirmUpdate = new HashMap<>();
    this.epochSeqnoValue = new HashMap<>();
    this.nextCrash = CrashType.NONE;
  }

  public Replica(ActorRef coordinator) {
    this.seqno = 0;
    this.epoch = 0;
    this.firstHeartbeatReceived = false;
    this.heartbeatReceived = false;
    this.electionAcksReceived = new HashMap<>();
    this.electionCoordinatorAcksReceived = new HashMap<>();
    this.coordinator = coordinator;
    this.iscoordinator = false;
    this.replicas = new ArrayList<>();
    this.replicasAlive = new ArrayList<>();
    this.membersUpdRcvd = new HashMap<>();
    this.AckRcvd = new HashMap<>();
    this.epochSeqnoAckCounter = new HashMap<>();
    this.epochSeqnoConfirmUpdate = new HashMap<>();
    this.epochSeqnoValue = new HashMap<>();
    this.nextCrash = CrashType.NONE;
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

      Functions.setTimeout(getContext(), UPD_TIMEOUT, getSelf(),
          new UpdTimeout(msg.c_snd, msg.op_cnt));
    } else {
      logger.info("{} received write request from {} with value {}", Functions.getName(getSelf()),
          Functions.getName(getSender()),
          msg.new_value);
      if (nextCrash == CrashType.WhileSendingUpdate) {
        Random rnd = new Random();
        int omittedReplicaIndex = rnd.nextInt(replicas.size());
        seqno++;
        for (int i = 0; i < replicas.size(); i++) {
          if (i != omittedReplicaIndex) {
            Functions.tellDelay(new UpdRqMsg(msg.c_snd, epoch, seqno, msg.op_cnt, msg.new_value), getSelf(),
                replicas.get(i));
          }
        }
        crash();
      } else {
        Functions.multicast(new UpdRqMsg(msg.c_snd, epoch, ++seqno, msg.op_cnt, msg.new_value), replicas, getSelf());
      }
    }
  }

  protected void onUpdRqMsg(UpdRqMsg msg) {
    logger.debug("{} received UPDATE message from coordinator with value {} of epoch {} and seqno {}",
        Functions.getName(getSelf()), msg.value, msg.epoch, msg.seqno);
    EpochSeqno epochSeqno = new EpochSeqno(msg.epoch, msg.seqno);
    epochSeqnoValue.put(epochSeqno, msg.value);

    Map<ActorRef, Integer> m = Map.of(msg.sender, msg.op_cnt);
    membersUpdRcvd.put(m, true);

    Functions.tellDelay(new AckMsg(msg.epoch, msg.seqno, msg.sender), getSelf(), getSender());
    // coordinator.tell(new AckMsg(msg.epoch, msg.seqno, msg.sender), getSelf());

    Functions.setTimeout(getContext(), WRITEOK_TIMEOUT, getSelf(),
        new AckTimeout(msg.epoch, msg.seqno));

    if (nextCrash == CrashType.AfterReceivingUpdate)
      crash();
  }

  protected void onWrOk(WrOk msg) {
    this.epoch = msg.epoch;
    this.seqno = msg.seqno;
    EpochSeqno epochSeqno = new EpochSeqno(msg.epoch, msg.seqno);
    value = epochSeqnoValue.get(epochSeqno);
    AckRcvd.put(epochSeqno, true);

    logger.info("Replica {} update {}:{} {}", Functions.getId(getSelf()), epoch, seqno, value);
  }

  protected void onUpdTimeout(UpdTimeout msg) {
    Map<ActorRef, Integer> m = Map.of(msg.c_snd, msg.op_cnt);
    if (!membersUpdRcvd.getOrDefault(m, false)) {
      logger.error("Replica {} did not receive update in time. Coordinator might have crashed.",
          Functions.getId(getSelf()));

      Map<ActorRef, EpochSeqno> candidates = new HashMap<>();
      coordinatorCrashRecovery(coordinator, candidates);
    }
  }

  protected void onAckTimeout(AckTimeout msg) {
    EpochSeqno epochSeqno = new EpochSeqno(msg.epoch, msg.seqno);
    if (!AckRcvd.getOrDefault(epochSeqno, false)) {
      logger.error("Replica {} did not receive write acknowledgment in time. Coordinator might have crashed.",
          Functions.getId(getSelf()));

      Map<ActorRef, EpochSeqno> candidates = new HashMap<>();
      coordinatorCrashRecovery(coordinator, candidates);
    }
  }

  protected void onCoordinatorHeartbeatMsg(CoordinatorHeartbeatMsg msg) {
    if (!firstHeartbeatReceived) {
      firstHeartbeatReceived = true;
      cReplicaHeartbeatPeriod = Functions.setTimeout(getContext(), REPLICA_HEARTBEAT_PERIOD, getSelf(),
          new HeartbeatPeriod());
      logger.debug("Heartbeat period {}: Created first", Functions.getName(getSelf()));
    }

    logger.debug("{} received a heartbeat message from the coordinator", Functions.getName(getSelf()));
    heartbeatReceived = true;

    Functions.tellDelay(new HeartbeatMsg(), getSelf(), getSender());
    // getSender().tell(new HeartbeatMsg(), getSelf());
  }

  protected void onHeartbeatPeriod(HeartbeatPeriod msg) {
    logger.debug("{} Heartbeat Period reached", Functions.getName(getSelf()));
    if (!heartbeatReceived) {
      logger.error("{} did not heartbeat in time. Coordinator ({}) might have crashed.", Functions.getName(getSelf()),
          Functions.getName(coordinator));

      Map<ActorRef, EpochSeqno> candidates = new HashMap<>();
      coordinatorCrashRecovery(coordinator, candidates);
    }
    heartbeatReceived = false;
    cReplicaHeartbeatPeriod = Functions.setTimeout(getContext(), REPLICA_HEARTBEAT_PERIOD, getSelf(),
        new HeartbeatPeriod());
    logger.debug("heartbeat period {}: created new one", Functions.getName(getSelf()));
  }

  private void onJoinGroupMsg(JoinGroupMsg msg) {
    // initialize group
    replicas.addAll(msg.group);
  }

  private void coordinatorCrashRecovery(ActorRef crashed_c, Map<ActorRef, EpochSeqno> candidates) {
    replicas.remove(crashed_c);

    if (!cReplicaHeartbeatPeriod.isCancelled())
    {
      cReplicaHeartbeatPeriod.cancel();
      logger.debug("Heartbeat period {}: stopped", Functions.getName(getSelf()));
    }
    // if (cUpdateTimeout != null)
    // cUpdateTimeout.cancel();
    // if (cWriteOkTimeout != null)
    // cWriteOkTimeout.cancel();
    int selfIndex = replicas.indexOf(getSelf());
    int nextIndex = (selfIndex + 1) % replicas.size();

    EpochSeqno actorEpochSeqno = new EpochSeqno(epoch, seqno);
    candidates.put(getSelf(), actorEpochSeqno);
    logger.debug("{} sending an election message to {}, {} was removed", Functions.getName(getSelf()),
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

    if (coordinator == getSelf() && !iscoordinator) {
      iscoordinator = true;
      epoch++;
      seqno = 0;
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
    logger.debug("{} sending coordinator message to {}", Functions.getName(getSelf()),
        Functions.getName(replicas.get(nextIndex)));
    Functions.tellDelay(new CoordinatorMsg(candidates), getSelf(), replicas.get(nextIndex));
    Functions.setTimeout(getContext(), EL_COORDINATOR_TIMEOUT, getSelf(),
        new CoordinatorAckTimeout(replicas.get(nextIndex), candidates));
    if(cReplicaHeartbeatPeriod.isCancelled())
      cReplicaHeartbeatPeriod = Functions.setTimeout(getContext(), RESTART_REPLICA_HEARTBEAT_PERIOD, getSelf(),
        new HeartbeatPeriod());
    logger.debug("Heartbeat period {}: created new one", Functions.getName(getSelf()));
    // replicas.get(nextIndex).tell(new CoordinatorMsg(candidates), getSelf());
  }

  private void onElectionMsg(ElectionMsg msg) {
    if (nextCrash == CrashType.WhileElection) {
      crash();
      return;
    }
    logger.debug("{} receiving an election message from {}", Functions.getName(getSelf()),
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
    int acksReceived = electionAcksReceived.getOrDefault(getSender(), 0) + 1;
    electionAcksReceived.put(getSender(), acksReceived);

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
    int ackReceived = electionCoordinatorAcksReceived.getOrDefault(getSender(), 0) + 1;
    electionCoordinatorAcksReceived.put(getSender(), ackReceived);

    logger.debug("{} received coordinator msg ACK from {}", Functions.getName(getSelf()),
        Functions.getName(getSender()));

  }

  private void onElectionAckTimeout(ElectionAckTimeout msg) {
    if (electionAcksReceived.getOrDefault(msg.nextReplica, 0) == 0) {
      logger.debug("{} has not received election msg ACK, remove {}", Functions.getName(getSelf()),
          Functions.getName(msg.nextReplica));
      replicas.remove(msg.nextReplica);

      int selfIndex = replicas.indexOf(getSelf());
      int nextIndex = (selfIndex + 1) % replicas.size();
      logger.debug("{} sending an election message to {}, {} was removed", Functions.getName(getSelf()),
          Functions.getName(replicas.get(nextIndex)), Functions.getName(coordinator));
      Functions.tellDelay(new ElectionMsg(msg.crashed_c, msg.coordinatorCandidates), getSelf(),
          replicas.get(nextIndex));
      Functions.setTimeout(getContext(), EL_TIMEOUT, getSelf(),
          new ElectionAckTimeout(replicas.get(nextIndex), coordinator, msg.coordinatorCandidates));
    } else {
      int ackReceived = electionAcksReceived.get(msg.nextReplica) - 1;
      electionAcksReceived.put(msg.nextReplica, ackReceived);
    }
  }

  private void onCoordinatorAckTimeout(CoordinatorAckTimeout msg) {
    if (electionCoordinatorAcksReceived.getOrDefault(msg.nextReplica, 0) == 0) {
      logger.debug("{} has not received coordinator msg ACK, remove {}", Functions.getName(getSelf()),
          Functions.getName(msg.nextReplica));
      replicas.remove(msg.nextReplica);

      int selfIndex = replicas.indexOf(getSelf());
      int nextIndex = (selfIndex + 1) % replicas.size();
      logger.debug("{} sending coordinator message to {}", Functions.getName(getSelf()),
          Functions.getName(replicas.get(nextIndex)));
      Functions.tellDelay(new CoordinatorMsg(msg.coordinatorCandidates), getSelf(), replicas.get(nextIndex));
      Functions.setTimeout(getContext(), EL_COORDINATOR_TIMEOUT, getSelf(),
          new CoordinatorAckTimeout(replicas.get(nextIndex), msg.coordinatorCandidates));
    } else {
      int ackReceived = electionCoordinatorAcksReceived.get(msg.nextReplica) - 1;
      electionCoordinatorAcksReceived.put(msg.nextReplica, ackReceived);
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
  }

  public void onCoordinatorHeartbeatPeriod(CoordinatorHeartbeatPeriod msg) {
    if (iscoordinator) {
      logger.debug("{} sent out a heartbeat message to all replicas", Functions.getName(getSelf()));
      Functions.setTimeout(getContext(), COORDINATOR_HEARTBEAT_PERIOD, getSelf(), new CoordinatorHeartbeatPeriod());
      CoordinatorHeartbeatMsg confAlive = new CoordinatorHeartbeatMsg();
      Functions.multicast(confAlive, replicas, getSelf());
      Functions.setTimeout(getContext(), COORDINATOR_HEARTBEAT_TIMEOUT, getSelf(), new CoordinatorHeartbeatTimeout());
    }
  }

  public void onHeartbeatMsg(HeartbeatMsg msg) {
    if (iscoordinator) {
      logger.debug("{} got a heartbeat message from {}", Functions.getName(getSelf()), Functions.getName(getSender()));
      replicasAlive.add(getSender());
    }
  }

  public void onCoordinatorHeartbeatTimeout(CoordinatorHeartbeatTimeout msg) {
    if (iscoordinator) {
      if (replicas.size() != replicasAlive.size()) {
        logger.warn("replicas did not respond. Initiating failure handling.");
        List<ActorRef> deadReplicas = new ArrayList<>();
        for (ActorRef r : replicas) {
          if (!replicasAlive.contains(r))
          {
            deadReplicas.add(r);
            logger.warn("{} is dead", Functions.getName(r));
          }
          else
            logger.debug("{} is alive", Functions.getName(r));
        }
        replicas.removeAll(deadReplicas);


        // contact all replicas with the new replicas Set of alive replicas
        Functions.multicast(new ChangeReplicaSet(replicas), replicas, coordinator);
      }
      replicasAlive.clear();
    }
  }

  private void onAck(AckMsg msg) {
    if (iscoordinator) {
      EpochSeqno es = new EpochSeqno(msg.epoch, msg.seqno);
      int counter = epochSeqnoAckCounter.getOrDefault(es, 0) + 1;
      epochSeqnoAckCounter.put(es, counter);
      logger.debug("{} received {} ack(s) from {} of epoch {} seqno {}", Functions.getName(getSelf()),
          epochSeqnoAckCounter.get(es),
          Functions.getName(getSender()), msg.epoch, msg.seqno);
      int Q = (replicas.size() / 2) + 1;

      if (epochSeqnoAckCounter.get(es) >= Q && epochSeqnoConfirmUpdate.getOrDefault(es, true)) {
        logger.debug("{} confirm the update to all the replica", Functions.getName(getSelf()));
        if (nextCrash == CrashType.WhileSendingWriteOk) {
          //Random rnd = new Random();
          //int omittedReplicaIndex = rnd.nextInt(replicas.size() - 1);
          for (int i = 0; i < replicas.size() - 2; i++) {
            //if (i != omittedReplicaIndex) {
              replicas.get(i).tell(new WrOk(msg.epoch, msg.seqno, getSelf()), getSelf());
            //}
          }
          crash();
        } else {
          Functions.multicast(new WrOk(msg.epoch, msg.seqno, getSelf()), replicas, getSelf());
        }
        epochSeqnoConfirmUpdate.put(es, false);
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
        .match(CoordinatorHeartbeatTimeout.class, this::onCoordinatorHeartbeatTimeout)
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

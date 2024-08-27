package it.unitn.ds1.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unitn.ds1.utils.Functions;
import it.unitn.ds1.utils.Functions.EpochSeqno;
import it.unitn.ds1.utils.Messages;

public class Replica extends AbstractActor {
  private static final Logger logger = LoggerFactory.getLogger(Replica.class);

  // Timeouts
  private static final int UPDATE_TIMEOUT = 1000;
  private static final int WRITE_OK_TIMEOUT = 1000;
  private static final int REPLICA_HEARTBEAT_PERIOD = Functions.DELAY_TIME + 2000;
  private static final int RESTART_REPLICA_HEARTBEAT_PERIOD = 5000;
  private static final int ELECTION_TIMEOUT = Functions.DELAY_TIME * 2 + 100;
  private static final int COORDINATOR_ELECTION_TIMEOUT = Functions.DELAY_TIME * 2 + 100;
  private static final int COORDINATOR_HEARTBEAT_PERIOD = 1000;
  private static final int COORDINATOR_HEARTBEAT_TIMEOUT = Functions.DELAY_TIME + 500;

  private int value = 0;
  private int epoch = 0;
  private int seqno = 0;

  private boolean firstHeartbeatReceived = false;
  private boolean heartbeatReceived = false;

  private final Map<ActorRef, Integer> electionAcksReceived = new HashMap<>();
  private final Map<ActorRef, Integer> electionCoordinatorAcksReceived = new HashMap<>();

  private ActorRef coordinator;
  private boolean isCoordinator;

  private final List<ActorRef> replicas = new ArrayList<>();
  private final List<ActorRef> replicasAlive = new ArrayList<>();

  private final Map<Map<ActorRef, Integer>, Boolean> membersUpdRcvd = new HashMap<>();
  private final Map<EpochSeqno, Boolean> ackRcvd = new HashMap<>();

  private final Map<EpochSeqno, Integer> epochSeqnoAckCounter = new HashMap<>();
  private final Map<EpochSeqno, Boolean> epochSeqnoConfirmUpdate = new HashMap<>();

  private final Map<EpochSeqno, Integer> epochSeqnoValue = new HashMap<>();

  private Cancellable replicaHeartbeatPeriod;

  private Messages.CrashType nextCrash = Messages.CrashType.NONE;

  public Replica() {
    this.coordinator = getSelf();
    this.isCoordinator = true;
  }

  public Replica(ActorRef coordinator) {
    this.coordinator = coordinator;
    this.isCoordinator = false;
  }

  static public Props props(ActorRef coordinator) {
    return Props.create(Replica.class, () -> new Replica(coordinator));
  }

  static public Props props() {
    return Props.create(Replica.class, Replica::new);
  }

  @Override
  public void preStart() {
    if (isCoordinator) {
      scheduleNextHeartbeat();
    }
  }

  private void scheduleNextHeartbeat() {
    Functions.setTimeout(getContext(), COORDINATOR_HEARTBEAT_PERIOD, getSelf(),
        new Messages.CoordinatorHeartbeatPeriod());
  }

  private void onRdRqMsg(Messages.RdRqMsg msg) {
    logger.info("{} received read request from client {}", Functions.getName(getSelf()),
        Functions.getId(getSender()));
    Functions.tellDelay(new Messages.RdRspMsg(value), getSelf(), getSender());
  }

  private void onWrRqMsg(Messages.WrRqMsg msg) {
    if (!isCoordinator) {
      forwardWriteRequestToCoordinator(msg);
    } else {
      handleCoordinatorWriteRequest(msg);
    }
  }

  private void forwardWriteRequestToCoordinator(Messages.WrRqMsg msg) {
    logger.info("{} received write request from client {} with value {}", Functions.getName(getSelf()),
        Functions.getId(msg.sender), msg.newValue);
    Functions.tellDelay(msg, getSelf(), coordinator);
    Functions.setTimeout(getContext(), UPDATE_TIMEOUT, getSelf(),
        new Messages.UpdTimeout(msg.sender, msg.operationCount));
  }

  private void handleCoordinatorWriteRequest(Messages.WrRqMsg msg) {
    logger.info("{} received write request from {} with value {}", Functions.getName(getSelf()),
        Functions.getName(getSender()), msg.newValue);
    if (nextCrash == Messages.CrashType.WhileSendingUpdate) {
      sendPartialUpdate(msg);
      crash();
    } else {
      broadcastUpdate(msg);
    }
  }

  private void sendPartialUpdate(Messages.WrRqMsg msg) {
    Random rnd = new Random();
    int omittedReplicaIndex = rnd.nextInt(replicas.size());
    seqno++;
    for (int i = 0; i < replicas.size(); i++) {
      if (i != omittedReplicaIndex) {
        Functions.tellDelay(new Messages.UpdRqMsg(msg.sender, epoch, seqno, msg.operationCount, msg.newValue),
            getSelf(),
            replicas.get(i));
      }
    }
  }

  private void broadcastUpdate(Messages.WrRqMsg msg) {
    Functions.multicast(new Messages.UpdRqMsg(msg.sender, epoch, ++seqno, msg.operationCount, msg.newValue), replicas,
        getSelf());
  }

  private void onUpdRqMsg(Messages.UpdRqMsg msg) {
    logger.debug("{} received UPDATE message from coordinator with value {} of epoch {} and seqno {}",
        Functions.getName(getSelf()), msg.value, msg.epoch, msg.seqno);
    EpochSeqno epochSeqno = new EpochSeqno(msg.epoch, msg.seqno);
    epochSeqnoValue.put(epochSeqno, msg.value);

    Map<ActorRef, Integer> m = Map.of(msg.sender, msg.operationCount);
    membersUpdRcvd.put(m, true);

    Functions.tellDelay(new Messages.AckMsg(msg.epoch, msg.seqno, msg.sender), getSelf(), getSender());

    Functions.setTimeout(getContext(), WRITE_OK_TIMEOUT, getSelf(),
        new Messages.AckTimeout(msg.epoch, msg.seqno));

    if (nextCrash == Messages.CrashType.AfterReceivingUpdate)
      crash();
  }

  private void onWrOk(Messages.WrOk msg) {
    this.epoch = msg.epoch;
    this.seqno = msg.seqno;
    EpochSeqno epochSeqno = new EpochSeqno(msg.epoch, msg.seqno);
    value = epochSeqnoValue.get(epochSeqno);
    ackRcvd.put(epochSeqno, true);

    logger.info("Replica {} update {}:{} {}", Functions.getId(getSelf()), epoch, seqno, value);
  }

  private void onUpdTimeout(Messages.UpdTimeout msg) {
    Map<ActorRef, Integer> m = Map.of(msg.sender, msg.operationCount);
    if (!membersUpdRcvd.getOrDefault(m, false)) {
      logger.error("Replica {} did not receive update in time. Coordinator might have crashed.",
          Functions.getId(getSelf()));

      Map<ActorRef, EpochSeqno> candidates = new HashMap<>();
      coordinatorCrashRecovery(coordinator, candidates);
    }
  }

  private void onAckTimeout(Messages.AckTimeout msg) {
    EpochSeqno epochSeqno = new EpochSeqno(msg.epoch, msg.seqno);
    if (!ackRcvd.getOrDefault(epochSeqno, false)) {
      logger.error("Replica {} did not receive write acknowledgment in time. Coordinator might have crashed.",
          Functions.getId(getSelf()));

      Map<ActorRef, EpochSeqno> candidates = new HashMap<>();
      coordinatorCrashRecovery(coordinator, candidates);
    }
  }

  private void onCoordinatorHeartbeatMsg(Messages.CoordinatorHeartbeatMsg msg) {
    if (!firstHeartbeatReceived) {
      firstHeartbeatReceived = true;
      replicaHeartbeatPeriod = Functions.setTimeout(getContext(), REPLICA_HEARTBEAT_PERIOD, getSelf(),
          new Messages.HeartbeatPeriod());
      logger.debug("Heartbeat period {}: Created first", Functions.getName(getSelf()));
    }

    logger.debug("{} received a heartbeat message from the coordinator", Functions.getName(getSelf()));
    heartbeatReceived = true;

    Functions.tellDelay(new Messages.HeartbeatMsg(), getSelf(), getSender());
  }

  private void onHeartbeatPeriod(Messages.HeartbeatPeriod msg) {
    logger.debug("{} Heartbeat Period reached", Functions.getName(getSelf()));
    if (!heartbeatReceived) {
      logger.error("{} did not heartbeat in time. Coordinator ({}) might have crashed.", Functions.getName(getSelf()),
          Functions.getName(coordinator));

      Map<ActorRef, EpochSeqno> candidates = new HashMap<>();
      coordinatorCrashRecovery(coordinator, candidates);
    }
    heartbeatReceived = false;
    replicaHeartbeatPeriod = Functions.setTimeout(getContext(), REPLICA_HEARTBEAT_PERIOD, getSelf(),
        new Messages.HeartbeatPeriod());
    logger.debug("heartbeat period {}: created new one", Functions.getName(getSelf()));
  }

  private void onJoinGroupMsg(Messages.JoinGroupMsg msg) {
    replicas.addAll(msg.group);
  }

  private void coordinatorCrashRecovery(ActorRef crashed_c, Map<ActorRef, EpochSeqno> candidates) {
    replicas.remove(crashed_c);

    if (replicaHeartbeatPeriod != null && !replicaHeartbeatPeriod.isCancelled()) {
      replicaHeartbeatPeriod.cancel();
      logger.debug("Heartbeat period {}: stopped", Functions.getName(getSelf()));
    }

    int selfIndex = replicas.indexOf(getSelf());
    int nextIndex = (selfIndex + 1) % replicas.size();

    EpochSeqno actorEpochSeqno = new EpochSeqno(epoch, seqno);
    candidates.put(getSelf(), actorEpochSeqno);
    logger.debug("{} sending an election message to {}, {} was removed", Functions.getName(getSelf()),
        Functions.getName(replicas.get(nextIndex)), Functions.getName(coordinator));
    Functions.tellDelay(new Messages.ElectionMsg(crashed_c, candidates), getSelf(), replicas.get(nextIndex));
    Functions.setTimeout(getContext(), ELECTION_TIMEOUT, getSelf(),
        new Messages.ElectionAckTimeout(replicas.get(nextIndex), coordinator, candidates));
  }

  private void electCoordinator(Map<ActorRef, EpochSeqno> candidates) {
    int maxEpoch = epoch;
    int maxSeqno = seqno;
    int maxId = 0;

    boolean incompleteBroadcast = false;

    for (Map.Entry<ActorRef, EpochSeqno> entry : candidates.entrySet()) {
      ActorRef actor = entry.getKey();
      EpochSeqno epochSeqno = entry.getValue();

      if (epochSeqno.epoch > maxEpoch ||
          (epochSeqno.epoch == maxEpoch && epochSeqno.seqno > maxSeqno) ||
          (epochSeqno.epoch == maxEpoch && epochSeqno.seqno == maxSeqno && Functions.getId(actor) > maxId)) {
        coordinator = actor;
        maxEpoch = epochSeqno.epoch;
        maxSeqno = epochSeqno.seqno;
        maxId = Functions.getId(actor);
        incompleteBroadcast = true;
      } else if (epochSeqno.epoch < maxEpoch ||
          (epochSeqno.epoch == maxEpoch && epochSeqno.seqno < maxSeqno)) {
        incompleteBroadcast = true;
      }
    }

    if (coordinator == getSelf() && !isCoordinator) {
      becomeCoordinator(incompleteBroadcast);
    } else {
      logger.info("{} [e: {}, sn: {}]: the new coordinator is {}, replica-size {}", Functions.getName(getSelf()), epoch,
          seqno, Functions.getName(coordinator), replicas.size());
    }

    sendCoordinatorMessage(candidates);
    if (replicaHeartbeatPeriod.isCancelled())
      replicaHeartbeatPeriod = Functions.setTimeout(getContext(), RESTART_REPLICA_HEARTBEAT_PERIOD, getSelf(),
          new Messages.HeartbeatPeriod());
  }

  private void becomeCoordinator(boolean incompleteBroadcast) {
    isCoordinator = true;
    epoch++;
    seqno = 0;
    onCoordinatorHeartbeatPeriod(new Messages.CoordinatorHeartbeatPeriod());
    logger.info("{} [e: {}, sn: {}]: the new coordinator is me, replica-size {}", Functions.getName(getSelf()), epoch,
        seqno, replicas.size());

    if (incompleteBroadcast)
      Functions.multicast(new Messages.UpdRqMsg(getSelf(), epoch, ++seqno, 0, value), replicas, getSelf());
  }

  private void sendCoordinatorMessage(Map<ActorRef, EpochSeqno> candidates) {
    int selfIndex = replicas.indexOf(getSelf());
    int nextIndex = (selfIndex + 1) % replicas.size();
    logger.debug("{} sending coordinator message to {}", Functions.getName(getSelf()),
        Functions.getName(replicas.get(nextIndex)));
    Functions.tellDelay(new Messages.CoordinatorMsg(candidates), getSelf(), replicas.get(nextIndex));
    Functions.setTimeout(getContext(), COORDINATOR_ELECTION_TIMEOUT, getSelf(),
        new Messages.CoordinatorAckTimeout(replicas.get(nextIndex), candidates));
  }

  private void onElectionMsg(Messages.ElectionMsg msg) {
    if (nextCrash == Messages.CrashType.WhileElection) {
      crash();
      return;
    }
    logger.debug("{} receiving an election message from {}", Functions.getName(getSelf()),
        Functions.getName(getSender()));
    Functions.tellDelay(new Messages.ElectionMsgAck(), getSelf(), getSender());
    if (msg.coordinatorCandidates.containsKey(getSelf())) {
      electCoordinator(msg.coordinatorCandidates);
    } else {
      coordinatorCrashRecovery(msg.crashedCoordinator, msg.coordinatorCandidates);
    }
  }

  private void onElectionMsgAck(Messages.ElectionMsgAck msg) {
    int acksReceived = electionAcksReceived.getOrDefault(getSender(), 0) + 1;
    electionAcksReceived.put(getSender(), acksReceived);

    logger.debug("{} received election msg ACK from {}", Functions.getName(getSelf()), Functions.getName(getSender()));
  }

  private void onCoordinatorMsg(Messages.CoordinatorMsg msg) {
    if (nextCrash == Messages.CrashType.WhileChoosingCoordinator) {
      crash();
      return;
    }
    Functions.tellDelay(new Messages.CoordinatorMsgAck(), getSelf(), getSender());
    if (!replicas.contains(coordinator))
      electCoordinator(msg.coordinatorCandidates);
  }

  private void onCoordinatorMsgAck(Messages.CoordinatorMsgAck msg) {
    int ackReceived = electionCoordinatorAcksReceived.getOrDefault(getSender(), 0) + 1;
    electionCoordinatorAcksReceived.put(getSender(), ackReceived);

    logger.debug("{} received coordinator msg ACK from {}", Functions.getName(getSelf()),
        Functions.getName(getSender()));
  }

  private void onElectionAckTimeout(Messages.ElectionAckTimeout msg) {
    if (electionAcksReceived.getOrDefault(msg.nextReplica, 0) == 0) {
      handleMissingElectionAck(msg);
    } else {
      decrementElectionAckCount(msg.nextReplica);
    }
  }

  private void handleMissingElectionAck(Messages.ElectionAckTimeout msg) {
    logger.debug("{} has not received election msg ACK, remove {}", Functions.getName(getSelf()),
        Functions.getName(msg.nextReplica));
    replicas.remove(msg.nextReplica);

    int nextIndex = getNextReplicaIndex();
    logger.debug("{} sending an election message to {}, {} was removed", Functions.getName(getSelf()),
        Functions.getName(replicas.get(nextIndex)), Functions.getName(coordinator));
    Functions.tellDelay(new Messages.ElectionMsg(msg.crashedCoordinator, msg.coordinatorCandidates), getSelf(),
        replicas.get(nextIndex));
    Functions.setTimeout(getContext(), ELECTION_TIMEOUT, getSelf(),
        new Messages.ElectionAckTimeout(replicas.get(nextIndex), coordinator, msg.coordinatorCandidates));
  }

  private void decrementElectionAckCount(ActorRef replica) {
    int ackReceived = electionAcksReceived.get(replica) - 1;
    electionAcksReceived.put(replica, ackReceived);
  }

  private void onCoordinatorAckTimeout(Messages.CoordinatorAckTimeout msg) {
    if (electionCoordinatorAcksReceived.getOrDefault(msg.nextReplica, 0) == 0) {
      handleMissingCoordinatorAck(msg);
    } else {
      decrementCoordinatorAckCount(msg.nextReplica);
    }
  }

  private void handleMissingCoordinatorAck(Messages.CoordinatorAckTimeout msg) {
    logger.debug("{} has not received coordinator msg ACK, remove {}", Functions.getName(getSelf()),
        Functions.getName(msg.nextReplica));
    replicas.remove(msg.nextReplica);

    int nextIndex = getNextReplicaIndex();
    logger.debug("{} sending coordinator message to {}", Functions.getName(getSelf()),
        Functions.getName(replicas.get(nextIndex)));
    Functions.tellDelay(new Messages.CoordinatorMsg(msg.coordinatorCandidates), getSelf(), replicas.get(nextIndex));
    Functions.setTimeout(getContext(), COORDINATOR_ELECTION_TIMEOUT, getSelf(),
        new Messages.CoordinatorAckTimeout(replicas.get(nextIndex), msg.coordinatorCandidates));
  }

  private void decrementCoordinatorAckCount(ActorRef replica) {
    int ackReceived = electionCoordinatorAcksReceived.get(replica) - 1;
    electionCoordinatorAcksReceived.put(replica, ackReceived);
  }

  private int getNextReplicaIndex() {
    int selfIndex = replicas.indexOf(getSelf());
    return (selfIndex + 1) % replicas.size();
  }

  private void onCrashMsg(Messages.CrashMsg msg) {
    if (msg.nextCrash == Messages.CrashType.NotResponding) {
      crash();
    } else {
      nextCrash = msg.nextCrash;
    }
  }

  private void onChangeReplicaSet(Messages.ChangeReplicaSet msg) {
    replicas.clear();
    replicas.addAll(msg.group);
  }

  private void onCoordinatorHeartbeatPeriod(Messages.CoordinatorHeartbeatPeriod msg) {
    if (isCoordinator) {
      logger.debug("{} sent out a heartbeat message to all replicas", Functions.getName(getSelf()));
      scheduleNextHeartbeat();
      Messages.CoordinatorHeartbeatMsg confAlive = new Messages.CoordinatorHeartbeatMsg();
      Functions.multicast(confAlive, replicas, getSelf());
      Functions.setTimeout(getContext(), COORDINATOR_HEARTBEAT_TIMEOUT, getSelf(),
          new Messages.CoordinatorHeartbeatTimeout());
    }
  }

  private void onHeartbeatMsg(Messages.HeartbeatMsg msg) {
    if (isCoordinator) {
      logger.debug("{} got a heartbeat message from {}", Functions.getName(getSelf()), Functions.getName(getSender()));
      replicasAlive.add(getSender());
    }
  }

  private void onCoordinatorHeartbeatTimeout(Messages.CoordinatorHeartbeatTimeout msg) {
    if (isCoordinator) {
      handleHeartbeatResponses();
    }
  }

  private void handleHeartbeatResponses() {
    if (replicas.size() != replicasAlive.size()) {
      logger.warn("replicas did not respond. Initiating failure handling.");
      List<ActorRef> deadReplicas = new ArrayList<>(replicas);
      deadReplicas.removeAll(replicasAlive);

      logReplicaStatus(deadReplicas);

      replicas.removeAll(deadReplicas);
      notifyReplicasOfNewSet();
    }
    replicasAlive.clear();
  }

  private void logReplicaStatus(List<ActorRef> deadReplicas) {
    for (ActorRef r : deadReplicas) {
      logger.warn("{} is dead", Functions.getName(r));
    }
    for (ActorRef r : replicasAlive) {
      logger.debug("{} is alive", Functions.getName(r));
    }
  }

  private void notifyReplicasOfNewSet() {
    Functions.multicast(new Messages.ChangeReplicaSet(replicas), replicas, coordinator);
  }

  private void onAck(Messages.AckMsg msg) {
    if (isCoordinator) {
      EpochSeqno es = new EpochSeqno(msg.epoch, msg.seqno);
      int counter = epochSeqnoAckCounter.getOrDefault(es, 0) + 1;
      epochSeqnoAckCounter.put(es, counter);
      logger.debug("{} received {} ack(s) from {} of epoch {} seqno {}", Functions.getName(getSelf()),
          epochSeqnoAckCounter.get(es),
          Functions.getName(getSender()), msg.epoch, msg.seqno);
      int Q = (replicas.size() / 2) + 1;

      if (epochSeqnoAckCounter.get(es) >= Q && epochSeqnoConfirmUpdate.getOrDefault(es, true)) {
        confirmUpdate(msg, es);
      }
    }
  }

  private void confirmUpdate(Messages.AckMsg msg, EpochSeqno es) {
    logger.debug("{} confirm the update to all the replica", Functions.getName(getSelf()));
    if (nextCrash == Messages.CrashType.WhileSendingWriteOk) {
      sendPartialWriteOk(msg);
      crash();
    } else {
      Functions.multicast(new Messages.WrOk(msg.epoch, msg.seqno, getSelf()), replicas, getSelf());
    }
    epochSeqnoConfirmUpdate.put(es, false);
  }

  private void sendPartialWriteOk(Messages.AckMsg msg) {
    for (int i = 0; i < replicas.size() - 2; i++) {
      replicas.get(i).tell(new Messages.WrOk(msg.epoch, msg.seqno, getSelf()), getSelf());
    }
  }

  private void crash() {
    logger.error("{} crashed", Functions.getName(getSelf()));
    getContext().become(crashed());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(Messages.JoinGroupMsg.class, this::onJoinGroupMsg)
        .match(Messages.RdRqMsg.class, this::onRdRqMsg)
        .match(Messages.WrRqMsg.class, this::onWrRqMsg)
        .match(Messages.UpdRqMsg.class, this::onUpdRqMsg)
        .match(Messages.UpdTimeout.class, this::onUpdTimeout)
        .match(Messages.AckTimeout.class, this::onAckTimeout)
        .match(Messages.CoordinatorHeartbeatMsg.class, this::onCoordinatorHeartbeatMsg)
        .match(Messages.HeartbeatPeriod.class, this::onHeartbeatPeriod)
        .match(Messages.WrOk.class, this::onWrOk)
        .match(Messages.CrashMsg.class, this::onCrashMsg)
        .match(Messages.ChangeReplicaSet.class, this::onChangeReplicaSet)
        .match(Messages.CoordinatorHeartbeatPeriod.class, this::onCoordinatorHeartbeatPeriod)
        .match(Messages.HeartbeatMsg.class, this::onHeartbeatMsg)
        .match(Messages.AckMsg.class, this::onAck)
        .match(Messages.CoordinatorHeartbeatTimeout.class, this::onCoordinatorHeartbeatTimeout)
        .match(Messages.ElectionMsg.class, this::onElectionMsg)
        .match(Messages.ElectionMsgAck.class, this::onElectionMsgAck)
        .match(Messages.ElectionAckTimeout.class, this::onElectionAckTimeout)
        .match(Messages.CoordinatorMsg.class, this::onCoordinatorMsg)
        .match(Messages.CoordinatorMsgAck.class, this::onCoordinatorMsgAck)
        .match(Messages.CoordinatorAckTimeout.class, this::onCoordinatorAckTimeout)
        .build();
  }

  private Receive crashed() {
    return receiveBuilder()
        .matchAny(msg -> {
          // Do nothing when crashed
        })
        .build();
  }
}

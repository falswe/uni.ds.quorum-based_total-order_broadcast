package it.unitn.ds1.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unitn.ds1.utils.Message;
import it.unitn.ds1.utils.Message.CrashType;
import it.unitn.ds1.utils.Helper;
import it.unitn.ds1.utils.Helper.TimeId;

public class Replica extends AbstractActor {

  private static final Logger logger = LoggerFactory.getLogger(Replica.class);

  // Timeouts
  private static final int UPDATE_TIMEOUT = 1000;
  private static final int WRITE_OK_TIMEOUT = 1000;
  private static final int HEARTBEAT_TIMEOUT = Helper.DELAY_TIME + 2000;
  private static final int RESTART_HEARTBEAT_TIMEOUT = 5000;
  private static final int ELECTION_TIMEOUT = Helper.DELAY_TIME * 2 + 100;
  private static final int COORDINATOR_TIMEOUT = Helper.DELAY_TIME * 2 + 100;
  private static final int HEARTBEAT_PERIOD = 1000;
  private static final int HEARTBEAT_RESPONSE_TIMEOUT = Helper.DELAY_TIME + 500;

  private int currentValue;
  private int epoch;
  private int seqno;

  private boolean firstHeartbeatReceived;
  private boolean heartbeatReceived;

  private final Map<ActorRef, Integer> electionAcksReceived;
  private final Map<ActorRef, Integer> electionCoordinatorAcksReceived;

  private ActorRef coordinator;
  private boolean isCoordinator;

  private final List<ActorRef> replicas;
  private final List<ActorRef> replicasAlive;

  private final Map<Map<ActorRef, Integer>, Boolean> memberUpdateReceived;
  private final Map<Helper.TimeId, Boolean> ackReceived;

  private final Map<Helper.TimeId, Integer> timeIdAckCounter;
  private final Map<Helper.TimeId, Boolean> timeIdConfirmUpdate;

  private final Map<Helper.TimeId, Integer> timeIdValue;

  private Cancellable heartbeatTimeout;

  private CrashType nextCrash;

  public Replica() {
    this.currentValue = 0;
    this.seqno = 0;
    this.epoch = 0;
    this.firstHeartbeatReceived = false;
    this.heartbeatReceived = false;
    this.electionAcksReceived = new HashMap<>();
    this.electionCoordinatorAcksReceived = new HashMap<>();
    this.coordinator = getSelf();
    this.isCoordinator = true;
    this.replicas = new ArrayList<>();
    this.replicasAlive = new ArrayList<>();
    this.memberUpdateReceived = new HashMap<>();
    this.ackReceived = new HashMap<>();
    this.timeIdAckCounter = new HashMap<>();
    this.timeIdConfirmUpdate = new HashMap<>();
    this.timeIdValue = new HashMap<>();
    this.nextCrash = CrashType.NONE;
    logger.debug("Replica {} initialized as coordinator", Helper.getId(getSelf()));
  }

  public Replica(ActorRef coordinator) {
    this();
    this.coordinator = coordinator;
    this.isCoordinator = false;
    logger.debug("Replica {} initialized with coordinator {}", Helper.getId(getSelf()), Helper.getId(coordinator));
  }

  public static Props props(ActorRef coordinator) {
    return Props.create(Replica.class, () -> new Replica(coordinator));
  }

  public static Props props() {
    return Props.create(Replica.class, Replica::new);
  }

  @Override
  public void preStart() {
    if (isCoordinator) {
      logger.debug("Coordinator {} starting heartbeat", Helper.getId(getSelf()));
      scheduleNextHeartbeat();
    }
  }

  private void scheduleNextHeartbeat() {
    Helper.setTimeout(getContext(), HEARTBEAT_PERIOD, getSelf(),
        new Message.Coordinator.HeartbeatPeriod());
  }

  private void onReadRequest(Message.Client.ReadRequest msg) {
    logger.debug("Replica {} received read request from Client {}", Helper.getId(getSelf()), Helper.getId(getSender()));
    Helper.tellDelay(new Message.Replica.ReadResponse(currentValue), getSelf(), getSender());
  }

  private void onWriteRequest(Message.Client.WriteRequest msg) {
    if (!isCoordinator) {
      logger.debug("Replica {} forwarding write request from Client {} to Coordinator {}",
          Helper.getId(getSelf()), Helper.getId(msg.sender), Helper.getId(coordinator));
      forwardWriteRequestToCoordinator(msg);
    } else {
      logger.debug("Coordinator {} received write request from Client {} with value {}",
          Helper.getId(getSelf()), Helper.getId(msg.sender), msg.newValue);
      handleCoordinatorWriteRequest(msg);
    }
  }

  private void forwardWriteRequestToCoordinator(Message.Client.WriteRequest msg) {
    Helper.tellDelay(msg, getSelf(), coordinator);
    scheduleUpdateTimeout(msg);
  }

  private void scheduleUpdateTimeout(Message.Client.WriteRequest msg) {
    Helper.setTimeout(getContext(), UPDATE_TIMEOUT, getSelf(),
        new Message.Replica.UpdateTimeout(msg.sender, msg.operationCount));
  }

  private void handleCoordinatorWriteRequest(Message.Client.WriteRequest msg) {
    seqno++;
    if (nextCrash == CrashType.WHILE_SENDING_UPDATE) {
      sendPartialUpdate(msg);
      crash();
    } else {
      broadcastUpdate(msg);
    }
  }

  private void sendPartialUpdate(Message.Client.WriteRequest msg) {
    logger.warn("Coordinator {} simulating partial update before crash", Helper.getId(getSelf()));
    Random rnd = new Random();
    int omittedReplicaIndex = rnd.nextInt(replicas.size());
    for (int i = 0; i < replicas.size(); i++) {
      if (i != omittedReplicaIndex) {
        sendUpdateToReplica(msg, replicas.get(i));
      }
    }
  }

  private void broadcastUpdate(Message.Client.WriteRequest msg) {
    logger.debug("Coordinator {} broadcasting update to {} replicas", Helper.getId(getSelf()), replicas.size());
    Helper.multicast(createUpdateMessage(msg), replicas, getSelf());
  }

  private Message.Coordinator.Update createUpdateMessage(Message.Client.WriteRequest msg) {
    return new Message.Coordinator.Update(msg.sender, epoch, seqno, msg.operationCount, msg.newValue);
  }

  private void sendUpdateToReplica(Message.Client.WriteRequest msg, ActorRef replica) {
    Helper.tellDelay(createUpdateMessage(msg), getSelf(), replica);
  }

  private void onUpdate(Message.Coordinator.Update msg) {
    logger.debug("Replica {} received UPDATE message: epoch={}, seqno={}, value={}",
        Helper.getId(getSelf()), msg.epoch, msg.seqno, msg.value);
    updateTimeIdValue(msg);
    markMemberUpdateReceived(msg);
    sendUpdateAck(msg);
    scheduleWriteOkTimeout(msg);
    checkForCrashAfterReceivingUpdate();
  }

  private void updateTimeIdValue(Message.Coordinator.Update msg) {
    Helper.TimeId timeId = new Helper.TimeId(msg.epoch, msg.seqno);
    timeIdValue.put(timeId, msg.value);
  }

  private void markMemberUpdateReceived(Message.Coordinator.Update msg) {
    Map<ActorRef, Integer> m = Map.of(msg.sender, msg.operationCount);
    memberUpdateReceived.put(m, true);
  }

  private void sendUpdateAck(Message.Coordinator.Update msg) {
    logger.debug("Replica {} sending UpdateAck to Coordinator {}", Helper.getId(getSelf()), Helper.getId(getSender()));
    Helper.tellDelay(new Message.Replica.UpdateAck(msg.epoch, msg.seqno, msg.sender), getSelf(), getSender());
  }

  private void scheduleWriteOkTimeout(Message.Coordinator.Update msg) {
    Helper.setTimeout(getContext(), WRITE_OK_TIMEOUT, getSelf(),
        new Message.Replica.WriteOkTimeout(msg.epoch, msg.seqno));
  }

  private void checkForCrashAfterReceivingUpdate() {
    if (nextCrash == CrashType.AFTER_RECEIVING_UPDATE) {
      logger.warn("Replica {} crashing after receiving update", Helper.getId(getSelf()));
      crash();
    }
  }

  private void onWriteOk(Message.Coordinator.WriteOk msg) {
    this.epoch = msg.epoch;
    this.seqno = msg.seqno;
    updateCurrentValue(msg);
    markAckReceived(msg);
    logger.info("Replica {} update {}:{} {}",
        Helper.getId(getSelf()), epoch, seqno, currentValue);
  }

  private void updateCurrentValue(Message.Coordinator.WriteOk msg) {
    Helper.TimeId timeId = new Helper.TimeId(msg.epoch, msg.seqno);
    currentValue = timeIdValue.get(timeId);
  }

  private void markAckReceived(Message.Coordinator.WriteOk msg) {
    Helper.TimeId timeId = new Helper.TimeId(msg.epoch, msg.seqno);
    ackReceived.put(timeId, true);
  }

  private void onUpdateTimeout(Message.Replica.UpdateTimeout msg) {
    Map<ActorRef, Integer> m = Map.of(msg.sender, msg.operationCount);
    if (!memberUpdateReceived.getOrDefault(m, false)) {
      logger.warn(
          "Replica {} did not receive update from Coordinator {}. Initiating coordinator crash recovery.",
          Helper.getId(getSelf()), Helper.getId(coordinator));
      Map<ActorRef, Helper.TimeId> candidates = new HashMap<>();
      coordinatorCrashRecovery(coordinator, candidates);
    }
  }

  private void onWriteOkTimeout(Message.Replica.WriteOkTimeout msg) {
    Helper.TimeId timeId = new Helper.TimeId(msg.epoch, msg.seqno);
    if (!ackReceived.getOrDefault(timeId, false)) {
      logger.warn("Replica {} did not receive WriteOk for epoch={}, seqno={}. Initiating coordinator crash recovery.",
          Helper.getId(getSelf()), msg.epoch, msg.seqno);
      Map<ActorRef, Helper.TimeId> candidates = new HashMap<>();
      coordinatorCrashRecovery(coordinator, candidates);
    }
  }

  private void onHeartbeat(Message.Coordinator.Heartbeat msg) {
    logger.debug("Replica {} received heartbeat from Coordinator {}", Helper.getId(getSelf()),
        Helper.getId(getSender()));
    if (!firstHeartbeatReceived) {
      firstHeartbeatReceived = true;
      heartbeatTimeout = Helper.setTimeout(getContext(), HEARTBEAT_TIMEOUT, getSelf(),
          new Message.Replica.HeartbeatTimeout());
    }
    heartbeatReceived = true;
    Helper.tellDelay(new Message.Replica.HeartbeatResponse(), getSelf(), getSender());
  }

  private void onHeartbeatTimeout(Message.Replica.HeartbeatTimeout msg) {
    logger.debug("Heartbeat timeout reached for replica {}", Helper.getId(getSelf()));
    if (!heartbeatReceived) {
      logger.warn("Replica {} did not receive a heartbeat in time. Coordinator {} might have crashed.",
          Helper.getId(getSelf()), Helper.getId(coordinator));
      Map<ActorRef, Helper.TimeId> candidates = new HashMap<>();
      coordinatorCrashRecovery(coordinator, candidates);
    }
    heartbeatReceived = false;
    heartbeatTimeout = Helper.setTimeout(getContext(), HEARTBEAT_TIMEOUT, getSelf(),
        new Message.Replica.HeartbeatTimeout());
  }

  private void onJoinGroup(Message.System.JoinGroup msg) {
    replicas.addAll(msg.group);
    logger.debug("Replica {} joined group with {} replicas", Helper.getId(getSelf()), replicas.size());
  }

  private void coordinatorCrashRecovery(ActorRef crashedCoordinator, Map<ActorRef, Helper.TimeId> candidates) {
    logger.debug("Replica {} initiating coordinator crash recovery. Crashed coordinator: {}",
        Helper.getId(getSelf()), Helper.getId(crashedCoordinator));
    if (replicas.contains(crashedCoordinator)) {
      logger.debug("{} change context to election", Helper.getName(getSelf()));
      getContext().become(election());
      replicas.remove(crashedCoordinator);
    }
    if (heartbeatTimeout != null && !heartbeatTimeout.isCancelled()) {
      heartbeatTimeout.cancel();
    }
    int selfIndex = replicas.indexOf(getSelf());
    int nextIndex = (selfIndex + 1) % replicas.size();
    Helper.TimeId timeId = new Helper.TimeId(epoch, seqno);
    candidates.put(getSelf(), timeId);
    logger.debug("Replica {} sending Election message to Replica {}",
        Helper.getId(getSelf()), Helper.getId(replicas.get(nextIndex)));
    Helper.tellDelay(new Message.Replica.Election(crashedCoordinator, candidates), getSelf(), replicas.get(nextIndex));
    Helper.setTimeout(getContext(), ELECTION_TIMEOUT, getSelf(),
        new Message.Replica.ElectionAckTimeout(replicas.get(nextIndex), coordinator, candidates));
  }

  private void electCoordinator(Map<ActorRef, Helper.TimeId> candidates) {
    logger.debug("Replica {} starting coordinator election process", Helper.getId(getSelf()));
    ActorRef bestActor = getSelf();
    TimeId bestTimeId = new TimeId(epoch, seqno);
    boolean incompleteBroadcast = false;

    for (Map.Entry<ActorRef, TimeId> entry : candidates.entrySet()) {
      ActorRef actor = entry.getKey();
      TimeId timeId = entry.getValue();

      if (isPreferredCandidate(timeId, bestTimeId, actor, bestActor)) {
        bestActor = actor;
        bestTimeId = timeId;
      }

      if (timeId.epoch != bestTimeId.epoch || timeId.seqno != bestTimeId.seqno) {
        incompleteBroadcast = true;
      }
    }

    coordinator = bestActor;
    logger.debug("{} change context to create receive", Helper.getName(getSelf()));
    getContext().become(createReceive());

    if (coordinator == getSelf() && !isCoordinator) {
      becomeCoordinator(incompleteBroadcast);
    } else {
      logger.info("Replica {} acknowledges new coordinator: {}. Epoch: {}, Seqno: {}, Total Replicas: {}",
          Helper.getId(getSelf()), Helper.getId(coordinator), epoch, seqno, replicas.size());
    }

    sendCoordinatorMessage(candidates);
    resetHeartbeatTimeout();
  }

  private boolean isPreferredCandidate(TimeId candidate, TimeId current, ActorRef candidateActor,
      ActorRef currentActor) {
    return candidate.epoch > current.epoch ||
        (candidate.epoch == current.epoch && candidate.seqno > current.seqno) ||
        (candidate.epoch == current.epoch && candidate.seqno == current.seqno &&
            Helper.getId(candidateActor) > Helper.getId(currentActor));
  }

  private void becomeCoordinator(boolean incompleteBroadcast) {
    isCoordinator = true;
    epoch++;
    seqno = 0;
    logger.info("Replica {} became the new coordinator. Epoch: {}, Seqno: {}, Replicas: {}",
        Helper.getId(getSelf()), epoch, seqno, replicas.size());
    onHeartbeatPeriod(new Message.Coordinator.HeartbeatPeriod());

    if (incompleteBroadcast) {
      logger.warn("Incomplete broadcast detected. Coordinator {} initiating new update.", Helper.getId(getSelf()));
      Helper.multicast(new Message.Coordinator.Update(getSelf(), epoch, ++seqno, 0, currentValue), replicas, getSelf());
    }
  }

  private void resetHeartbeatTimeout() {
    if (heartbeatTimeout == null || heartbeatTimeout.isCancelled()) {
      heartbeatTimeout = Helper.setTimeout(getContext(), RESTART_HEARTBEAT_TIMEOUT, getSelf(),
          new Message.Replica.HeartbeatTimeout());
    }
  }

  private void sendCoordinatorMessage(Map<ActorRef, Helper.TimeId> candidates) {
    int selfIndex = replicas.indexOf(getSelf());
    int nextIndex = (selfIndex + 1) % replicas.size();
    logger.debug("Replica {} sending coordinator message to Replica {}",
        Helper.getId(getSelf()), Helper.getId(replicas.get(nextIndex)));
    Helper.tellDelay(new Message.Replica.Coordinator(candidates), getSelf(), replicas.get(nextIndex));
    Helper.setTimeout(getContext(), COORDINATOR_TIMEOUT, getSelf(),
        new Message.Replica.CoordinatorAckTimeout(replicas.get(nextIndex), candidates));
  }

  private void onElection(Message.Replica.Election msg) {
    logger.debug("Replica {} received an election message from {}",
        Helper.getId(getSelf()), Helper.getName(getSender()));
    if (nextCrash == CrashType.WHILE_ELECTION) {
      logger.warn("Replica {} crashing during election", Helper.getId(getSelf()));
      crash();
      return;
    }
    Helper.tellDelay(new Message.Replica.ElectionAck(), getSelf(), getSender());
    if (msg.coordinatorCandidates.containsKey(getSelf())) {
      electCoordinator(msg.coordinatorCandidates);
    } else {
      coordinatorCrashRecovery(msg.crashedCoordinator, msg.coordinatorCandidates);
    }
  }

  private void onElectionAck(Message.Replica.ElectionAck msg) {
    logger.debug("Replica {} received election ACK from {}", Helper.getId(getSelf()), Helper.getName(getSender()));
    int acksReceived = electionAcksReceived.getOrDefault(getSender(), 0) + 1;
    electionAcksReceived.put(getSender(), acksReceived);
  }

  private void onCoordinator(Message.Replica.Coordinator msg) {
    logger.debug("Replica {} received coordinator message", Helper.getId(getSelf()));
    if (nextCrash == CrashType.WHILE_CHOOSING_COORDINATOR) {
      logger.warn("Replica {} crashing while choosing coordinator", Helper.getId(getSelf()));
      crash();
      return;
    }
    Helper.tellDelay(new Message.Replica.CoordinatorAck(), getSelf(), getSender());
    if (!replicas.contains(coordinator)) {
      electCoordinator(msg.coordinatorCandidates);
    }
  }

  private void onCoordinatorAck(Message.Replica.CoordinatorAck msg) {
    logger.debug("Replica {} received coordinator ACK from {}", Helper.getId(getSelf()), Helper.getName(getSender()));
    int ackReceived = electionCoordinatorAcksReceived.getOrDefault(getSender(), 0) + 1;
    electionCoordinatorAcksReceived.put(getSender(), ackReceived);
  }

  private void onElectionAckTimeout(Message.Replica.ElectionAckTimeout msg) {
    logger.debug("Replica {} election ACK timeout for {}", Helper.getId(getSelf()), Helper.getName(msg.nextReplica));
    if (electionAcksReceived.getOrDefault(msg.nextReplica, 0) == 0) {
      handleMissingElectionAck(msg);
    } else {
      decrementElectionAckCount(msg.nextReplica);
    }
  }

  private void handleMissingElectionAck(Message.Replica.ElectionAckTimeout msg) {
    logger.debug("Replica {} has not received election ACK, removing {}", Helper.getId(getSelf()),
        Helper.getName(msg.nextReplica));
    replicas.remove(msg.nextReplica);
    int nextIndex = getNextReplicaIndex();
    logger.debug("Replica {} sending an election message to {}, {} was removed", Helper.getId(getSelf()),
        Helper.getName(replicas.get(nextIndex)), Helper.getName(coordinator));
    Helper.tellDelay(new Message.Replica.Election(msg.crashedCoordinator, msg.coordinatorCandidates), getSelf(),
        replicas.get(nextIndex));
    Helper.setTimeout(getContext(), ELECTION_TIMEOUT, getSelf(),
        new Message.Replica.ElectionAckTimeout(replicas.get(nextIndex), coordinator, msg.coordinatorCandidates));
  }

  private void decrementElectionAckCount(ActorRef replica) {
    int ackReceived = electionAcksReceived.get(replica) - 1;
    electionAcksReceived.put(replica, ackReceived);
  }

  private void onCoordinatorAckTimeout(Message.Replica.CoordinatorAckTimeout msg) {
    logger.debug("Replica {} coordinator ACK timeout for {}", Helper.getId(getSelf()), Helper.getName(msg.nextReplica));
    if (electionCoordinatorAcksReceived.getOrDefault(msg.nextReplica, 0) == 0) {
      handleMissingCoordinatorAck(msg);
    } else {
      decrementCoordinatorAckCount(msg.nextReplica);
    }
  }

  private void handleMissingCoordinatorAck(Message.Replica.CoordinatorAckTimeout msg) {
    logger.debug("Replica {} has not received coordinator ACK, removing {}", Helper.getId(getSelf()),
        Helper.getName(msg.nextReplica));
    replicas.remove(msg.nextReplica);
    int nextIndex = getNextReplicaIndex();
    logger.debug("Replica {} sending coordinator message to {}", Helper.getId(getSelf()),
        Helper.getName(replicas.get(nextIndex)));
    Helper.tellDelay(new Message.Replica.Coordinator(msg.coordinatorCandidates), getSelf(), replicas.get(nextIndex));
    Helper.setTimeout(getContext(), COORDINATOR_TIMEOUT, getSelf(),
        new Message.Replica.CoordinatorAckTimeout(replicas.get(nextIndex), msg.coordinatorCandidates));
  }

  private void decrementCoordinatorAckCount(ActorRef replica) {
    int ackReceived = electionCoordinatorAcksReceived.get(replica) - 1;
    electionCoordinatorAcksReceived.put(replica, ackReceived);
  }

  private int getNextReplicaIndex() {
    int selfIndex = replicas.indexOf(getSelf());
    return (selfIndex + 1) % replicas.size();
  }

  private void onCrash(Message.System.Crash msg) {
    if (msg.crashType == CrashType.NOT_RESPONDING) {
      crash();
    } else {
      nextCrash = msg.crashType;
      logger.warn("Replica {} set to crash on next {} operation", Helper.getId(getSelf()), msg.crashType);
    }
  }

  private void onChangeReplicaSet(Message.Coordinator.ChangeReplicaSet msg) {
    replicas.clear();
    replicas.addAll(msg.group);
    logger.info("Replica {} updated replica set. New size: {}", Helper.getId(getSelf()), replicas.size());
  }

  private void onHeartbeatPeriod(Message.Coordinator.HeartbeatPeriod msg) {
    if (isCoordinator) {
      logger.debug("Coordinator {} sending heartbeat to all replicas", Helper.getId(getSelf()));
      scheduleNextHeartbeat();
      Message.Coordinator.Heartbeat heartbeat = new Message.Coordinator.Heartbeat();
      Helper.multicast(heartbeat, replicas, getSelf());
      Helper.setTimeout(getContext(), HEARTBEAT_RESPONSE_TIMEOUT, getSelf(),
          new Message.Coordinator.HeartbeatResponseTimeout());
    }
  }

  private void onHeartbeatResponse(Message.Replica.HeartbeatResponse msg) {
    if (isCoordinator) {
      logger.debug("Coordinator {} received heartbeat response from {}",
          Helper.getId(getSelf()), Helper.getId(getSender()));
      replicasAlive.add(getSender());
    }
  }

  private void onHeartbeatResponseTimeout(Message.Coordinator.HeartbeatResponseTimeout msg) {
    if (isCoordinator) {
      handleHeartbeatResponses();
    }
  }

  private void handleHeartbeatResponses() {
    logger.debug("Coordinator {} processing heartbeat responses", Helper.getId(getSelf()));
    if (replicas.size() != replicasAlive.size()) {
      logger.warn("Coordinator {} detected non-responsive replicas. Initiating failure handling.",
          Helper.getId(getSelf()));
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
      logger.debug("Replica {} is unresponsive", Helper.getId(r));
    }
    for (ActorRef r : replicasAlive) {
      logger.debug("Replica {} is alive", Helper.getId(r));
    }
  }

  private void notifyReplicasOfNewSet() {
    logger.info("Coordinator {} notifying replicas of new set. New size: {}", Helper.getId(getSelf()), replicas.size());
    Helper.multicast(new Message.Coordinator.ChangeReplicaSet(replicas), replicas, coordinator);
  }

  private void onUpdateAck(Message.Replica.UpdateAck msg) {
    if (isCoordinator) {
      Helper.TimeId timeId = new Helper.TimeId(msg.epoch, msg.seqno);
      int counter = timeIdAckCounter.getOrDefault(timeId, 0) + 1;
      timeIdAckCounter.put(timeId, counter);

      logger.debug("Coordinator {} received ACK from {}. Epoch: {}, Seqno: {}, Total ACKs: {}",
          Helper.getId(getSelf()),
          Helper.getName(getSender()),
          msg.epoch,
          msg.seqno,
          counter);
      int Q = (replicas.size() / 2) + 1;

      if (timeIdAckCounter.get(timeId) >= Q && timeIdConfirmUpdate.getOrDefault(timeId, true)) {
        confirmUpdate(msg, timeId);
      }
    }
  }

  private void confirmUpdate(Message.Replica.UpdateAck msg, Helper.TimeId es) {
    logger.info("Coordinator {} confirming update to all replicas. Epoch: {}, Seqno: {}",
        Helper.getId(getSelf()), es.epoch, es.seqno);
    if (nextCrash == CrashType.WHILE_SENDING_WRITE_OK) {
      sendPartialWriteOk(msg);
      crash();
    } else {
      Helper.multicast(new Message.Coordinator.WriteOk(msg.epoch, msg.seqno, getSelf()), replicas, getSelf());
    }
    timeIdConfirmUpdate.put(es, false);
  }

  private void sendPartialWriteOk(Message.Replica.UpdateAck msg) {
    logger.warn("Coordinator {} sending partial WriteOk before crashing", Helper.getId(getSelf()));
    for (int i = 0; i < replicas.size() - 2; i++) {
      replicas.get(i).tell(new Message.Coordinator.WriteOk(msg.epoch, msg.seqno, getSelf()), getSelf());
    }
  }

  private void crash() {
    logger.error("Replica {} has crashed", Helper.getId(getSelf()));
    getContext().become(crashed());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(Message.System.JoinGroup.class, this::onJoinGroup)
        .match(Message.Client.ReadRequest.class, this::onReadRequest)
        .match(Message.Client.WriteRequest.class, this::onWriteRequest)
        .match(Message.Coordinator.Update.class, this::onUpdate)
        .match(Message.Replica.UpdateTimeout.class, this::onUpdateTimeout)
        .match(Message.Replica.WriteOkTimeout.class, this::onWriteOkTimeout)
        .match(Message.Coordinator.Heartbeat.class, this::onHeartbeat)
        .match(Message.Replica.HeartbeatTimeout.class, this::onHeartbeatTimeout)
        .match(Message.Coordinator.WriteOk.class, this::onWriteOk)
        .match(Message.System.Crash.class, this::onCrash)
        .match(Message.Coordinator.ChangeReplicaSet.class, this::onChangeReplicaSet)
        .match(Message.Coordinator.HeartbeatPeriod.class, this::onHeartbeatPeriod)
        .match(Message.Replica.HeartbeatResponse.class, this::onHeartbeatResponse)
        .match(Message.Replica.UpdateAck.class, this::onUpdateAck)
        .match(Message.Coordinator.HeartbeatResponseTimeout.class, this::onHeartbeatResponseTimeout)
        .match(Message.Replica.Election.class, this::onElection)
        .match(Message.Replica.ElectionAck.class, this::onElectionAck)
        .match(Message.Replica.ElectionAckTimeout.class, this::onElectionAckTimeout)
        .match(Message.Replica.Coordinator.class, this::onCoordinator)
        .match(Message.Replica.CoordinatorAck.class, this::onCoordinatorAck)
        .match(Message.Replica.CoordinatorAckTimeout.class, this::onCoordinatorAckTimeout)
        .build();
  }

  public Receive election() {
    return receiveBuilder()
        .match(Message.System.JoinGroup.class, this::onJoinGroup)
        .match(Message.Client.ReadRequest.class, this::onReadRequest)
        .match(Message.Coordinator.Update.class, this::onUpdate)
        .match(Message.Replica.UpdateTimeout.class, this::onUpdateTimeout)
        .match(Message.Replica.WriteOkTimeout.class, this::onWriteOkTimeout)
        .match(Message.Coordinator.Heartbeat.class, this::onHeartbeat)
        .match(Message.Replica.HeartbeatTimeout.class, this::onHeartbeatTimeout)
        .match(Message.Coordinator.WriteOk.class, this::onWriteOk)
        .match(Message.System.Crash.class, this::onCrash)
        .match(Message.Coordinator.ChangeReplicaSet.class, this::onChangeReplicaSet)
        .match(Message.Coordinator.HeartbeatPeriod.class, this::onHeartbeatPeriod)
        .match(Message.Replica.HeartbeatResponse.class, this::onHeartbeatResponse)
        .match(Message.Replica.UpdateAck.class, this::onUpdateAck)
        .match(Message.Coordinator.HeartbeatResponseTimeout.class, this::onHeartbeatResponseTimeout)
        .match(Message.Replica.Election.class, this::onElection)
        .match(Message.Replica.ElectionAck.class, this::onElectionAck)
        .match(Message.Replica.ElectionAckTimeout.class, this::onElectionAckTimeout)
        .match(Message.Replica.Coordinator.class, this::onCoordinator)
        .match(Message.Replica.CoordinatorAck.class, this::onCoordinatorAck)
        .match(Message.Replica.CoordinatorAckTimeout.class, this::onCoordinatorAckTimeout)
        .matchAny(msg -> {
          logger.warn("{} is not handled during election", msg);
        })
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

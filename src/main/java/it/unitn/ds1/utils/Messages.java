package it.unitn.ds1.utils;

import akka.actor.ActorRef;
import it.unitn.ds1.utils.Functions.EpochSeqno;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;

public final class Messages {
  private Messages() {
    // Private constructor to prevent instantiation
  }

  public static final class Client {
    private Client() {
    }

    public static class Read implements Serializable {
      public final int replicaId;

      public Read(int replicaId) {
        this.replicaId = replicaId;
      }
    }

    public static class Write implements Serializable {
      public final int replicaId;

      public Write(int replicaId) {
        this.replicaId = replicaId;
      }
    }
  }

  public static class RdRqMsg implements Serializable {
  }

  public static class WrRqMsg implements Serializable {
    public final ActorRef sender;
    public final int operationCount;
    public final int newValue;

    public WrRqMsg(final ActorRef sender, final int operationCount, final int newValue) {
      this.sender = sender;
      this.operationCount = operationCount;
      this.newValue = newValue;
    }
  }

  public static class RdRspMsg implements Serializable {
    public final int value;

    public RdRspMsg(final int value) {
      this.value = value;
    }
  }

  public static class JoinGroupMsg implements Serializable {
    public final List<ActorRef> group;

    public JoinGroupMsg(List<ActorRef> group) {
      this.group = Collections.unmodifiableList(new ArrayList<>(group));
    }
  }

  public static class UpdRqMsg implements Serializable {
    public final ActorRef sender;
    public final int epoch;
    public final int seqno;
    public final int operationCount;
    public final int value;

    public UpdRqMsg(ActorRef sender, int epoch, int seqno, int operationCount, int value) {
      this.sender = sender;
      this.epoch = epoch;
      this.seqno = seqno;
      this.operationCount = operationCount;
      this.value = value;
    }
  }

  public static class AckMsg implements Serializable {
    public final int epoch;
    public final int seqno;
    public final ActorRef sender;

    public AckMsg(int epoch, int seqno, ActorRef sender) {
      this.epoch = epoch;
      this.seqno = seqno;
      this.sender = sender;
    }
  }

  public static class UpdTimeout implements Serializable {
    public final ActorRef sender;
    public final int operationCount;

    public UpdTimeout(final ActorRef sender, final int operationCount) {
      this.sender = sender;
      this.operationCount = operationCount;
    }
  }

  public static class AckTimeout implements Serializable {
    public final int epoch;
    public final int seqno;

    public AckTimeout(final int epoch, final int seqno) {
      this.epoch = epoch;
      this.seqno = seqno;
    }
  }

  public static class ElectionMsg implements Serializable {
    public final ActorRef crashedCoordinator;
    public final Map<ActorRef, EpochSeqno> coordinatorCandidates;

    public ElectionMsg(ActorRef crashedCoordinator, Map<ActorRef, EpochSeqno> group) {
      this.crashedCoordinator = crashedCoordinator;
      this.coordinatorCandidates = new HashMap<>(group);
    }
  }

  public static class ElectionMsgAck implements Serializable {
  }

  public static class CoordinatorMsg implements Serializable {
    public final Map<ActorRef, EpochSeqno> coordinatorCandidates;

    public CoordinatorMsg(Map<ActorRef, EpochSeqno> group) {
      this.coordinatorCandidates = Collections.unmodifiableMap(new HashMap<>(group));
    }
  }

  public static class CoordinatorMsgAck implements Serializable {
  }

  public static class ElectionAckTimeout implements Serializable {
    public final ActorRef nextReplica;
    public final ActorRef crashedCoordinator;
    public final Map<ActorRef, EpochSeqno> coordinatorCandidates;

    public ElectionAckTimeout(final ActorRef nextReplica, final ActorRef crashedCoordinator,
        final Map<ActorRef, EpochSeqno> group) {
      this.nextReplica = nextReplica;
      this.crashedCoordinator = crashedCoordinator;
      this.coordinatorCandidates = Collections.unmodifiableMap(new HashMap<>(group));
    }
  }

  public static class CoordinatorAckTimeout implements Serializable {
    public final ActorRef nextReplica;
    public final Map<ActorRef, EpochSeqno> coordinatorCandidates;

    public CoordinatorAckTimeout(final ActorRef nextReplica, final Map<ActorRef, EpochSeqno> group) {
      this.nextReplica = nextReplica;
      this.coordinatorCandidates = Collections.unmodifiableMap(new HashMap<>(group));
    }
  }

  public static class CoordinatorHeartbeatPeriod implements Serializable {
  }

  public static class CoordinatorHeartbeatTimeout implements Serializable {
  }

  public static class CoordinatorHeartbeatMsg implements Serializable {
  }

  public static class HeartbeatPeriod implements Serializable {
  }

  public static class HeartbeatMsg implements Serializable {
  }

  public static class CrashMsg implements Serializable {
    public final CrashType nextCrash;

    public CrashMsg(CrashType nextCrash) {
      this.nextCrash = nextCrash;
    }
  }

  public static class ChangeReplicaSet implements Serializable {
    public final List<ActorRef> group;

    public ChangeReplicaSet(List<ActorRef> group) {
      this.group = Collections.unmodifiableList(new ArrayList<>(group));
    }
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

  public enum CrashType {
    NONE,
    NotResponding,
    WhileSendingUpdate,
    AfterReceivingUpdate,
    WhileSendingWriteOk,
    WhileElection,
    WhileChoosingCoordinator
  }
}

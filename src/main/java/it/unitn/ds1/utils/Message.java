package it.unitn.ds1.utils;

import akka.actor.ActorRef;
import it.unitn.ds1.utils.Helper.TimeId;

import java.io.Serializable;
import java.util.*;

public final class Message {
  private Message() {
    // Private constructor to prevent instantiation
  }

  // Base message interface
  public interface BaseMessage extends Serializable {
  }

  // Client messages
  public static class Client {
    public static class Read implements BaseMessage {
      public final int replicaId;

      public Read(int replicaId) {
        this.replicaId = replicaId;
      }
    }

    public static class Write implements BaseMessage {
      public final int replicaId;

      public Write(int replicaId) {
        this.replicaId = replicaId;
      }
    }

    public static class ReadRequest implements BaseMessage {
    }

    public static class WriteRequest implements BaseMessage {
      public final ActorRef sender;
      public final int operationCount;
      public final int newValue;

      public WriteRequest(ActorRef sender, int operationCount, int newValue) {
        this.sender = sender;
        this.operationCount = operationCount;
        this.newValue = newValue;
      }
    }
  }

  // Replica messages
  public static class Replica {
    public static class ReadResponse implements BaseMessage {
      public final int value;

      public ReadResponse(int value) {
        this.value = value;
      }
    }

    public static class UpdateTimeout implements BaseMessage {
      public final ActorRef sender;
      public final int operationCount;

      public UpdateTimeout(ActorRef sender, int operationCount) {
        this.sender = sender;
        this.operationCount = operationCount;
      }
    }

    public static class UpdateAck implements BaseMessage {
      public final int epoch;
      public final int seqno;
      public final ActorRef sender;

      public UpdateAck(int epoch, int seqno, ActorRef sender) {
        this.epoch = epoch;
        this.seqno = seqno;
        this.sender = sender;
      }
    }

    public static class WriteOkTimeout implements BaseMessage {
      public final int epoch;
      public final int seqno;

      public WriteOkTimeout(int epoch, int seqno) {
        this.epoch = epoch;
        this.seqno = seqno;
      }
    }

    public static class HeartbeatResponse implements BaseMessage {
    }

    public static class HeartbeatTimeout implements BaseMessage {
    }

    public static class Election implements BaseMessage {
      public final ActorRef crashedCoordinator;
      public final Map<ActorRef, TimeId> coordinatorCandidates;

      public Election(ActorRef crashedCoordinator, Map<ActorRef, TimeId> candidates) {
        this.crashedCoordinator = crashedCoordinator;
        this.coordinatorCandidates = new HashMap<>(candidates);
      }
    }

    public static class ElectionAck implements BaseMessage {
    }

    public static class ElectionAckTimeout implements BaseMessage {
      public final ActorRef nextReplica;
      public final ActorRef crashedCoordinator;
      public final Map<ActorRef, TimeId> coordinatorCandidates;

      public ElectionAckTimeout(ActorRef nextReplica, ActorRef crashedCoordinator,
          Map<ActorRef, TimeId> candidates) {
        this.nextReplica = nextReplica;
        this.crashedCoordinator = crashedCoordinator;
        this.coordinatorCandidates = new HashMap<>(candidates);
      }
    }

    public static class Synchronization implements BaseMessage {
      public final Map<ActorRef, TimeId> coordinatorCandidates;

      public Synchronization(Map<ActorRef, TimeId> candidates) {
        this.coordinatorCandidates = new HashMap<>(candidates);
      }
    }

    public static class SynchronizationAck implements BaseMessage {
    }

    public static class SynchronizationAckTimeout implements BaseMessage {
      public final ActorRef nextReplica;
      public final Map<ActorRef, TimeId> coordinatorCandidates;

      public SynchronizationAckTimeout(ActorRef nextReplica, Map<ActorRef, TimeId> candidates) {
        this.nextReplica = nextReplica;
        this.coordinatorCandidates = new HashMap<>(candidates);
      }
    }
  }

  // Coordinator messages
  public static class Coordinator {
    public static class Update implements BaseMessage {
      public final ActorRef sender;
      public final int epoch;
      public final int seqno;
      public final int operationCount;
      public final int value;

      public Update(ActorRef sender, int epoch, int seqno, int operationCount, int value) {
        this.sender = sender;
        this.epoch = epoch;
        this.seqno = seqno;
        this.operationCount = operationCount;
        this.value = value;
      }
    }

    public static class WriteOk implements BaseMessage {
      public final int epoch;
      public final int seqno;
      public final ActorRef sender;

      public WriteOk(int epoch, int seqno, ActorRef sender) {
        this.epoch = epoch;
        this.seqno = seqno;
        this.sender = sender;
      }
    }

    public static class ChangeReplicaSet implements BaseMessage {
      public final List<ActorRef> group;

      public ChangeReplicaSet(List<ActorRef> group) {
        this.group = new ArrayList<>(group);
      }
    }

    public static class HeartbeatPeriod implements BaseMessage {
    }

    public static class Heartbeat implements BaseMessage {
    }

    public static class HeartbeatResponseTimeout implements BaseMessage {
    }
  }

  // System messages
  public static class System {
    public static class JoinGroup implements BaseMessage {
      public final List<ActorRef> group;

      public JoinGroup(List<ActorRef> group) {
        this.group = new ArrayList<>(group);
      }
    }

    public static class Crash implements BaseMessage {
      public final CrashType crashType;

      public Crash(CrashType crashType) {
        this.crashType = crashType;
      }
    }
  }

  public enum CrashType {
    NONE,
    NOT_RESPONDING,
    WHILE_SENDING_UPDATE,
    AFTER_RECEIVING_UPDATE,
    WHILE_SENDING_WRITE_OK,
    WHILE_ELECTION,
    WHILE_CHOOSING_COORDINATOR
  }
}

package it.unitn.ds1.utils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.ArrayList;
import akka.actor.ActorRef;
import it.unitn.ds1.actors.Replica.CrashType;
import it.unitn.ds1.utils.Functions.EpochSeqno;

public class Messages {

  /*-- Main to Client Message classes ------------------------------------------------------ */
  public static class ClientRead implements Serializable {
    public final int replicaId;

    public ClientRead(int replicaId){
      this.replicaId = replicaId;
    }
  }

  public static class ClientWrite implements Serializable {
    public final int replicaId;

    public ClientWrite(int replicaId){
      this.replicaId = replicaId;
    }
  }

  /*-- Client Message classes ------------------------------------------------------ */
  public static class RdRqMsg implements Serializable {
  }

  public static class WrRqMsg implements Serializable {
    public final ActorRef c_snd;
    public final int op_cnt;
    public final int new_value;

    public WrRqMsg(final ActorRef c_snd, final int op_cnt, final int new_value) {
      this.c_snd = c_snd;
      this.op_cnt = op_cnt;
      this.new_value = new_value;
    }
  }

  /*-- Replica Message classes ------------------------------------------------------ */

  public static class ChangeReplicaSet implements Serializable {
    public final List<ActorRef> group; // an array of group members

    public ChangeReplicaSet(List<ActorRef> group) {
      this.group = Collections.unmodifiableList(new ArrayList<>(group));
    }
  }

  public static class ElectionMsg implements Serializable {
    public final ActorRef crashedCoordinator;
    public final Map<ActorRef, EpochSeqno> coordinatorCandidates; // an array of group members

    public ElectionMsg(ActorRef crashedCoordinator, Map<ActorRef, EpochSeqno> group) {
      this.crashedCoordinator = crashedCoordinator;
      this.coordinatorCandidates = new HashMap<>(group);
    }
  }

  public static class ElectionMsgAck implements Serializable {
  }

  public static class CoordinatorMsg implements Serializable {
    public final Map<ActorRef, EpochSeqno> coordinatorCandidates; // an array of group members

    public CoordinatorMsg(Map<ActorRef, EpochSeqno> group) {
      this.coordinatorCandidates = Collections.unmodifiableMap(new HashMap<>(group));
    }
  }

  public static class CoordinatorMsgAck implements Serializable {
  }


  public static class RdRspMsg implements Serializable {
    public final int v;

    public RdRspMsg(final int v) {
      this.v = v;
    }
  }

  // Start message that informs every participant about its peers
  public static class JoinGroupMsg implements Serializable {
    public final List<ActorRef> group; // an array of group members

    public JoinGroupMsg(List<ActorRef> group) {
      this.group = Collections.unmodifiableList(new ArrayList<>(group));
    }
  }

  public static class SendChatMsg implements Serializable {
  }

  public static class ChatMsg implements Serializable {
    public final Integer viewId;
    public final ActorRef sender;
    public final Integer seqno;
    public final String content;

    public ChatMsg(int viewId, ActorRef sender, int seqno, String content) {
      this.viewId = viewId;
      this.sender = sender;
      this.seqno = seqno;
      this.content = content;
    }
  }

  public static class UpdRqMsg implements Serializable {
    public final ActorRef sender;
    public final int epoch;
    public final int seqno;
    public final int op_cnt;
    public final int value;

    public UpdRqMsg(ActorRef sender, int epoch, int seqno, int op_cnt, int value) {
      this.sender = sender;
      this.epoch = epoch;
      this.seqno = seqno;
      this.op_cnt = op_cnt;
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
    public final ActorRef c_snd;
    public final int op_cnt;

    public UpdTimeout(final ActorRef c_snd, final int op_cnt) {
      this.c_snd = c_snd;
      this.op_cnt = op_cnt;
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

  public static class ElectionAckTimeout implements Serializable {
    public final ActorRef nextReplica;
    public final ActorRef crashed_c;
    public final Map<ActorRef, EpochSeqno> coordinatorCandidates; // an array of group members

    public ElectionAckTimeout(final ActorRef nextReplica, final ActorRef crashed_c, final Map<ActorRef, EpochSeqno> group){
      this.nextReplica = nextReplica;
      this.crashed_c = crashed_c;
      this.coordinatorCandidates = Collections.unmodifiableMap(new HashMap<>(group));
    }
  }

  public static class CoordinatorAckTimeout implements Serializable {
    public final ActorRef nextReplica;
    public final Map<ActorRef, EpochSeqno> coordinatorCandidates; // an array of group members

    public CoordinatorAckTimeout(final ActorRef nextReplica, final Map<ActorRef, EpochSeqno> group){
      this.nextReplica = nextReplica;
      this.coordinatorCandidates = Collections.unmodifiableMap(new HashMap<>(group));
    }
  }

  public static class StableChatMsg implements Serializable {
    public final ChatMsg stableMsg;

    public StableChatMsg(ChatMsg stableMsg) {
      this.stableMsg = stableMsg;
    }
  }

  public static class StableTimeoutMsg implements Serializable {
    public final ChatMsg unstableMsg;
    public final ActorRef sender;

    public StableTimeoutMsg(ChatMsg unstableMsg, ActorRef sender) {
      this.unstableMsg = unstableMsg;
      this.sender = sender;
    }
  }

  public static class ViewChangeMsg implements Serializable {
    public final Integer viewId;
    public final List<ActorRef> proposedView;

    public ViewChangeMsg(int viewId, List<ActorRef> proposedView) {
      this.viewId = viewId;
      this.proposedView = Collections.unmodifiableList(new ArrayList<>(proposedView));
    }
  }

  public static class ViewFlushMsg implements Serializable {
    public final Integer viewId;

    public ViewFlushMsg(int viewId) {
      this.viewId = viewId;
    }
  }

  public static class FlushTimeoutMsg implements Serializable {
    public final Integer viewId;

    public FlushTimeoutMsg(int viewId) {
      this.viewId = viewId;
    }
  }

  public static class CrashMsg implements Serializable {
    public final CrashType nextCrash;

    public CrashMsg(CrashType nextCrash) {
      this.nextCrash = nextCrash;
    }
  }

  public static class RecoveryMsg implements Serializable {
  }

  /*-- Coordinator Messages ------------------------------------------------------ */

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

  public static class CrashReportMsg implements Serializable {
    public final List<ActorRef> crashedMembers;

    public CrashReportMsg(List<ActorRef> crashedMembers) {
      this.crashedMembers = Collections.unmodifiableList(crashedMembers);
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
}
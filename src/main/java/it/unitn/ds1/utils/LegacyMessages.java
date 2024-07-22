package it.unitn.ds1.utils;

import java.io.Serializable;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import akka.actor.ActorRef;

/**
 * Defines the messages used across the distributed system.
 */
public class LegacyMessages {

  public static class AbstractMessage implements Serializable {
    public int sender_id;

    public AbstractMessage(int sender_id) {
      this.sender_id = sender_id;
    }
  }

  /**
   * Message sent to initialize the actors with the list of participants.
   */
  public static class StartMessage implements Serializable {
    public final List<ActorRef> group;

    public StartMessage(List<ActorRef> group) {
      this.group = Collections.unmodifiableList(new ArrayList<>(group));
    }
  }

  // Client messages
  public static class ReadRequest extends AbstractMessage {

    public ReadRequest(int sender_id) {
      super(sender_id);
    }
  }

  public static class ReadResponse extends AbstractMessage {
    public final int value;

    public ReadResponse(int sender_id, int value) {
      super(sender_id);
      this.value = value;
    }
  }

  public static class WriteRequest extends AbstractMessage {
    public int new_value;

    public WriteRequest(int sender_id, int new_value) {
      super(sender_id);
      this.new_value = new_value;
    }
  }

  // Replica messages
  public static class Ack extends AbstractMessage {
    public Ack(int sender_id) {
      super(sender_id);
    }
  }

  public static class UpdateTimeout implements Serializable {
  }

  public static class WriteOkTimeout implements Serializable {
  }

  public static class ReplicaAlive extends AbstractMessage {
    public ReplicaAlive(int sender_id) {
      super(sender_id);
    }
  }

  // Coordinator messages
  public static class UpdateRequest implements Serializable {
    public int new_value;

    public UpdateRequest(int new_value) {
      this.new_value = new_value;
    }
  }

  public static class WriteOk implements Serializable {
  }

  public static class BroadcastTimeout implements Serializable {
  }

  public static class AreYouStillAlive implements Serializable {
  }

  public static class ConfirmationTimeout implements Serializable {
  }
}

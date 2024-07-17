package it.unitn.ds1.messages;

import java.io.Serializable;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import akka.actor.ActorRef;

/**
 * Defines the messages used across the distributed system.
 */
public class Messages {

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
  public static class ReadRequest implements Serializable {
  }

  public static class ReadResponse implements Serializable {
    public final int value;

    public ReadResponse(int value) {
      this.value = value;
    }
  }

  public static class WriteRequest implements Serializable {
    public int new_value;

    public WriteRequest(int new_value) {
      this.new_value = new_value;
    }
  }

  // Replica messages
  public static class Ack implements Serializable {
  }

  public static class UpdateTimeout implements Serializable {
  }

  public static class WriteOkTimeout implements Serializable {
  }

  public static class ReplicaAlive implements Serializable {
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

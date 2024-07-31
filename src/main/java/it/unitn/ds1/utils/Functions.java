package it.unitn.ds1.utils;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.util.Objects;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Functions {

  public final static int DELAYTIME = 100;

  public static class EpochSeqno {
    public final int epoch;
    public final int seqno;

    public EpochSeqno(final int epoch, final int seqno) {
      this.epoch = epoch;
      this.seqno = seqno;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EpochSeqno that = (EpochSeqno) o;
        return epoch == that.epoch && seqno == that.seqno;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, seqno);
    }
  }

  /**
   * This function send a message m to the actor when the time specified ends.
   * 
   * @param context
   * @param time
   * @param actor   The actor that will be the sender and receiver of the Timeout
   *                Message
   * @param m       The message that will be sent
   */
  public static Cancellable setTimeout(ActorContext context, int time, ActorRef actor, Serializable m) {
    return context.system().scheduler().scheduleOnce(
        Duration.create(time, TimeUnit.MILLISECONDS),
        actor,
        m,
        context.system().dispatcher(), actor);
  }

  public static void tellDelay(Serializable m, ActorRef sender, ActorRef receiver) {
    Random rnd = new Random();
    // ActorContext context = getContext();

    // context.system().scheduler().scheduleOnce(
    //     Duration.create(rnd.nextInt(DELAYTIME), TimeUnit.MILLISECONDS),
    //     receiver,
    //     m,
    //     context.system().dispatcher(), sender);

    try {Thread.sleep(rnd.nextInt(DELAYTIME));} catch (Exception ignored) {}
    receiver.tell(m, sender);
  }

  public static void multicast(Serializable m, List<ActorRef> receivers, ActorRef sender) {
    for (ActorRef r : receivers) {
      r.tell(m, sender);
    }
  }

  // Extracts the ID out of a string like
  // Actor[akka://Quorum-based-Total-Order-Broadcast/user/client0#-293215613]
  public static int getId(ActorRef actor) {
    Pattern regex = Pattern.compile("(client|replica)(\\d+)");
    Matcher matcher = regex.matcher(actor.toString());
    if (matcher.find()) {
      return Integer.parseInt(matcher.group(2));
    }
    return -1;
  }

  public static String getName(ActorRef actor) {
    Pattern regex = Pattern.compile("(client|replica)(\\d+)");
    Matcher matcher = regex.matcher(actor.toString());
    if (matcher.find()) {
      return matcher.group(1) + " " + matcher.group(2);
    }
    return null; // Return null or handle the case when no match is found
  }
}

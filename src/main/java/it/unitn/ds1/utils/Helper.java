package it.unitn.ds1.utils;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.Cancellable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import scala.concurrent.duration.Duration;

public final class Helper {
  public static final int DELAY_TIME = 100;

  private static final Pattern ACTOR_NAME_PATTERN = Pattern.compile("(Client|Replica)(\\d+)");

  private Helper() {
    // Private constructor to prevent instantiation
  }

  public static class TimeId {
    public final int epoch;
    public final int seqno;

    public TimeId(final int epoch, final int seqno) {
      this.epoch = epoch;
      this.seqno = seqno;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      TimeId that = (TimeId) o;
      return epoch == that.epoch && seqno == that.seqno;
    }

    @Override
    public int hashCode() {
      return Objects.hash(epoch, seqno);
    }

    @Override
    public String toString() {
      return String.format("TimeId(epoch=%d, seqno=%d)", epoch, seqno);
    }
  }

  public static Cancellable setTimeout(ActorContext context, int timeInMillis, ActorRef actor, Serializable message) {
    return context.system().scheduler().scheduleOnce(
        Duration.create(timeInMillis, "milliseconds"),
        actor,
        message,
        context.system().dispatcher(),
        actor);
  }

  public static void tellDelay(Serializable message, ActorRef sender, ActorRef receiver) {
    int delay = ThreadLocalRandom.current().nextInt(DELAY_TIME);
    try {
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    receiver.tell(message, sender);
  }

  public static void multicast(Serializable message, List<ActorRef> receivers, ActorRef sender) {
    for (ActorRef receiver : receivers) {
      receiver.tell(message, sender);
    }
  }

  public static int getId(ActorRef actor) {
    Matcher matcher = ACTOR_NAME_PATTERN.matcher(actor.toString());
    if (matcher.find()) {
      return Integer.parseInt(matcher.group(2));
    }
    return -1;
  }

  public static String getName(ActorRef actor) {
    Matcher matcher = ACTOR_NAME_PATTERN.matcher(actor.toString());
    if (matcher.find()) {
      return matcher.group(1) + " " + matcher.group(2);
    }
    return null;
  }
}

package it.unitn.ds1.utils;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.Set;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Functions {
  /**
   * This function send a message m to the actor when the time specified ends.
   * 
   * @param context
   * @param time
   * @param actor   The actor that will be the sender and receiver of the Timeout
   *                Message
   * @param m       The message that will be sent
   */
  public static void setTimeout(ActorContext context, int time, ActorRef actor, Serializable m) {
    context.system().scheduler().scheduleOnce(
        Duration.create(time, TimeUnit.MILLISECONDS),
        actor,
        m,
        context.system().dispatcher(), actor);
  }

  public static void multicast(Serializable m, Set<ActorRef> receivers, ActorRef sender) {
    for (ActorRef r : receivers) {
      r.tell(m, sender);
    }
  }

  // Extracts the ID out of a string like
  // Actor[akka://Quorum-based-Total-Order-Broadcast/user/client0#-293215613]
  public static String getId(ActorRef actor) {
    Pattern regex = Pattern.compile("(client|replica)(\\d+)");
    Matcher matcher = regex.matcher(actor.toString());
    if (matcher.find()) {
      return matcher.group(2);
    }
    return null;
  }

  public static String getName(ActorRef actor) {
    Pattern regex = Pattern.compile("(client|replica|coordinator)(\\d*)");
    Matcher matcher = regex.matcher(actor.toString());
    if (matcher.find()) {
      return matcher.group(1) + " " + matcher.group(2);
    }
    return null; // Return null or handle the case when no match is found
  }
}

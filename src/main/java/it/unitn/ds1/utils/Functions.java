package it.unitn.ds1.utils;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.util.List;

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

  public static void multicast(Serializable m, List<ActorRef> receivers, ActorRef sender) {
    for (ActorRef r : receivers) {
      r.tell(m, sender);
    }
  }
}

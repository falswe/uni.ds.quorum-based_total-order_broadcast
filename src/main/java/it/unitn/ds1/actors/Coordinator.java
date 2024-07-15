package it.unitn.ds1.actors;

import java.util.List;

import akka.actor.ActorRef;

public class Coordinator extends Node{

  public Coordinator(int id, List<ActorRef> replicas) {
    super(id, replicas);
  }
  
}

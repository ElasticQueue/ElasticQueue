package mq.http

import akka.actor.Actor

/**
 * Created by Bruce on 3/1/15.
 */
class Consumer extends Actor {
  override def receive: Receive = {
    case m => println(m)
  }
}

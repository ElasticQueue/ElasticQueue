package mq.producer

import akka.actor.{ActorLogging, Actor}
import mq.Messages

/**
 * Created by Bruce on 3/13/15.
 */
class Producer extends Actor with ActorLogging {

  log.info("Consumer path: {}", self.path)

  override def receive: Receive = {
    case (appId: String, topic: String, payload: String) =>
      Messages.enqueue(appId, topic, payload)
    case _ =>
  }
}

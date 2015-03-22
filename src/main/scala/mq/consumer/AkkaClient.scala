package mq.consumer

import akka.actor.Actor.Receive
import akka.actor.{ActorRef, Actor, Props, ActorLogging}
import akka.routing.RoundRobinPool
import mq.consumer.MyJsonProtocol._
import mq.{Consumer, ClientConfig, Message, BrokerActor}
import spray.json.DefaultJsonProtocol
import spray.json.DefaultJsonProtocol
import spray.json._

/**
 * Created by Bruce on 3/1/15.
 */

class AkkaClient extends BrokerActor {

  var akkaClientRouted: ActorRef = _

  def process(m: Message) = {
    akkaClientRouted ! m
  }

  override def start(consumer: Consumer): Unit = {
    akkaClientRouted = context.actorOf(
      Props(new AkkaClientActor(consumer.config)), name = "akkaClientRoutedActor")
  }
}

class AkkaClientActor(config: ClientConfig) extends Actor with ActorLogging {

  var endpoints = List[String]()
  val r = scala.util.Random

  override def preStart() = {
    endpoints = config.endpoints.getOrElse(List())

  }

  def process(m: Message) = {

    val endpointStr = endpoints(r.nextInt(endpoints.length))
    val endpoint = context.actorSelection(endpointStr)

    endpoint ! m
  }

  override def receive: Receive = {
    case m: Message => process(m)
  }
}

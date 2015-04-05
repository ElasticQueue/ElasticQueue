package mq

import akka.actor.{ActorLogging, Actor}
import mq.ClusterProtocol._
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Created by bruce on 25/02/15.
 */
trait BrokerActor extends Actor with ActorLogging {

  import context._

//  var consumerId: String = ""
//  var shardId: Int = 0
//  var appId: String = ""
//  var topic: String = ""
  var offset: String = ""
//  var config: ClientConfig = _
  var count = 0

  var tps = 0L

  var taskQueue = scala.collection.mutable.Queue.empty[Message]

  var start = System.currentTimeMillis()

  system.scheduler.scheduleOnce(1000 millis, self, TickFetch)
  system.scheduler.scheduleOnce(1000 millis, self, Tick)

  def start(consumer: Consumer) = {}

  def process(m: Message)

  def receive = idle()

  def idle(): Receive = {
    case consumer: Consumer =>
      log.info("Init consumer: {}", consumer)
      start(consumer)
      offset = consumer.offset
      context.become(running(consumer))
      println("become running")
  }

  def running(consumer: Consumer): Receive = {

    case TickFetch =>
      // Back preasure
      if(taskQueue.length < 1000) {
        val messageSliceF = Messages.getMsgsF(consumer.appId, consumer.topic, offset, consumer.shardId)
        val messageSlice = Await.result(messageSliceF, 3 second)
        messageSlice.messages.foreach { (m) =>
          taskQueue.enqueue(m)
        }
        offset = messageSlice.end_offset
        Consumers.updateOffset(consumer.consumerId, consumer.shardId, offset)

        if (messageSlice.messages.length > 0) {
          system.scheduler.scheduleOnce(50 millis, self, TickFetch)
        } else {
          system.scheduler.scheduleOnce(1000 millis, self, TickFetch)
        }
      } else {
        system.scheduler.scheduleOnce(1000 millis, self, TickFetch)
      }

      val now = System.currentTimeMillis()
      val sec = (now - start) / 1000
      if(sec > 10) {
        tps = count / sec
        count = 0
        start = now
        log.info("queue size: " + taskQueue.length + " count: " + count + " tps: " + tps)
      }

    case Tick =>

      if(taskQueue.nonEmpty) {
        count = count + 1
        process(taskQueue.dequeue())
      }
      system.scheduler.scheduleOnce(consumer.config.delay.getOrElse(0) millis, self, Tick)

    case _ =>

  }
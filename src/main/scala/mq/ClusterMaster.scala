package mq

import akka.actor._
import akka.cluster.{MemberStatus, Cluster}
import akka.cluster.ClusterEvent._
import mq.util.{HashRing, HashRingNode}
import scala.concurrent.duration._
import mq.ClusterProtocol._

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by bruce on 04/03/15.
 */
object ClusterProtocol {
  case object ShowStatus
  case object Balance
  case class StartConsumer(consumerId: String)
  case class StopConsumer(consumerId: String)

  case class Start(consumer: Consumer)
  case class Stop(consumer: Consumer)
  case object Info
  case object StopAll

  case object Tick
  case object TickFetch
}

class ClusterMaster extends Actor with ActorLogging {

  var count = 0
  val cluster = Cluster(context.system)
  var cancellable = context.system.scheduler.scheduleOnce(1.seconds, self, Balance)

  //context.system.scheduler.schedule(0.seconds, 10.seconds, self, ShowStatus)

  override def preStart(): Unit = {
    log.info("Cluster master is UP")
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def getConsumerNodeRing(cluster: Cluster): HashRing = {
    var nodeList : List[HashRingNode] = List()
    cluster.state.members.filter(_.status == MemberStatus.Up ).map{ (m) =>
      if(m.roles.contains("consumer")) {
        nodeList = HashRingNode(m.address.toString, 50) :: nodeList
      }
    }
    new HashRing(nodeList)
  }

  def receive: Receive = {

    case "hi" => sender() ! "hello"

    case ShowStatus =>
      sender() ! cluster.state.members
      log.debug("ShowStatus - Cluster members: " + cluster.state.members)

    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
      if(!cancellable.isCancelled) cancellable.cancel()
      cancellable = context.system.scheduler.scheduleOnce(1.seconds, self, Balance)

    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)

    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
      if(!cancellable.isCancelled) cancellable.cancel()
        cancellable = context.system.scheduler.scheduleOnce(1.seconds, self, Balance)

    case Balance =>
      log.info("Cluster master begin to rebalancing")
      val members = cluster.state.members.filter(_.status == MemberStatus.Up)
      members.foreach{ (m) =>
        println("ask to stop " + m.address.toString)
        val node = context.actorSelection(m.address.toString + "/user/brokerMaster")
        node ! StopAll
      }

      val consumers = Consumers.getAllConsumers()
      for(consumer <- consumers) {
        consumer.foreach { c =>
          Consumers.updateStatus(c.consumerId, c.shardId, status = false, "")
        }
      }
      var consumerIds = Set[String]()
      for(consumer <- consumers) {
        consumer.foreach { c =>
          if(!consumerIds.contains(c.consumerId)) {
            self ! StartConsumer(c.consumerId)
            consumerIds = consumerIds + c.consumerId
          }
        }
      }

    case StartConsumer(consumerId) =>
      log.info("Begin to start: " + consumerId)
      val nodeRing = getConsumerNodeRing(cluster)

      val consumers = Consumers.getConsumersById(consumerId)
      for(consumer <- consumers) {
        consumer.foreach { (c) =>
          if(!c.status) {
            val addressStr = nodeRing.get(c.consumerId + c.shardId).getOrElse("")
            val node = context.actorSelection(addressStr + "/user/brokerMaster")
            log.info("Start " + c + " at " + addressStr + "/user/brokerMaster")
            node ! Start(c)
            Consumers.updateStatus(c.consumerId, c.shardId, status = true, addressStr)
          }
        }
      }

    case StopConsumer(consumerId) =>
      log.info("Begin to stop: " + consumerId)

      val nodeRing =  getConsumerNodeRing(cluster)

      val consumers = Consumers.getConsumersById(consumerId)
      for(consumer <- consumers) {
        consumer.foreach { (c) =>
          if(c.status) {
            val addressStr = nodeRing.get(c.consumerId + c.shardId).getOrElse("")
            val node = context.actorSelection(addressStr + "/user/brokerMaster")
            log.info("Stop " + c + " at " + addressStr + "/user/brokerMaster")
            node ! Stop(c)
            Consumers.updateStatus(c.consumerId, c.shardId, status = false, "")
          }
        }
      }

    case _: MemberEvent => // ignore
    case _ =>
  }

}

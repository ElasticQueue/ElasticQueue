package mq

import akka.actor._
import akka.cluster.{MemberStatus, Cluster}
import akka.contrib.pattern.ClusterSingletonProxy
import com.typesafe.config.ConfigFactory
import mq.ClusterProtocol._
import mq.util.{RemoteAddressExtension, HashRingNode, HashRing}
import scala.util.Try
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

/**
 * Created by bruce on 25/02/15.
 */
class BrokerMaster extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val timeout = Timeout(5 seconds)

  // http://doc.akka.io/docs/akka/2.3.1/scala/cluster-usage.html#Cluster_Aware_Routers
  val mqMaster = context.actorOf(ClusterSingletonProxy.props(singletonPath = "/user/clusterSingleton/mqMaster",
    role = Some("seed")), name = "mqMasterProxy")

  // node master
  // 1. ask self nodeId -> feedback
  // 2. ask self consumers to run -> feedback
  // 3. ask self consumers to run -> feedback
  // 4. be told consumers to run -> feedback

  // cluster master
  // 1. node list
  // 2. consumer list
  // 3. tell node master to run consumer
  // 4. add new consumer
  // 5. stop old consumer
  // 6. rebalancing
  // 7. stop a node or mark a node standby
  // 8. stop a consumer or mark a consumer pause
  // 9. consistent hash assign jobs
  // 10. consumers/ node management as a queue
  // 11. when cluster master down, will receive cluster from all nodes then recovery old status/ or save cluster status to cassandra and reload
  // 12. cluster status: nodeId -> consumerId list
  // 13. log cluster status changes to cassandra


  // consumer
  // 1. status: pause/run
  // 2. nodeId

  // node
  // 1. status: nodeId
  // 2. capicity
  // 3. consumers Id

  def getConsumerNodeRing(cluster: Cluster): HashRing = {
    var nodeList : List[HashRingNode] = List()
    cluster.state.members.filter(_.status == MemberStatus.Up ).map{ (m) =>
      if(m.roles.contains("consumer")) {
        nodeList = HashRingNode(m.address.toString, 50) :: nodeList
      }
    }
    new HashRing(nodeList)
  }

  // start actor on remote node
  // val one = AddressFromURIString("akka.tcp://sys@host:1234")
  // val ref = system.actorOf(Props[SampleActor].
  // withDeploy(Deploy(scope = RemoteScope(address))))

  def receive: Receive = {

    case consumer: Consumer =>
      val workerId = "consumer-" + consumer.consumerId + "-" + consumer.shardId
      log.info("Bring up consumer: " + workerId)

      // create new consumer
      // consumerType: "mq.consumer.ConsolePrint"
      val worker = context.actorOf(Props(Class.forName(consumer.consumertype).asInstanceOf[Class[BrokerActor]]), workerId)
      context.watch(worker)
      worker ! consumer

      // update db to mark consumer status

    case Start(consumer) =>

      val address = RemoteAddressExtension(context.system).address
      log.debug("Self remote address: {}" + address)

      val nodeRing = getConsumerNodeRing(cluster)

      val nid = nodeRing.get(consumer.consumerId + consumer.shardId)

      if(nid.getOrElse("") == address.toString) {
        log.info("Begin to start consumer: {}", consumer)
        self ! consumer
      }

    case Stop(consumer) =>
      val workerId = "consumer-" + consumer.consumerId + "-" + consumer.shardId
      context.children.foreach { c =>
        if(c.path.name.equals(workerId)) {
          log.info("Begin to stop " + c.path.name + " " + c.path)
          c ! PoisonPill
        }
      }

//    // will not be used
//    case "start" =>
//      println("recalculate the cluster:")
//
//      val address = RemoteAddressExtension(context.system).address
//
//      println("self address: " + address)
//
//      var nodeList : List[HashRingNode] = List()
//      cluster.state.members.filter(_.status == MemberStatus.Up ).map{ (m) =>
//        if(m.roles.contains("consumer")) {
//          nodeList = HashRingNode(m.address.toString, 50) :: nodeList
//        }
//      }
//
//      val nodeRing = new HashRing(nodeList)
//
//      println("node list:" + nodeList)
//
//      val consumers = Consumers.getAllConsumers()
//      for(consumer <- consumers) {
//        consumer.foreach{(c) =>
//          // only start consumer marked to run at this node
//          val nid = nodeRing.get(c.consumerId + c.shardId)
//          //println(c + " -> " + nid.getOrElse(""))
//
//          if(nid.getOrElse("") == address.toString) {
//            println("will start: " + c)
//            self ! c
//          }
//
//        }
//      }

    case Info =>
      val members = cluster.state.members.filter(_.status == MemberStatus.Up)
      log.debug("cluster info: " + members)

      mqMaster ? "hi" onSuccess {
        case resp => println(resp)
      }

      log.debug("Node children: " + context.children.toString())

    case StopAll =>
      context.children.foreach { c =>
        if(c.path.name.startsWith("consumer")) {
          log.info("Begin to stop: " + c.path.name + " " + c.path)
          c ! PoisonPill
        }
      }

    case _ =>

  }
}



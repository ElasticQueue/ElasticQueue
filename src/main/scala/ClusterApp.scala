import akka.actor.{PoisonPill, Props, ActorSystem}
import akka.contrib.pattern.ClusterSingletonManager
import akka.io.IO
import com.typesafe.config.ConfigFactory
import mq._
import mq.admin.AdminApiActor
import mq.http.ApiActor
import mq.producer.Producer
import spray.can.Http

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Created by bruce on 04/03/15.
 */
object ClusterApp {

  // https://github.com/Synesso/scratch-akka-cluster-singleton
  // http://doc.akka.io/docs/akka/snapshot/contrib/cluster-singleton.html
  val seedPorts = Set("2551", "2552")

  def main(args: Array[String]): Unit = {
    startup(args.headOption.getOrElse("0"))
  }

  //sbt "run-main ClusterApp 2551"
  //sbt "run-main ClusterApp 2552"
  //sbt "run-main ClusterApp"

  def startup(port: String): Unit = {
    val roles = if (seedPorts contains port) "roles = [seed,adminApi,api,consumer,producer]" else ""

    val config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port)
      .withFallback(ConfigFactory.parseString(
        s"""
          |akka.cluster {
          |  $roles
          |  role {
          |    seed.min-nr-of-members = 1 // ${seedPorts.size}
          |  }
          |}
         """.stripMargin))
      .withFallback(ConfigFactory.load())

    // Create an Akka system
    implicit val system = ActorSystem("ClusterSystem", config)

    val clusterSingletonProperties = ClusterSingletonManager.props(
      singletonProps = Props(classOf[ClusterMaster]),
      singletonName = "mqMaster",
      terminationMessage = PoisonPill,
      role = None
    )
    // start clusterSingleton
    system.actorOf(clusterSingletonProperties, "clusterSingleton")

    // start node master
    system.actorOf(Props[BrokerMaster], name = "brokerMaster")

    import collection.JavaConversions._
    val nodeRoles = config.getStringList("akka.cluster.roles").toSet

    println("Roles: " + nodeRoles)

    if(nodeRoles.contains("adminApi")) {
      val handler = system.actorOf(Props[AdminApiActor], name = "admin-api")
      IO(Http)(system) ! Http.Bind(handler, "localhost", 4321)
    }

    if(nodeRoles.contains("api")) {
      val handler = system.actorOf(Props[ApiActor], name = "http-api")
      IO(Http)(system) ! Http.Bind(handler, "localhost", 4322)
    }

    if(nodeRoles.contains("producer")) {
      val producer = system.actorOf(Props[Producer], name = "akka-producer")
    }

    if(port == "2551") Init.start()

    sys.addShutdownHook(system.shutdown())

  }
}

object Init {

  def start () {
    // Create tables
    //Await.ready(Queues.create.future(), 2.seconds)
    //Await.ready(Messages.create.future(), 2.seconds)
    //Await.ready(Consumers.create.future(), 2.seconds)

    // Init queue
    Queues.newQueue("app1", "test", 3)
    Consumers.newConsumer("app1", "c1", "test", "mq.consumer.HttpClient",
      ClientConfig(endpoints = Some(List("http://localhost:4322/test/webhook")), isSecret = Some(false) ))

    Consumers.newConsumer("app1", "c1", "test", "mq.consumer.ConsolePrint",
      ClientConfig())


    for (a <- 1 to 1000000) {
      val f = Messages.enqueue("app1", "test", "Hello world " * 20 + a.toString, a)
      val r = Await.result(f, 2.seconds)
      //println(r)
    }
  }
}

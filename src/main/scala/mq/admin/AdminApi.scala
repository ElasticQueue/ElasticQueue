package mq.admin

import akka.cluster.Member
import akka.util.Timeout
import mq.ClusterProtocol.{StopConsumer, StartConsumer, ShowStatus}
import mq._
import spray.httpx.SprayJsonSupport
import spray.json.DefaultJsonProtocol
import spray.routing.{HttpServiceActor, HttpService}
import spray.http.MediaTypes._

import scala.collection.immutable.TreeSet
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global // for futures

import akka.pattern._

/**
 * Created by Bruce on 3/1/15.
 */

case class Node(address: String, status: String)

object HttpNodeJsonProtocol extends DefaultJsonProtocol {
  implicit val httpNodeFormat = jsonFormat2(Node)
}

trait AdminApi extends HttpServiceActor with SprayJsonSupport {

  import scala.concurrent.ExecutionContext.Implicits.global

  val mqMaster = context.actorSelection("/user/brokerMaster/mqMasterProxy")
  implicit val timeout = Timeout(5 seconds)


  import HttpResponseJsonProtocol._

  val admin = {
    get {
      path("api") {
        respondWithMediaType(`application/json`) {
          complete {
            "hello"
          }
        }
      } ~ path("api" / "node") {
          import HttpNodeJsonProtocol._
          complete {
            val future = mqMaster ? ShowStatus
            val members = Await.result(future, timeout.duration).asInstanceOf[TreeSet[Member]]
            var nodes = List[Node]()
            members.foreach{ m =>
              nodes = Node(m.address.toString, m.status.toString) +: nodes
            }
            nodes
          }
      } ~ path("api" / "queue") {
        post {
          entity(as[Queue]) { m =>
            Queues.newQueue(m.appId, m.topic, m.shards)
            complete(m)
          }
        } ~ get {
          // http://127.0.0.1:4322/api/queue?appId=app1&topic=test
          parameter('appId, 'topic) { (appId, topic) => {
            val queue = Queues.getQueue(appId, topic)
            complete(queue)
          }
          }
        }
      } ~ path("api"  / "queue") {
        complete {
          Queues.getAllQueues()
        }
      } ~ path("api" / "consumer") {
        complete {
          Consumers.getAllConsumers()
        }
      } ~ path("api" / "consumer" / Segment) { consumerId =>
        parameter('status.as[Boolean]) { status =>
          if(status) {
            mqMaster ! StartConsumer(consumerId)
          } else {
            mqMaster ! StopConsumer(consumerId)
          }
          complete("OK")

        }


      }
    } ~ path("api"  / "consumer" / Segment) { consumerId =>
      val consumer = Await.result(Consumers.getConsumersById(consumerId), 3 second)
      complete(consumer)
    }

  }

  val adminRoute = admin

}

class AdminApiActor extends AdminApi {

  def receive = runRoute(adminRoute)
}

package mq.http

import akka.actor.Status.Failure
import akka.util.Timeout
import mq._
import spray.httpx.SprayJsonSupport
import spray.httpx.encoding.{NoEncoding, Gzip}
import spray.json.DefaultJsonProtocol
import spray.routing.HttpServiceActor

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.util.Success
import scala.concurrent.ExecutionContext.Implicits.global // for futures


/**
 * Created by bruce on 03/03/15.
 */

case class HttpMessage(
                        appId: String,
                        topic: String,
                        payload: String,
                        hashFactor: Option[Int] = None
                        )

object HttpMessageJsonProtocol extends DefaultJsonProtocol {
  implicit val httpMessageFormat = jsonFormat4(HttpMessage)
}

trait Api extends HttpServiceActor with SprayJsonSupport {

  import HttpMessageJsonProtocol._
  import HttpResponseJsonProtocol._

  implicit val timeout = Timeout(5 seconds)

  val api = {

    path("api" / "queue") {
      get {
        // http://127.0.0.1:4322/api/queue?appId=app1&topic=test
        parameter('appId, 'topic) { (appId, topic) => {
            val queuef = Queues.getQueue(appId, topic)
            complete(queuef)
          }
        }
      }
    } ~ path("api" / "message") {
      // curl -H "Content-Type: application/json" -d '{"appid":"app1","topic":"test", "payload":"hello"}' http://127.0.0.1:4322/api/message
      post {
        entity(as[HttpMessage]) { m =>
          val idf = Messages.enqueue(m.appId, m.topic, m.payload)
          complete(idf)
        }
      } ~ get {
        // http://127.0.0.1:4322/api/message?appId=app1&topic=test&offset=59175f21-c1db-11e4-a7a1-73862d9aef3b&shardId=0
        parameters('appId, 'topic, 'offset, 'shardId.as[Int]) { (appId, topic, offset, shardId) => {

          val messageSlicef = Messages.getHttpMsgs(appId, topic, offset, shardId)
          complete(messageSlicef)
        }
        }
      }
    } ~ path("test" / "webhook") {
      post {
        decompressRequest(Gzip, NoEncoding) {
          entity(as[String]) { m =>
            //println(m)
            complete("OK")
          }
        }
      }
    }

  }

  val apiRoute = api

}

class ApiActor extends Api {

  def receive = runRoute(apiRoute)
}

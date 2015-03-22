package mq.consumer

import java.util.UUID
import akka.actor.Actor.Receive
import akka.actor._
import akka.cluster.Member
import akka.routing.FromConfig
import akka.util.Timeout
import mq.{Consumer, ClientConfig, Message, BrokerActor}
import shapeless.~>
import spray.http.{HttpRequest, HttpResponse, BasicHttpCredentials}
import spray.httpx.encoding.{Deflate, Gzip}
import spray.json.DefaultJsonProtocol
import spray.json._
import spray.httpx.encoding.{Gzip, Deflate}
import spray.client.pipelining._
import akka.routing.RoundRobinPool
import scala.collection.immutable.TreeSet
import scala.collection.mutable._
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout


import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}


/**
 * Created by Bruce on 3/1/15.
 */

case class HttpMessage(
                        appId: String,
                        topic: String,
                        shardId: Int,
                        date: String,
                        id: String,
                        payload: String
                        )

case class HttpConfirm(success: Boolean)

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val messageFormat = jsonFormat6(HttpMessage)
  implicit val confirmFormat = jsonFormat1(HttpConfirm)
}

class HttpClient extends BrokerActor {

  var httpClient: ActorRef = _
  import scala.concurrent.ExecutionContext.Implicits.global

  override def start(consumer: Consumer) = {
    httpClient = context.actorOf(
      Props(new HttpClientActor(consumer.config)), name = "httpClientActor")

  }

  def process(m: Message) = {
    implicit val timeout = Timeout(60.seconds)
    httpClient ? m map {
      case r => //println(r)
    }
  }
}

class HttpClientActor(config: ClientConfig) extends Actor with ActorLogging {


  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val timeout = Timeout(50 seconds)

  val configStr = config
  var pipeline: HttpRequest => Future[HttpResponse] = _
  var endpoints = List[String]()
  val r = scala.util.Random

  override def preStart() = {
    import MyJsonProtocol._
    val httpClientConfig = config

    if(httpClientConfig.isSecret.getOrElse(false)) {
      pipeline = (
        addHeader("X-Client", "ElasticQueue")
          ~> addCredentials(BasicHttpCredentials(httpClientConfig.username.getOrElse(""), httpClientConfig.password.getOrElse("")))
          ~> encode(Gzip)
          ~> sendReceive
          ~> decode(Deflate)
        )

    } else {
      pipeline = (
        addHeader("X-Client", "ElasticQueue")
          ~> encode(Gzip)
          ~> sendReceive
          ~> decode(Deflate)
        )
    }

    endpoints = httpClientConfig.endpoints.getOrElse(List())

  }

  def receive: Receive = {
    case m: Message => process(m)
  }

  def process(m: Message) = {
    import MyJsonProtocol._

    val json = HttpMessage(m.appId, m.topic, m.shardId, m.date.toString, m.id.toString, m.payload).toJson.prettyPrint
    val endpoint = endpoints(r.nextInt(endpoints.length))

    val response: Future[HttpResponse] = pipeline(Post(endpoint, json))

    response onComplete {

      case Success(res) => sender() ! "ok"  //println(res)
      case Failure(e) => sender() ! "error" //println(e)
    }



  }
}

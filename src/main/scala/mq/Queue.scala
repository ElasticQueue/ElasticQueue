package mq

/**
 * Created by bruce on 25/02/15.
 */

import java.util.UUID
import com.datastax.driver.core.utils.UUIDs
import java.util.Date
import com.google.common.util.concurrent.{ListenableFuture, Futures, FutureCallback}
import org.joda.time.LocalDate
import spray.json.DefaultJsonProtocol
import scala.concurrent.duration._

import scala.concurrent.{Promise, Await, Future}
import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import scala.collection.JavaConversions._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.concurrent.ExecutionContext.Implicits.global

case class MessageSlice(
                       start_offset: String,
                       messages: Seq[Message],
                       end_offset: String
                         )


case class Message(
                    appId: String,
                    topic: String,
                    shardId: Int,
                    date: LocalDate,
                    id: UUID,
                    payload: String
                    )

case class MessageId(
                    id: String
                      )

case class HttpMessage(
                        appId: String,
                        topic: String,
                        shardId: Int,
                        date: String,
                        id: String,
                        payload: String
                        )

case class HttpMessageSlice(
                             start_offset: String,
                             messages: List[HttpMessage],
                             end_offset: String,
                             count: Int
                             )


case class ClientConfig(
                         endpoints: Option[List[String]] = None,
                         isSecret: Option[Boolean] = None,
                         username: Option[String] = None,
                         password: Option[String] = None,
                         delay: Option[Int] = None
                         )
case class Consumer(
                     consumerId: String,
                     appId: String,
                     topic: String,
                     shardId: Int,
                     offset: String,
                     consumertype: String,
                     config: ClientConfig,
                     status: Boolean,
                     node: String
                     )

object HttpResponseJsonProtocol extends DefaultJsonProtocol {
  implicit val HttpMessageFormat = jsonFormat6(HttpMessage)
  implicit val HttpMessageSliceFormat = jsonFormat4(HttpMessageSlice)
  implicit val HttpMessageQueueFormat = jsonFormat3(Queue)
  implicit val HttpClientConfigFormat = jsonFormat5(ClientConfig)
  implicit val HttpMessageConsumerFormat = jsonFormat9(Consumer)
  implicit val HttpMesssageIdFormat = jsonFormat1(MessageId)
}

case class Queue(
                appId: String,
                topic: String,
                shards: Int
                  )

object CassandraUtils {

  implicit class RichListenableFuture[T](val lf: ListenableFuture[T]) extends AnyVal {
    def toScalaFuture: Future[T] = {
      val p = Promise[T]
      Futures.addCallback[T](lf, new FutureCallbackAdapter(p))
      p.future
    }
  }

  class FutureCallbackAdapter[V](p: Promise[V]) extends FutureCallback[V] {
    override def onSuccess(result: V): Unit = p success result
    override def onFailure(t: Throwable): Unit = p failure t
  }

}

object Queues {
  import CassandraUtils._

  val tableName = "queue"
  //insert into queue ( appid, topic, shards) values ('app1', 'test', 1);

  def newQueue(appId: String, topic: String, shards: Int) = {
    insertNewQueue(Queue(appId, topic, shards))
  }

  def insertNewQueue(queue: Queue) = {
    val query = {
      QueryBuilder
        .insertInto(tableName)
        .value("appId", queue.appId)
        .value("topic", queue.topic)
        .value("shards", queue.shards)
    }
    Cassandra.session.execute(query)
  }

  def getQueue(appId: String, topic: String): Future[Queue] = {
    val query = {
      QueryBuilder.select().all()
        .from(tableName)
        .where(QueryBuilder.eq("appId", appId))
        .and(QueryBuilder.eq("topic", topic)).limit(1)
    }
    Cassandra.session.executeAsync(query).toScalaFuture.map(rs => queue(rs.one()))
  }

  def queue(row: Row) =
    Queue(row.getString("appId"), row.getString("topic"), row.getInt("shards"))

  def getAllQueues(): Future[List[Queue]] = {
    val query = {
      QueryBuilder.select().all()
        .from(tableName)
    }
    Cassandra.session.executeAsync(query).toScalaFuture.map { rows =>
      rows.map(row => queue(row)).toList
    }
  }

  def getAllQueuesShards(): Map[String, Int] = {
    val fetchQueues = Queues.getAllQueues()
    val queues = Await.result(fetchQueues, 3 second)
    queues.map(queue => queue.appId + "-" + queue.topic -> queue.shards)(collection.breakOut)
  }

}

object Messages {

  import CassandraUtils._

  lazy val tableName = "mq"
  val r = scala.util.Random

  var topicShards = Queues.getAllQueuesShards()

  // random sharding
  def enqueue(appId: String, topic: String, payload: String): Future[MessageId] = {
    val shardId = r.nextInt(topicShards.getOrElse(appId + "-" + topic, 0))
    val message = Message(appId, topic, shardId, new LocalDate(), UUIDs.timeBased(), payload)
    Messages.insertNewRecord(message).map { r =>
      MessageId(message.id.toString)
    }
  }

  // consistent hash sharding
  def enqueue(appId: String, topic: String, payload: String, hashFactor: Int): Future[MessageId] = {
    val shardId = hashFactor % topicShards.getOrElse(appId + "-" + topic, 0)
    val message = Message(appId, topic, shardId, new LocalDate(), UUIDs.timeBased(), payload)
    Messages.insertNewRecord(message).map { r =>
      MessageId(message.id.toString)
    }
  }

  // schedule message
  def enqueueSchedule(appId: String, topic: String, payload: String, timestamp: Long): Future[MessageId] = {
    val shardId = r.nextInt(topicShards.getOrElse(appId + "-" + topic, 0))
    val message = Message(appId, topic, shardId, new LocalDate(timestamp), UUIDs.startOf(timestamp), payload)
    Messages.insertNewRecord(message).map { r =>
      MessageId(message.id.toString)
    }
  }

  // delay message,
  def enqueueDelay(appId: String, topic: String, payload: String, delay: Int): Future[MessageId] = {
    val shardId = r.nextInt(topicShards.getOrElse(appId + "-" + topic, 0))
    val targetTimestamp = new Date().getTime + delay * 1000
    val message = Message(appId, topic, shardId, new LocalDate(targetTimestamp), UUIDs.startOf(targetTimestamp), payload)
    Messages.insertNewRecord(message).map { r =>
      MessageId(message.id.toString)
    }
  }


  def insertNewRecord(message: Message): Future[ResultSet] = {

    val query = {
      QueryBuilder
        .insertInto(tableName)
        .value("appId", message.appId)
        .value("topic", message.topic)
        .value("shardId", message.shardId)
        .value("date", message.date.toDate)
        .value("id", message.id)
        .value("payload", message.payload)
    }
    Cassandra.session.executeAsync(query).toScalaFuture
  }

  def getRecord(appId: String, shardId: Int, topic: String, fromId: String): Future[Seq[Message]] = {

    val uuid = UUID.fromString(fromId)

    val date = new LocalDate((uuid.timestamp() - 0x01b21dd213814000L)/10000)

    val uuidNow = UUIDs.timeBased()

    val query = {
      QueryBuilder.select().all()
        .from(tableName)
        .where(QueryBuilder.eq("appId", appId))
        .and(QueryBuilder.eq("topic", topic))
        .and(QueryBuilder.eq("shardId", shardId))
        .and(QueryBuilder.eq("date", date.toDate))
        .and(QueryBuilder.gt("id", uuid))
        .and(QueryBuilder.lte("id", uuidNow)).limit(100)
    }
    Cassandra.session.executeAsync(query).toScalaFuture.map { rows =>
      rows.map(row => message(row)).toList
    }

  }

  def message(row: Row) =
  Message(row.getString(0), row.getString(1), row.getInt(2), new LocalDate(row.getDate(3)), row.getUUID(4), row.getString(5))

  def getRecordNextDay(appId: String, shardId: Int, topic: String, fromId: String): Future[Seq[Message]] = {

    val uuid = UUID.fromString(fromId)

    val ts = (uuid.timestamp() - 0x01b21dd213814000L)/10000 + 3600*24*1000

    val date = new LocalDate(ts)

    val uuidNow = UUIDs.timeBased()

    //println(s"fetch next day: $ts, $appId, $topic, $date, $uuid")

    val query = {
      QueryBuilder.select().all()
        .from(tableName)
        .where(QueryBuilder.eq("appId", appId))
        .and(QueryBuilder.eq("topic", topic))
        .and(QueryBuilder.eq("shardId", shardId))
        .and(QueryBuilder.eq("date", date.toDate))
        .and(QueryBuilder.gt("id", uuid))
        .and(QueryBuilder.lte("id", uuidNow)).limit(100)
    }
    Cassandra.session.executeAsync(query).toScalaFuture.map { rows =>
      rows.map(row => message(row)).toList
    }

  }

  def getMsgsF(appId: String, topic: String, offset: String, shardId: Int): Future[MessageSlice] = {
    val fetchMessages = Messages.getRecord(appId, shardId, topic, offset)
    var _offset = offset

    fetchMessages.map { msgs =>
      if (msgs.length > 0) {
        _offset = msgs.last.id.toString
        Future { MessageSlice(offset, msgs, _offset) }
      } else {
        val fetchMessagesNextDay = Messages.getRecordNextDay(appId, shardId, topic, offset)
        fetchMessagesNextDay.map { msgsNd =>
          if (msgsNd.length > 0) {
            _offset = msgsNd.last.id.toString
            MessageSlice(offset, msgsNd, _offset)
          } else {
            val uuid = UUID.fromString(offset)
            val DayOffset = new LocalDate((uuid.timestamp() - 0x01b21dd213814000L) / 10000)
            if (DayOffset == new LocalDate()) {
              //    if next day is tomorrow then keep reading today and next day
            } else {
              //    if next day is not tomorrow then jump to next day
              _offset = UUIDs.startOf(UUIDs.unixTimestamp(uuid) + 3600 * 24 * 1000).toString
            }
            MessageSlice(offset, msgsNd, _offset)
          }
        }
      }
    } flatMap(f => f)
  }

  def getMsgs(appId: String, topic: String, offset: String, shardId: Int): MessageSlice = {

    var _offset = offset

    var messages: Seq[Message] = Seq[Message]()

    val fetchMessages = Messages.getRecord(appId, shardId, topic, offset)
    val msgs = Await.result(fetchMessages, 3 second)

    var empty = false

    messages = msgs

    if (msgs.length > 0) {
      _offset = msgs.last.id.toString
    }

    if (msgs.length == 0) {
      // if today is empty then read today and next day
      val fetchMessagesNextDay = Messages.getRecordNextDay(appId, shardId, topic, offset)
      val msgsNextDay = Await.result(fetchMessagesNextDay, 3 second)

      // if next day have data then jump to next day
      if (msgsNextDay.length > 0) {
        messages = msgsNextDay
        _offset = msgsNextDay.last.id.toString
      } else {
        // if next day do not have data also
        val uuid = UUID.fromString(offset)
        val DayOffset = new LocalDate((uuid.timestamp() - 0x01b21dd213814000L) / 10000)
        if (DayOffset == new LocalDate()) {
          //    if next day is tomorrow then keep reading today and next day
        } else {
          //    if next day is not tomorrow then jump to next day
          _offset = UUIDs.startOf(UUIDs.unixTimestamp(uuid) + 3600 * 24 * 1000).toString
        }
        empty = true
      }

    }

    MessageSlice(offset, messages, _offset)
  }

  def getHttpMsgs(appId: String, topic: String, offset: String, shardId: Int): Future[HttpMessageSlice] = {
    val messageSliceF = getMsgsF(appId, topic, offset, shardId)
    messageSliceF.map{ messageSlice =>

      var messages = List[HttpMessage]()
      var count = 0

      messageSlice.messages.foreach { (m) =>
        messages = messages :+ HttpMessage(m.appId, m.topic, m.shardId, m.date.toDate.toString, m.id.toString, m.payload)
        count = count + 1
      }

      HttpMessageSlice(messageSlice.start_offset, messages, messageSlice.end_offset, count)

    }
  }

}

object Consumers {

  import CassandraUtils._

  lazy val tableName = "consumer"
  //insert into consumer (consumerid, appid, offset, topic, shardId) values ('c1', 'app1', '37ebe8d1-beab-11e4-8c6b-9f4fb0fa7538', 'test', 0);

  def newConsumer(appId: String, consumerId: String, topic: String, consumertype: String, config: ClientConfig) = {
    val queueF = Queues.getQueue(appId, topic)
    val queue = Await.result(queueF, 3.seconds)

    for (shardId <- 0 until queue.shards - 1) {
      //"37ebe8d1-beab-11e4-8c6b-9f4fb0fa7538"
      insertNewRecord(Consumer(consumerId, appId, topic, shardId, UUIDs.timeBased().toString, consumertype, config, false, ""))
    }
  }

  def insertNewRecord(consumer: Consumer): Future[ResultSet] = {

    val query = {
      QueryBuilder
        .insertInto(tableName)
        .value("consumerId", consumer.consumerId)
        .value("appId", consumer.appId)
        .value("topic", consumer.topic)
        .value("shardId", consumer.shardId)
        .value("offset", consumer.offset)
        .value("consumertype", consumer.consumertype)
        .value("config", consumer.config)
        .value("status", consumer.status)
        .value("node", consumer.node)

    }
    Cassandra.session.executeAsync(query).toScalaFuture
  }

  def consumer(row: Row) =
  Consumer(row.getString(0), row.getString(1), row.getString(2), row.getInt(3), row.getString(4), row.getString(5),
    parse(row.getString(6)).asInstanceOf[ClientConfig], row.getBool(7), row.getString(8)
  )

  def getRecord(consumerId: String, shardId: Int): Future[List[Consumer]] = {
    val query = {
      QueryBuilder.select().all()
        .from(tableName)
        .where(QueryBuilder.eq("consumerId", consumerId))
        .and(QueryBuilder.eq("shardId", shardId)).limit(1)
    }
    Cassandra.session.executeAsync(query).toScalaFuture.map (rows =>
      rows.map(row => consumer(row)).toList
    )
  }

  def getAllConsumers: Future[Seq[Consumer]] = {

    val query = {
      QueryBuilder.select().all()
        .from(tableName)
    }
    Cassandra.session.executeAsync(query).toScalaFuture.map { rows =>
      rows.map(row => consumer(row)).toList
    }
  }

  def updateOffset(consumerId: String, shardId: Int, offset: String): Future[ResultSet] = {

    val query = {
      QueryBuilder.update(tableName)
        .`with`(QueryBuilder.set("offset", offset))
        .where(QueryBuilder.eq("consumerId", consumerId))
      .and(QueryBuilder.eq("shardId", shardId))
    }
    Cassandra.session.executeAsync(query).toScalaFuture
  }

  def updateStatus(consumerId: String, shardId: Int, status: Boolean, node: String): Future[ResultSet] = {

    val query = {
      QueryBuilder.update(tableName)
        .`with`(QueryBuilder.set("status", status))
        .and(QueryBuilder.set("node", node))
        .where(QueryBuilder.eq("consumerId", consumerId))
        .and(QueryBuilder.eq("shardId", shardId))
    }
    Cassandra.session.executeAsync(query).toScalaFuture
  }

  def getConsumersById(consumerId: String): Future[List[Consumer]] = {
    val query = {
      QueryBuilder.select("consumerId")
        .from(tableName)
        .where(QueryBuilder.eq("consumerId", consumerId))
    }

    Cassandra.session.executeAsync(query).toScalaFuture.map { rows =>
      rows.map(row => consumer(row)).toList
    }
  }
}


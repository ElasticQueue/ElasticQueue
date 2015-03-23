package mq

/**
 * Created by bruce on 25/02/15.
 */

import java.util.UUID
import com.datastax.driver.core.utils.UUIDs
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.Implicits._
import java.util.Date
import com.websudos.phantom.iteratee.Iteratee
import org.joda.time.LocalDate
import spray.httpx.SprayJsonSupport
import spray.json.DefaultJsonProtocol
import scala.concurrent.duration._

import scala.concurrent.{Future => ScalaFuture, Await}
import com.datastax.driver.core.Row

import scala.util.{Failure, Success}
import spray.json._

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

sealed class Messages extends CassandraTable[Messages, Message] {

  object appId extends StringColumn(this) with PartitionKey[String]

  object topic extends StringColumn(this) with PartitionKey[String]

  object shardId extends IntColumn(this) with PartitionKey[Int]

  object date extends DateColumn(this) with PartitionKey[Date]

  object id extends TimeUUIDColumn(this) with PrimaryKey[UUID]

  object payload extends StringColumn(this)

  def fromRow(row: Row): Message = {
    Message(
      appId(row),
      topic(row),
      shardId(row),
      new LocalDate(date(row)),
      id(row),
      payload(row)
    )
  }
}

sealed class Consumers extends CassandraTable[Consumers, Consumer] {

  object consumerId extends StringColumn(this) with PartitionKey[String]

  object shardId extends IntColumn(this) with PrimaryKey[Int]

  object appId extends StringColumn(this)

  object topic extends StringColumn(this)

  object offset extends StringColumn(this)

  object consumertype extends StringColumn(this)

  object config extends JsonColumn[Consumers, Consumer, ClientConfig](this) {
    import HttpResponseJsonProtocol._
    override def fromJson(obj: String): ClientConfig = {
      obj.parseJson.convertTo[ClientConfig]
    }

    override def toJson(obj: ClientConfig): String = {
      obj.toJson.prettyPrint
    }
  }

  object status extends BooleanColumn(this)

  object node extends StringColumn(this)

  def fromRow(row: Row): Consumer = {
    Consumer(
      consumerId(row),
      appId(row),
      topic(row),
      shardId(row),
      offset(row),
      consumertype(row),
      config(row),
      status(row),
      node(row)
    )
  }
}

sealed class Queues extends CassandraTable[Queues, Queue] {

  object appId extends StringColumn(this) with PartitionKey[String]

  object topic extends StringColumn(this) with PrimaryKey[String]

  object shards extends IntColumn(this)

  def fromRow(row: Row): Queue = {
    Queue(
      appId(row),
      topic(row),
      shards(row)
    )
  }
}

object Messages extends Messages with MyConnector {
  override lazy val tableName = "mq"
  val r = scala.util.Random

  var topicShards = Queues.getAllQueuesShards()

  // random sharding
  def enqueue(appId: String, topic: String, payload: String): ScalaFuture[MessageId] = {
    val shardId = r.nextInt(topicShards.getOrElse(appId + "-" + topic, 0))
    val message = Message(appId, topic, shardId, new LocalDate(), UUIDs.timeBased(), payload)
    Messages.insertNewRecord(message).map { r =>
      MessageId(message.id.toString)
    }
  }

  // consistent hash sharding
  def enqueue(appId: String, topic: String, payload: String, hashFactor: Int): ScalaFuture[MessageId] = {
    val shardId = hashFactor % topicShards.getOrElse(appId + "-" + topic, 0)
    val message = Message(appId, topic, shardId, new LocalDate(), UUIDs.timeBased(), payload)
    Messages.insertNewRecord(message).map { r =>
      MessageId(message.id.toString)
    }
  }

  // schedule message
  def enqueueSchedule(appId: String, topic: String, payload: String, timestamp: Long): ScalaFuture[MessageId] = {
    val shardId = r.nextInt(topicShards.getOrElse(appId + "-" + topic, 0))
    val message = Message(appId, topic, shardId, new LocalDate(timestamp), UUIDs.startOf(timestamp), payload)
    Messages.insertNewRecord(message).map { r =>
      MessageId(message.id.toString)
    }
  }

  // delay message,
  def enqueueDelay(appId: String, topic: String, payload: String, delay: Int): ScalaFuture[MessageId] = {
    val shardId = r.nextInt(topicShards.getOrElse(appId + "-" + topic, 0))
    val targetTimestamp = new Date().getTime + delay * 1000
    val message = Message(appId, topic, shardId, new LocalDate(targetTimestamp), UUIDs.startOf(targetTimestamp), payload)
    Messages.insertNewRecord(message).map { r =>
      MessageId(message.id.toString)
    }
  }


  def insertNewRecord(message: Message): ScalaFuture[ResultSet] = {
    insert.value(_.appId, message.appId)
      .value(_.topic, message.topic)
      .value(_.shardId, message.shardId)
      .value(_.date, message.date.toDate)
      .value(_.id, message.id)
      .value(_.payload, message.payload)
      .future()
  }

  def getRecord(appId: String, shardId: Int, topic: String, fromId: String): ScalaFuture[Seq[Message]] = {

    val uuid = UUID.fromString(fromId)

    val date = new LocalDate((uuid.timestamp() - 0x01b21dd213814000L)/10000)

    val uuidNow = UUIDs.timeBased()

    //println(s"fetch today: $appId, $topic, $date, $uuid")

    select.where(_.appId eqs appId).and(_.topic eqs topic).and(_.shardId eqs shardId).and(_.date eqs date.toDate)
      .and(_.id gt uuid).and(_.id lte uuidNow).limit(100).fetch()
  }

  def getRecordNextDay(appId: String, shardId: Int, topic: String, fromId: String): ScalaFuture[Seq[Message]] = {

    val uuid = UUID.fromString(fromId)

    val ts = (uuid.timestamp() - 0x01b21dd213814000L)/10000 + 3600*24*1000

    val date = new LocalDate(ts)

    val uuidNow = UUIDs.timeBased()

    //println(s"fetch next day: $ts, $appId, $topic, $date, $uuid")

    select.where(_.appId eqs appId).and(_.topic eqs topic).and(_.shardId eqs shardId).and(_.date eqs date.toDate)
      .and(_.id gt uuid).and(_.id lte uuidNow).limit(100).fetch()
  }

  def getMsgsF(appId: String, topic: String, offset: String, shardId: Int): ScalaFuture[MessageSlice] = {
    val fetchMessages = Messages.getRecord(appId, shardId, topic, offset)
    var _offset = offset

    fetchMessages.map { msgs =>
      if (msgs.length > 0) {
        _offset = msgs.last.id.toString
        ScalaFuture { MessageSlice(offset, msgs, _offset) }
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

  def getHttpMsgs(appId: String, topic: String, offset: String, shardId: Int): ScalaFuture[HttpMessageSlice] = {
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

object Consumers extends Consumers with MyConnector {
  override lazy val tableName = "consumer"
  //insert into consumer (consumerid, appid, offset, topic, shardId) values ('c1', 'app1', '37ebe8d1-beab-11e4-8c6b-9f4fb0fa7538', 'test', 0);

  def newConsumer(appId: String, consumerId: String, topic: String, consumertype: String, config: ClientConfig) = {
    val fetchQueue = Queues.getQueue(appId, topic)
    val queue = Await.result(fetchQueue, 3 second)
    for (shardId <- 0 to queue.getOrElse(Queue(appId, topic, 0)).shards - 1) {
      //"37ebe8d1-beab-11e4-8c6b-9f4fb0fa7538"
      insertNewRecord(Consumer(consumerId, appId, topic, shardId, UUIDs.timeBased().toString, consumertype, config, false, ""))
    }
  }

  def insertNewRecord(consumer: Consumer): ScalaFuture[ResultSet] = {
    insert.value(_.consumerId, consumer.consumerId)
      .value(_.appId, consumer.appId)
      .value(_.topic, consumer.topic)
      .value(_.shardId, consumer.shardId)
      .value(_.offset, consumer.offset)
      .value(_.consumertype, consumer.consumertype)
      .value(_.config, consumer.config)
      .value(_.status, consumer.status)
      .value(_.node, consumer.node)
      .future()
  }

  def getRecord(consumerId: String, shardId: Int): ScalaFuture[Option[Consumer]] = {
    select.where(_.consumerId eqs consumerId).and(_.shardId eqs shardId).one()
  }

  def getAllConsumers(): ScalaFuture[Seq[Consumer]] = {
    select.fetchEnumerator() run Iteratee.collect()
  }

  def updateOffset(consumerId: String, shardId: Int, offset: String): ScalaFuture[ResultSet] = {
    update.where(_.consumerId eqs consumerId).and(_.shardId eqs shardId).modify(_.offset setTo offset).future()
  }

  def updateStatus(consumerId: String, shardId: Int, status: Boolean, node: String): ScalaFuture[ResultSet] = {
    update.where(_.consumerId eqs consumerId).and(_.shardId eqs shardId).modify(_.status setTo status).and(_.node setTo node).future()
  }

  def getConsumersById(consumerId: String): ScalaFuture[Seq[Consumer]] = {
    select.where(_.consumerId eqs consumerId).fetchEnumerator() run Iteratee.collect()
  }
}

object Queues extends Queues with MyConnector {
  override lazy val tableName = "queue"
  //insert into queue ( appid, topic, shards) values ('app1', 'test', 1);

  def newQueue(appId: String, topic: String, shards: Int): ScalaFuture[ResultSet] = {
    insertNewQueue(Queue(appId, topic, shards))
  }

  def insertNewQueue(queue: Queue): ScalaFuture[ResultSet] = {
    insert.value(_.appId, queue.appId)
      .value(_.topic, queue.topic)
      .value(_.shards, queue.shards)
      .future()
  }

  def getQueue(appId: String, topic: String): ScalaFuture[Option[Queue]] = {
    select.where(_.appId eqs appId).and(_.topic eqs topic).one()
  }

  def getAllQueues(): ScalaFuture[Seq[Queue]] = {
    select.fetchEnumerator() run Iteratee.collect()
  }

  def getAllQueuesShards(): Map[String, Int] = {
    val fetchQueues = Queues.getAllQueues()
    val queues = Await.result(fetchQueues, 3 second)
    queues.map(queue => queue.appId + "-" + queue.topic -> queue.shards)(collection.breakOut)
  }

}
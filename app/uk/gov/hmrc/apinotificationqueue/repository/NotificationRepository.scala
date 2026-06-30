/*
 * Copyright 2023 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.hmrc.apinotificationqueue.repository

import com.google.inject.ImplementedBy
import com.mongodb.client.model.Indexes.{ascending, descending}
import com.mongodb.client.model.Projections._
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Updates._
import org.bson.UuidRepresentation
import org.bson.codecs.UuidCodec
import org.bson.conversions.Bson
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model._
import org.mongodb.scala.result.{DeleteResult, InsertOneResult}
import play.api.libs.json.{JsNull, Json}
import uk.gov.hmrc.apinotificationqueue.model.NotificationStatus._
import uk.gov.hmrc.apinotificationqueue.model._
import uk.gov.hmrc.customs.api.common.logging.CdsLogger
import uk.gov.hmrc.mongo.MongoComponent
import uk.gov.hmrc.mongo.MongoUtils.DuplicateKey
import uk.gov.hmrc.mongo.play.json.{Codecs, PlayMongoRepository}

import java.util.UUID
import java.util.concurrent.TimeUnit
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@ImplementedBy(classOf[NotificationMongoRepository])
trait NotificationRepository {
  def save(clientId: String, notification: Notification): Future[Notification]

  def update(clientId: String, notification: Notification): Future[Notification]

  def fetch(clientId: String, notificationId: UUID): Future[Option[Notification]]

  def fetchNotificationIds(clientId: String, notificationStatus: Option[NotificationStatus.Value]): Future[List[NotificationWithIdOnly]]

  def fetchNotificationIds(clientId: String, conversationId: UUID, notificationStatus: NotificationStatus.Value): Future[List[NotificationWithIdOnly]]

  def fetchNotificationIds(clientId: String, conversationId: UUID): Future[List[NotificationWithIdAndPulled]]

  def fetchOverThreshold(threshold: Int): Future[List[ClientOverThreshold]]

  def delete(clientId: String, notificationId: UUID): Future[Boolean]

  def deleteAll(): Future[Unit]

  def jsonToBson(json: (String, Json.JsValueWrapper)*): BsonValue = {
    Codecs.toBson(Json.obj(json :_*))
  }
}

@Singleton
class NotificationMongoRepository @Inject()(mongo: MongoComponent,
                                            cdsLogger: CdsLogger,
                                            config: ApiNotificationQueueConfig)(implicit ec: ExecutionContext)
  extends PlayMongoRepository[ClientNotification](
    mongoComponent = mongo,
    collectionName = "notifications",
    domainFormat = ClientNotification.ClientNotificationJF,
    replaceIndexes = true,
    indexes = Seq(
      IndexModel(
        keys = ascending("clientId"),
        indexOptions = IndexOptions()
          .name("clientId-Index")
          .unique(false)
      ),
      IndexModel(
        keys = ascending("clientId", "notification.notificationId"),
        indexOptions = IndexOptions()
          .name("clientId-notificationId-Index")
          .unique(true)
      ),
      IndexModel(
        keys = ascending("clientId", "notification.datePulled"),
        indexOptions = IndexOptions()
          .name("clientId-datePulled-Index")
          .unique(false)
      ),
      IndexModel(
        keys = ascending("clientId", "notification.conversationId"),
        indexOptions = IndexOptions()
          .name("clientId-conversationId-Index")
          .unique(false)
      ),
      IndexModel(
        keys = ascending("clientId", "notification.conversationId", "notification.datePulled"),
        indexOptions = IndexOptions()
          .name("clientId-conversationId-datePulled-Index")
          .unique(false)
      ),
      IndexModel(
        keys = descending("notification.dateReceived"),
        indexOptions = IndexOptions()
          .name("dateReceived-Index")
          .unique(false)
          .expireAfter(config.ttlInSeconds.toLong, TimeUnit.SECONDS)
      )
    ),
    extraCodecs = Seq(
      new UuidCodec(UuidRepresentation.STANDARD),
      Codecs.playFormatCodec(NotificationWithIdAndPulled.notificationWithIdAndPulledStatusJF),
      Codecs.playFormatCodec(NotificationWithIdOnly.notificationWithIdOnlyJF),
      Codecs.playFormatCodec(ClientOverThreshold.ClientOverThresholdJF)
    )
  )
    with NotificationRepository {

    private val uniqueIndex = "clientId-notificationId-Index"
    dropInvalidIndexes()

    def handleError(e: Exception, errorLogMessage: String): Nothing = {
    lazy val errorMsg = errorLogMessage + s"\n ${e.getMessage}"
    cdsLogger.error(errorMsg)
    throw new RuntimeException(errorMsg)
    }

    override def save(clientId: String, notification: Notification): Future[Notification] = {
    cdsLogger.debug(s"saving clientId: [$clientId]'s notification: [$notification]")

    val clientNotification = ClientNotification(clientId, notification)

    lazy val errorMsg = s"Notification not saved for client: [$clientId] notification: [$notification]"

    collection.insertOne(clientNotification).toFuture().map {
      case result: InsertOneResult if result.wasAcknowledged() =>
        notification
    }.recover {
      case DuplicateKey(d) =>
        cdsLogger.error(s"Duplicate Key [$uniqueIndex] [$errorMsg]", d)
        notification
      case e: Exception =>
        handleError(e, errorMsg)
    }
  }

    override def update(clientId: String, notification: Notification): Future[Notification] = {
    cdsLogger.debug(s"updating clientId: [$clientId]'s, notificationId: [${notification.notificationId}]")

    val query: Bson = and(
      equal("clientId", clientId),
      equal("notification.notificationId", Codecs.toBson(notification.notificationId))
    )

    val update = combine(
      Updates.set("notification", Codecs.toBson(notification))
    )

    collection.findOneAndUpdate(
      filter = query,
      update = update,
      options = FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
    ).toFutureOption().map {
      case Some(clientNotification: ClientNotification) =>
        clientNotification.notification
      case None =>
        lazy val errorLogMessage = s"Notification not found. clientId: [$clientId], notificationId: [${notification.notificationId}]"
        handleError(new RuntimeException(errorLogMessage), errorLogMessage)
    }.recoverWith {
      case e =>
        lazy val errorLogMessage = s"Notification not updated for clientId [$clientId], notificationId: [${notification.notificationId}]"
        lazy val errorMsg = errorLogMessage + s"\n ${e.getMessage}"
        cdsLogger.error(errorMsg)
        Future.failed(e)
    }
  }

    override def fetch(clientId: String, notificationId: UUID): Future[Option[Notification]] = {

    val filter = and(
      equal("clientId", clientId),
      equal("notification.notificationId", Codecs.toBson(notificationId))
    )

    collection.find(filter).headOption().map(_.map(_.notification))
  }

    override def fetchOverThreshold(threshold: Int): Future[List[ClientOverThreshold]] = {

    val groupByClientId = {

      Aggregates.group(
        id = jsonToBson("clientId" -> "$clientId"),
        fieldAccumulators =
          Accumulators.sum("notificationTotal", 1),
        Accumulators.min("oldestNotification", "$notification.dateReceived"),
        Accumulators.max("latestNotification", "$notification.dateReceived")
      )
    }

    val filterByThreshold = Aggregates.filter(
      gte("notificationTotal", threshold)
    )

    val projection =
      project(fields(
        computed("_id", 0),
        computed("clientId", "$_id.clientId"),
        include("notificationTotal", "oldestNotification", "latestNotification")
      ))

    collection.aggregate[ClientOverThreshold](
      pipeline = Seq(groupByClientId, filterByThreshold, projection)
    ).toFuture().map(_.toList)
  }

    override def delete(clientId: String, notificationId: UUID): Future[Boolean] = {

    val filter = and(
      equal("clientId", clientId),
      equal("notification.notificationId", Codecs.toBson(notificationId))
    )

    collection.deleteOne(filter = filter).toFuture()
      .map(deleteResult => deleteResult.getDeletedCount > 0)
      .recover {
        case e: Exception =>
          handleError(e, s"Could not delete entity for clientId: [$clientId], notificationId: [$notificationId]")
      }
  }

    override def fetchNotificationIds(clientId: String, notificationStatus: Option[NotificationStatus.Value]): Future[List[NotificationWithIdOnly]] = {

    def notificationExistsFilter(isPulled: Boolean): Bson = {
      and(
        equal("clientId", clientId),
        exists("notification.datePulled", isPulled)
      )
    }

    val filter = Aggregates.filter(
      notificationStatus match {
        case Some(Pulled) => notificationExistsFilter(true)
        case Some(Unpulled) => notificationExistsFilter(false)
        case _ => equal("clientId", clientId)
      }
    )

    val projection: Bson =
      project(fields(
        computed("notification.notificationId", 1),
        computed("_id", 0)
      ))

    collection.aggregate[NotificationWithIdOnly](
      pipeline = Seq(filter, projection)
    ).hintString("clientId-Index").toFuture().map(_.toList)
  }

    override def fetchNotificationIds(clientId: String, conversationId: UUID, notificationStatus: NotificationStatus.Value): Future[List[NotificationWithIdOnly]] = {

    def notificationFilter(isPulled: Boolean): Bson = {
      Aggregates.filter(
        and(
          equal("clientId", clientId),
          equal("notification.conversationId", Codecs.toBson(conversationId)),
          exists("notification.datePulled", isPulled)
        )
      )
    }

    val filter = notificationStatus match {
      case Pulled => notificationFilter(true)
      case Unpulled => notificationFilter(false)
    }

    val projection: Bson =
      project(fields(
        computed("notification.notificationId", 1),
        computed("_id", 0)
      ))

    collection.aggregate[NotificationWithIdOnly](
      pipeline = Seq(filter, projection)
    ).toFuture().map(_.toList)
  }

    override def fetchNotificationIds(clientId: String, conversationId: UUID): Future[List[NotificationWithIdAndPulled]] = {

    val filter: Bson = {
      Aggregates.filter(
        and(
          equal("clientId", clientId),
          equal("notification.conversationId", Codecs.toBson(conversationId))
        )
      )
    }

    val projection: Bson =
      project(fields(
        computed("_id", 0),
        computed("notification", 1),
        computed("pulled", jsonToBson("$gt" -> Json.arr("$notification.datePulled", JsNull)))
      ))

    collection.aggregate[BsonValue](
      pipeline = Seq(filter, projection)
    ).toFuture().map(_.toList.map(Codecs.fromBson[NotificationWithIdAndPulled]))
  }

    override def deleteAll(): Future[Unit] = {
    cdsLogger.debug(s"deleting all notifications")
    collection.deleteMany(
      exists("clientId")
    ).toFuture().map {(deleteResult: DeleteResult) =>
      cdsLogger.debug(s"deleted ${deleteResult.getDeletedCount} notifications")
    }
  }

    private def dropInvalidIndexes(): Future[_] = {
    collection.listIndexes[IndexModel]().toFuture().map { (indexes: Seq[IndexModel]) =>
      indexes.find { (index: IndexModel) =>
        val indexName = index.getOptions.getName
        val verifyIndexName = indexName.contains("ttlIndexName")
        val verifyIndexExpiry = index.getOptions.getExpireAfter(TimeUnit.SECONDS).toInt != config.ttlInSeconds
        verifyIndexName && verifyIndexExpiry
      }.map { (index) =>
        val indexName = index.getOptions.getName
        cdsLogger.debug(s"dropping [$indexName] index as ttl value is incorrect")
        collection.dropIndex(indexName)
      }
        .getOrElse(Future.successful(()))
    }
  }
}

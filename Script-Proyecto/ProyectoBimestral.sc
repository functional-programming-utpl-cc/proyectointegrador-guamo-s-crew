// Importación de librerias
import java.io.File
// Manejo de Fecha
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.collection.immutable.ListMap

//Kantan
import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._
import kantan.csv.java8.localDateTimeDecoder

//Try
import scala.util.{Try,Success,Failure}

//Ruta para acceder al archivo csv

val path2DataFile = "C:\\Users\\Usuario iTC\\Documents" +
  "\\programacion_funcional\\proyectoFinal\\ods_2_1 (2).csv"

//Formato de la fecha empleada en el DataSet
val formatDateTime = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")

//Creación de la clase Tweet donde se almacenara la información del dataSet

case class Tweet (
                   idStr: String,
                   fromUser: String,
                   text: String,
                   createdAt: String,
                   time: LocalDateTime,
                   geoCoordinates: String,
                   userLang: String,
                   inReply2UserId: String,
                   inReply2ScreenName: String,
                   fromUserId: String,
                   inReply2StatusId: String,
                   source: String,
                   profileImageURL: String,
                   userFollowersCount: Double,
                   userFriendsCount: Double,
                   userLocation: String,
                   statusURL: String,
                   entitiesStr: String
                 )
//Cambio del formato por defecto de la fecha LocalDateTime por la establecida en formatDateTime
implicit val decoder : CellDecoder[LocalDateTime] = localDateTimeDecoder(formatDateTime)

//Creación de un nuevo archivo con la infomación del DataSet
val dataSource = new File(path2DataFile).readCsv[List, Tweet](rfc.withHeader)

// Selección de los archivos útiles del DataSet
val values = dataSource.collect({ case Right(tweet) => tweet })

//first Sentence: Tweets y Retweets por día

// Retweets por día

//Filtrado de la columna text y agrupación por día
val retweetperDay = ListMap(values.filter(tweet => tweet.text.startsWith("RT"))
  .map(tweet => tweet.time.getDayOfMonth).groupBy(identity).map({case(k, v) => (k, v.length)})
  .toSeq.sortWith(_._1 > _._1): _*)

//Creación de un archivo Temporal
val out2 = java.io.File.createTempFile("retweetperDay.csv", "csv")
val retweetcsv = out2.asCsvWriter[(Int, Int)](rfc.withHeader("Day", "Retweet N") )
retweetperDay.foreach(retweetcsv.write(_) )
retweetcsv.close()

// Tweets por día

//Filtrado de la columna text y agrupación por día
val tweetperDay = ListMap(values.filterNot(tweet => tweet.text.startsWith("RT"))
  .map(tweet => tweet.time.getDayOfMonth).groupBy(identity).map({case(k, v) => (k, v.length)})
  .toSeq.sortWith(_._1 > _._1): _*)

//Creación de un archivo Temporal
val out3 = java.io.File.createTempFile("tweetperDay.csv", "csv")
val tweetcsv = out3.asCsvWriter[(Int, Int)](rfc.withHeader("Day", "Tweet N") )
tweetperDay.foreach(tweetcsv.write(_) )
tweetcsv.close()


//Second Sentence: Tweets y Retweets por hora


//Retweets por hora

//Filtrado de la columna text y agrupación por hora
val retweetperHour = ListMap(values.filter(tweet => tweet.text.startsWith("RT"))
  .map(tweet => tweet.time.getHour).groupBy(identity).map({case(k, v) => (k, v.length)})
  .toSeq.sortWith(_._1 > _._1): _*)

//Creación de un archivo Temporal
val out4 = java.io.File.createTempFile("retweetperHour.csv", "csv")
val retweetHourcsv = out4.asCsvWriter[(Int, Int)](rfc.withHeader("Hour", "Retweet N") )
retweetperHour.foreach(retweetHourcsv.write(_) )
retweetHourcsv.close()

//Tweets por hora

//Filtrado de la columna text y agrupación por
val tweetperHour = ListMap(values.filterNot(tweet => tweet.text.startsWith("RT"))
  .map(tweet => tweet.time.getHour).groupBy(identity).map({case(k, v) => (k, v.length)})
  .toSeq.sortWith(_._1 > _._1): _*)

//Creación de un archivo Temporal
val out5 = java.io.File.createTempFile("tweetperHour.csv", "csv")
val tweetHourcsv = out5.asCsvWriter[(Int, Int)](rfc.withHeader("Hour", "Tweet N") )
tweetperHour.foreach(tweetHourcsv.write(_) )
tweetHourcsv.close()

// Third Sentence: Aplicaciones más usadas para publicar

// Filtrado de la columna source y agrupación por caracteres semejantes
val apps = ListMap(values.filterNot(tweet => tweet.text.startsWith("RT"))
  .map(tweet => tweet.source.split (">")(1).replace("</a", "")).groupBy(identity)
  .map({case(k, v) => (k, v.length)})
  .toSeq.sortWith(_._2 > _._2): _*)

//Creación de un archivo temporal
val outApps = java.io.File.createTempFile("apps.csv", "csv")
val appsCsv = outApps.asCsvWriter[(String, Int)](rfc.withHeader("Aplicacion", "Cantidad de Usuarios") )
apps.foreach(appsCsv.write(_))
appsCsv.close()

// Fourth Sentence: Distribución de hashtags

val hashtagsN = values.map(tweet => ujson.read(tweet.entitiesStr).obj("hashtags").arr.length)
  .groupBy(identity).map({case (k, v) => (k, v.length)})

// Creación de un archivo temporal
val outHashtags = java.io.File.createTempFile("hashtagsN.csv", "csv")

val writerHashtags = outHashtags.asCsvWriter[(Int, Int)](rfc.withHeader("hashtagN", "count"))

hashtagsN.foreach(writerHashtags.write(_))

writerHashtags.close()

// Fifth Sentence: Distribución de menciones
val mentionsN = values.map(tweet => ujson.read(tweet.entitiesStr).obj("user_mentions").arr.length)
  .groupBy(identity).map({case (k, v) => (k, v.length)})

// Creación de un archivo temporal
val outMentions = java.io.File.createTempFile("mentionN.csv", "csv")

val writerMentions = outMentions.asCsvWriter[(Int, Int)](rfc.withHeader("mentionN", "count"))

mentionsN.foreach(writerMentions.write(_))

writerMentions.close()

// Sixth Sentence: Distribución de urls
val urlN = values.map(tweet => ujson.read(tweet.entitiesStr).obj("urls").arr.length)
  .groupBy(identity).map({case (k, v) => (k, v.length)})

// Creación de un archivo temporal
val outURL = java.io.File.createTempFile("urlN.csv", "csv")

val writerURL = outURL.asCsvWriter[(Int, Int)](rfc.withHeader("urlN", "count"))

urlN.foreach(writerURL.write(_))

writerURL.close()

// Seven Sentence:

/*

// Manejo de json y uso del objeto media usando Try
val mediaN = values.map(tweet => Try{ujson.read(tweet.entitiesStr).obj("media").arr.length}).map(result => result match{
  case Success(s) => s
  case Failure(e) => 0
}).groupBy(identity).map({case (k, v) => (k, v.length)})

//Creación de un archivo temporal
val outMedia = java.io.File.createTempFile("media.csv", "csv")
val writerMedia = outMedia.asCsvWriter[(Int, Int)](rfc.withHeader("mediaN", "count"))
mediaN.foreach(writerMedia.write(_))
writerMedia.close()
*/

//Eighth Sentence: Cálculo Covarianza de correlación de Pearson

/*
//Cálculo de la media marginal
val friendsMarginal = values.map(tweet => tweet.userFriendsCount).sum /
  values.length.toDouble
val followersMarginal = values.map(tweet => tweet.userFollowersCount).sum /
  values.length.toDouble

//Cálculo de la desviación
val friendsDesv =math.sqrt((values.map(tweet => math.pow(tweet.userFriendsCount,2)).sum) /
  values.length.toDouble -
  math.pow(friendsMarginal,2))
val followersDesv =math.sqrt((values.map(tweet => math.pow(tweet.userFollowersCount,2)).sum) /
  values.length.toDouble -
  math.pow(followersMarginal,2))

//Cálculo de la covarianza
val covarianza = values.map(tweet => tweet.userFriendsCount * tweet.userFollowersCount).sum /
values.length - friendsMarginal * followersMarginal

//Cálculo Covarianza de correlación de Pearson
val pearson = covarianza / (friendsDesv * followersDesv)

//Coordenas para el gráfico
val friendsFollowers = values.map(tweet => (tweet.userFriendsCount, tweet.userFollowersCount))

//Creación de un archivo temporal
val outPearson = java.io.File.createTempFile("pearson.csv", "csv")
val writerPearson = outPearson.asCsvWriter[(Double, Double)](rfc.withHeader("x", "y"))
friendsFollowers.foreach(writerPearson.write(_))
writerPearson.close()
*/

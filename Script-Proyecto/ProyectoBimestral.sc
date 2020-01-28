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

/*
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
*/

//Second Sentence: Tweets y Retweets por hora

/*
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
*/

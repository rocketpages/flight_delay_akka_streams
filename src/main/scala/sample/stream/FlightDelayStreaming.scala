package sample.stream

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer }
import akka.stream.scaladsl._
import FlowGraph.Implicits._
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }
import scala.concurrent.ExecutionContext.Implicits._

object FlightDelayStreaming {

  // implicit actor system
  implicit val system = ActorSystem("Sys")

  // implicit actor flow materializer
  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {

    val printSink = Sink.ignore // TODO output delay records to a SQL database using Slick
    val countSink = Sink.ignore

    // read lines from a log file
    val flightDelayFile = io.Source.fromFile("src/main/resources/2008.csv", "utf-8")

    // @formatter:off
    val materializedGraph = FlowGraph.closed(printSink, countSink)((_, countSink) => countSink) { implicit builder =>
      (ps, cs) =>
        val bcast = builder.add(Broadcast[DelayRecord](2))
        Source(() => flightDelayFile.getLines()) ~>
          csvToFlightEvent ~> flightEventToDelayRecord ~>
          bcast ~> ps.inlet
          bcast ~> countByCarrier ~> cs.inlet
    }.run()
    // @formatter:on

    // ensure the output file is closed and the system shutdown upon completion
    materializedGraph.onComplete {
      case Success(_) =>
        system.shutdown()
      case Failure(e) =>
        println(s"Failure: ${e.getMessage}")
        system.shutdown()
    }

  }

  // immutable flow step (val, evaluated only once)
  val csvToFlightEvent = Flow[String].map(csv => csv.split(",").map(_.trim)).map(stringArrayToDelayRecord)

  // map a string array to FlightEvent
  def stringArrayToDelayRecord(cols: Array[String]) = new FlightEvent(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6), cols(7), cols(8), cols(9), cols(10), cols(11), cols(12), cols(13), cols(14), cols(15), cols(16), cols(17), cols(18), cols(19), cols(20), cols(21), cols(22), cols(23), cols(24), cols(25), cols(26), cols(27), cols(28))

  // transform FlightEvents to DelayRecords (only for records with a delay)
  val flightEventToDelayRecord = Flow[FlightEvent]
    .filter(r => Try(r.arrDelayMins.toInt).getOrElse(-1) > 0) // convert arrival delays to ints, filter out non delays
    .mapAsync(parallelism = 2) { r => Future(new DelayRecord(r.year, r.month, r.dayOfMonth, r.flightNum, r.uniqueCarrier, r.arrDelayMins)) } // output a DelayRecord

  val countByCarrier = Flow[DelayRecord]
    .groupBy(_.uniqueCarrier) // groupBy returns a key value and a producer
    .map {
      case (carrier, producer) =>
        producer.runWith(foldTotalDelayMinutesSink) // the producer for each group stream must be materialized and run
    }

  val foldTotalDelayMinutesSink = Sink.fold((0, 0)) { (x: (Int, Int), y: DelayRecord) =>
    println(s"Delays for carrier ${y.uniqueCarrier}: ${Try(x._2 / x._1).getOrElse(0)} average mins, ${x._1} delayed flights") // TODO emit as events for another graph, perhaps event storage + SSE
    val count = x._1 + 1
    val totalMins = x._2 + Try(y.arrDelayMins.toInt).getOrElse(0)
    (count, totalMins)
  }

}

case class FlightEvent(
  year: String,
  month: String,
  dayOfMonth: String,
  dayOfWeek: String,
  depTime: String,
  scheduledDepTime: String,
  arrTime: String,
  scheduledArrTime: String,
  uniqueCarrier: String,
  flightNum: String,
  tailNum: String,
  actualElapsedMins: String,
  crsElapsedMins: String,
  airMins: String,
  arrDelayMins: String,
  depDelayMins: String,
  originAirportCode: String,
  destinationAirportCode: String,
  distanceInMiles: String,
  taxiInTimeMins: String,
  taxiOutTimeMins: String,
  flightCancelled: String,
  cancellationCode: String, // (A = carrier, B = weather, C = NAS, D = security)
  diverted: String, // 1 = yes, 0 = no
  carrierDelayMins: String,
  weatherDelayMins: String,
  nasDelayMins: String,
  securityDelayMins: String,
  lateAircraftDelayMins: String)

case class DelayRecord(
    year: String,
    month: String,
    dayOfMonth: String,
    flightNum: String,
    uniqueCarrier: String,
    arrDelayMins: String) {
  override def toString = s"${year}/${month}/${dayOfMonth} - ${uniqueCarrier} ${flightNum} - ${arrDelayMins}"
}
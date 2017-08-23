package stream

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.javadsl.Sink
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Partition, RunnableGraph, Source}
import akka.stream.stage._

import scala.concurrent.duration.FiniteDuration

/*
 * Created by icemo on 17-8-23.
 */
object RouteGraphStageDemo extends App {

  class BackpressSource(val maxTickCounter:Int = 100) extends GraphStage[SourceShape[Int]] {

    val out:Outlet[Int] = Outlet("backpress.in")

    override def shape: SourceShape[Int] = SourceShape(out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      private var tickCounter = 0

      setHandler(out, new OutHandler {
        override def onPull() = {
          push(out, tickCounter)
          tickCounter += 1
          if (tickCounter > maxTickCounter) {
            onDownstreamFinish()
          }
        }
      })
    }
  }

  class StoreSink extends GraphStage[SinkShape[String]] {

    val in:Inlet[String] = Inlet("SaveRow")

    override def shape: SinkShape[String] = SinkShape(in)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

      private val silencePeriod = FiniteDuration(1, TimeUnit.SECONDS)

      setHandler(in, new InHandler {
        override def onPush() = {
          val el = grab(in)
          println(s"Store:$el")
        }
      })

      override protected def onTimer(timerKey: Any): Unit = {
        pull(in)
        scheduleOnce(None, silencePeriod)
      }

      override def preStart(): Unit = {
        scheduleOnce(None, silencePeriod)
      }
    }
  }

  class SquareGraphStage extends GraphStage[FlowShape[Int, String]]() {

    val in:Inlet[Int] = Inlet("Square.in")

    val out:Outlet[String] = Outlet("Square.out")

    override def shape: FlowShape[Int, String] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(in, new InHandler {
        override def onPush() = {
          val el = grab(in)
          push(out, s"Square: $el ${el * el}")
        }
      })

      setHandler(out, new OutHandler {
        override def onPull() = pull(in)
      })
    }
  }

  class CubeGraphStage extends GraphStage[FlowShape[Int, String]]() {

    val in:Inlet[Int] = Inlet("Cube.in")

    val out:Outlet[String] = Outlet("Cube.out")

    override def shape: FlowShape[Int, String] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(in, new InHandler {
        override def onPush() = {
          val el = grab(in)
          push(out, s"Cube: $el ${el * el * el}")
        }
      })

      setHandler(out, new OutHandler {
        override def onPull() = pull(in)
      })
    }
  }


  val testFlowGraph = Flow.fromGraph(GraphDSL.create(){ implicit builder =>

    import GraphDSL.Implicits._

    val routeInlet = builder.add(Partition[Int](2, {
      case el:Int if el % 2 == 0 => 0
      case _ => 1
    }))

    val out = builder.add(Merge[String](2))

    val squareGraph = builder.add(new SquareGraphStage())

    val cubeGraphStage = builder.add(new CubeGraphStage())

    routeInlet.out(0) ~> squareGraph ~> out.in(0)
    routeInlet.out(1) ~> cubeGraphStage ~> out.in(1)
    FlowShape(routeInlet.in, out.out)
  })

  implicit val actorSystem = ActorSystem()
  implicit val mat = ActorMaterializer()

  Source.fromGraph(new BackpressSource()).via(testFlowGraph).to(Sink.fromGraph(new StoreSink())).run()

}

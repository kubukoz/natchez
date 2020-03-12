// Copyright (c) 2019 by Rob Norris
// This software is licensed under the MIT License (MIT).
// For more information see LICENSE or https://opensource.org/licenses/MIT

package natchez
package opentracing

import cats.Applicative
import cats.effect.{Bracket, ExitCase, Resource, Sync}
import cats.implicits._
import cats.effect.implicits._
import io.opentracing.SpanContext
import io.opentracing.log.Fields
import io.opentracing.propagation.{Format, TextMapAdapter}
import io.{opentracing => ot}

import scala.jdk.CollectionConverters._

object OTTracer {

  private[opentracing] def makeSpan[F[_]: Sync](name: String,
                                                tracer: ot.Tracer,
                                                parentContext: Option[ot.SpanContext]): Resource[F, Span[F]] =
    Resource
      .makeCase {
        Sync[F].delay {
          val newSpan: ot.Span = parentContext.foldLeft(tracer.buildSpan(name))(_.asChildOf(_)).start()
          tracer.scopeManager().activate(newSpan)
          newSpan
        }
      }(finishSpan(tracer))
      .map(OTSpan(tracer, _))

  private def finishSpan[F[_]: Sync](t: ot.Tracer): (ot.Span, ExitCase[Throwable]) => F[Unit] =
    (s, exitCase) => {
      val attachPossibleException: F[Unit] = exitCase match {
        case ExitCase.Error(ex) =>
          val map: java.util.Map[String, Throwable] = new java.util.HashMap[String, Throwable]
          map.put(Fields.ERROR_OBJECT, ex)

          Sync[F].delay(s.log(map)).void
        case _ => Applicative[F].unit
      }

      attachPossibleException.guarantee(Sync[F].delay {
        t.scopeManager().activate(s)
        s.finish()
      })
    }

  def entryPoint[F[_]: Sync](acquireTracer: F[ot.Tracer]): Resource[F, EntryPoint[F]] =
    Resource
      .fromAutoCloseable(acquireTracer)
      .map(withTracer[F])

  private def withTracer[F[_]: Sync](tracer: ot.Tracer): EntryPoint[F] =
    new EntryPoint[F] {
      override def root(name: String): Resource[F, Span[F]] =
        makeSpan(name, tracer, none)

      override def continue(name: String, kernel: Kernel): Resource[F, Span[F]] = {
        val parentContext: F[SpanContext] = Sync[F].delay {
          tracer.extract(
            Format.Builtin.HTTP_HEADERS,
            new TextMapAdapter(kernel.toHeaders.asJava)
          )
        }

        Resource.suspend {
          parentContext.map { context =>
            makeSpan(name, tracer, context.some)
          }
        }
      }

      override def continueOrElseRoot(name: String, kernel: Kernel): Resource[F, Span[F]] =
        continue(name, kernel)
          .flatMap {
            case null => root(name) // hurr, means headers are incomplete or invalid
            case a    => a.pure[Resource[F, *]]
          }
          .recoverWith {
            case _: Exception => root(name)
          }
    }

}

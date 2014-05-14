package au.com.cba.omnia.maestro.core
package codec

import scala.util.control.NonFatal

import scalaz._, Scalaz._, \&/._

import shapeless.{ProductTypeClass, ProductTypeClassCompanion, GenericMacros}

import au.com.cba.omnia.maestro.core.data._

sealed trait DecodeSource
case class UnknownDecodeSource(source: List[String]) extends DecodeSource
case class ValDecodeSource(source: List[Val]) extends DecodeSource

case class Decode[A](run: (DecodeSource, Int) => DecodeResult[(DecodeSource, Int, A)]) {
  def decode(source: DecodeSource): DecodeResult[A] =
    run(source, 0).flatMap({
      case (ValDecodeSource(Nil), _, a) =>
        DecodeOk(a)
      case (UnknownDecodeSource(Nil), _, a) =>
        DecodeOk(a)
      case (remainder, counter, a) =>
        DecodeError(remainder, counter, TooMuchInput)
    })

  def map[B](f: A => B): Decode[B] =
    flatMap(f andThen Decode.strict[B])

  def flatMap[B](f: A => Decode[B]): Decode[B] =
    Decode((source, start) => run(source, start).flatMap({
      case (remainder, counter, value) =>
        f(value).run(remainder, counter)
    }))
}

object Decode extends ProductTypeClassCompanion[Decode] {
  def decode[A: Decode](source: DecodeSource): DecodeResult[A] =
    Decode.of[A].decode(source)

  def of[A: Decode]: Decode[A] = {
    import auto._
    implicitly[Decode[A]]
  }


  def ok[A](a: => A): Decode[A] =
    Decode((source, n) => DecodeOk((source, n, a)))

  def strict[A](a: A): Decode[A] =
    ok(a)

  def error[A](reason: Reason): Decode[A] =
    Decode((source, n) => DecodeError(source, n, reason))

  implicit val BooleanDecode: Decode[Boolean] =
    one("Boolean",
      { case BooleanVal(v) => v },
      { s => unsafe { s.toBoolean } })

  implicit val IntDecode: Decode[Int] =
    one("Int",
      { case IntVal(v) => v },
      { s => unsafe { s.toInt } })

  implicit val LongDecode: Decode[Long] =
    one("Long",
      { case LongVal(v) => v },
      { s => unsafe { s.toLong } })

  implicit val DoubleDecode: Decode[Double] =
    one("Double",
      { case DoubleVal(v) => v },
      { s => unsafe { s.toDouble } })

  implicit val StringDecode: Decode[String] =
    one("String",
      { case StringVal(v) => v },
      { s => s.right })


  implicit def ListDecode[A: Decode]: Decode[List[A]] =
    VectorDecode[A].map(_.toList)

  implicit def VectorDecode[A: Decode]: Decode[Vector[A]] =
    Decode((source, n) => {
      def loop(acc: DecodeResult[Vector[A]], source: DecodeSource, n: Int): DecodeResult[(DecodeSource, Int, Vector[A])] =
        acc.flatMap(xs =>
          Decode.of[A].run(source, n) match {
            case DecodeOk((remainder, counter, value)) =>
              loop(DecodeOk((xs :+ value)), remainder, counter)
            case DecodeError(_, _, _) =>
              acc.map((source, n, _))
          })
      loop(DecodeOk(Vector()), source, n)
    })

  implicit def DecodeTypeClass: ProductTypeClass[Decode] = new ProductTypeClass[Decode] {
    import shapeless._

    def emptyProduct =
      strict(HNil)

    def product[A, T <: HList](A: Decode[A], T: Decode[T]) =
      Decode((source, n) =>
        A.run(source, n) match {
          case DecodeOk((remainder, counter, a)) =>
            T.run(remainder, counter).map(_.map(a :: _))
          case DecodeError(remainder, counter, reason) =>
            DecodeError(remainder, counter, reason)
        })

    def project[F, G](instance: => Decode[G], to : F => G, from : G => F) =
      instance.map(from)
  }

  implicit def DecodeMonad: Monad[Decode] = new Monad[Decode] {
    def point[A](a: => A) = Decode((source, n) => DecodeOk((source, n, a)))
    def bind[A, B](m: Decode[A])(f: A => Decode[B]) = m flatMap f
  }

  def unsafe[A](a: => A): These[String, Throwable] \/ A =
    try a.right
    catch { case NonFatal(e) => That(e).left[A] }

  def one[A](tag: String, valDecode: PartialFunction[Val, A], unknownDecode: String => These[String, Throwable] \/ A): Decode[A] =
    Decode((source, n) => source match {
      case ValDecodeSource(v :: remainder) =>
        valDecode.lift(v) match {
          case None =>
            DecodeError(ValDecodeSource(remainder), n, ValTypeMismatch(v, tag))
          case Some(a) =>
            DecodeOk((ValDecodeSource(remainder), n + 1, a))
        }
      case UnknownDecodeSource(s :: remainder) =>
        unknownDecode(s) match {
          case -\/(err) =>
            DecodeError(UnknownDecodeSource(remainder), n, ParseError(s, tag, err))
          case \/-(a) =>
            DecodeOk((UnknownDecodeSource(remainder), n + 1, a))
        }
      case _ =>
        DecodeError(source, n, NotEnoughInput(1, tag))
    })


  import scala.language.experimental.macros
  import shapeless._
  implicit def ProductDecode[T](implicit ev: ProductTypeClass[Decode]): Decode[T] = //{
    //auto.derive[T](DecodeTypeClass)
    //macro shapeless.TypeClass.derive_impl[Decode, A]
    macro GenericMacros.deriveProductInstance[Decode, T]
  //}

}

object Test {
  //val a = Decode.of[(String, Int)]
}

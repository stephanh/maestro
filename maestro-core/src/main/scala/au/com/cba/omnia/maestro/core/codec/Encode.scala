package au.com.cba.omnia.maestro.core
package codec

import scalaz._, Scalaz._

import shapeless._

import au.com.cba.omnia.maestro.core.data._

case class Encode[A](run: A => List[Val]) {
  def contramap[B](f: B => A): Encode[B] =
    Encode[B](f andThen run)
}

object Encode extends TypeClassCompanion[Encode] {
  def encode[A: Encode](a: A): List[Val] =
    Encode.of[A].run(a)

  def of[A: Encode]: Encode[A] =
    implicitly[Encode[A]]

  def value[A](run: A => Val): Encode[A] =
    Encode[A](run andThen (_ :: Nil))

  implicit val BooleanEncode: Encode[Boolean] =
    value(BooleanVal)

  implicit val IntEncode: Encode[Int] =
    value(IntVal)

  implicit val LongEncode: Encode[Long] =
    value(LongVal)

  implicit val DoubleEncode: Encode[Double] =
    value(DoubleVal)

  implicit val StringEncode: Encode[String] =
    value(StringVal)

  implicit def ListEncode[A: Encode]: Encode[List[A]] =
    Encode(_.flatMap(encode[A]))

  implicit def VectorEncode[A: Encode]: Encode[Vector[A]] =
    Encode.of[List[A]].contramap(_.toList)

  /*implicit def ProductEncode[A]: Encode[A] =
    //apply[A](implicitly[TypeClass[Encode]])
    //macro GenericMacros.deriveInstance[Encode, A]
    macro TypeClass.derive_impl[Encode, A]
   */
  implicit def EncodeTypeClass: ProductTypeClass[Encode] = new ProductTypeClass[Encode] {
    def emptyProduct =
      Encode(_ => List())

    def product[A, T <: HList](A: Encode[A], T: Encode[T]) =
      Encode(a => A.run(a.head) ++ T.run(a.tail))

    def project[F, G](instance: => Encode[G], to : F => G, from : G => F) =
      instance.contramap(to)
  }

  implicit def EncodeContravariant: Contravariant[Encode] = new  Contravariant[Encode] {
    def contramap[A, B](r: Encode[A])(f: B => A) = r contramap f
  }
}

//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.macros

import au.com.cba.omnia.maestro.core.codec._
import com.twitter.scrooge._

import scala.reflect.macros.{Universe, Context}

object DescribeMacro {
  def impl[A <: ThriftStruct: c.WeakTypeTag](c: Context): c.Expr[Describe[A]] = {
    import c.universe._
    val typ = c.universe.weakTypeOf[A].typeSymbol
    val companion = typ.companionSymbol
    val entries = Inspect.fields[A](c)
    val fields = entries.map({
      case (method, field) =>
        val name = Literal(Constant(field))
        val tfield =  Select(Ident(companion), newTermName(field + "Field"))
        q"""($name, ${tfield})"""
    })
    val tablename = Literal(Constant(typ.name.toString))
    c.Expr[Describe[A]](q"au.com.cba.omnia.maestro.core.codec.Describe($tablename, List(..$fields))")
  }
}

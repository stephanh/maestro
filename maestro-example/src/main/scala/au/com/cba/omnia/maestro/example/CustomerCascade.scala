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

package au.com.cba.omnia.maestro.example

import scalaz.{Tag => _, _}, Scalaz._

import com.twitter.scalding._, TDsl._

import org.apache.hadoop.hive.conf.HiveConf

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.example.thrift._

class CustomerCascade(args: Args) extends MaestroCascade[Customer](args) {
  val env           = args("env")
  val domain        = "customer"
  val inputs        = Guard.expandPaths(s"${env}/source/${domain}/*")
  val errors        = s"${env}/errors/${domain}"
  val conf          = new HiveConf
  val validators    = Validator.all[Customer]()
  val filter        = RowFilter.keep
  val cleaners      = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )

  val dateTable =
    HiveTable(domain, "by_date", Partition.byDate(Fields.EffectiveDate) )
  val idTable =
    HiveTable(domain, "by_id", Partition(List("pid"), Fields.Id.get, "%s"))
  val jobs = Seq(
    new UniqueJob(args) {
      load[Customer]("|", inputs, errors, Maestro.now(), cleaners, validators, filter) |>
      (viewHive(dateTable, conf) _ &&&
        viewHive(idTable, conf)
      )
    },
    hiveQuery(
      args, "test",
      s"INSERT OVERWRITE TABLE ${idTable.name} PARTITION (pid) SELECT id, name, acct, cat, sub_cat, -10, effective_date FROM ${dateTable.name}",
      List(dateTable, idTable), None, conf
    )
  )
}

class SplitCustomerCascade(args: Args) extends Maestro[Customer](args) {
  val env           = args("env")
  val domain        = "customer"
  val inputs        = Guard.expandPaths(s"${env}/source/${domain}/*")
  val errors        = s"${env}/errors/${domain}"
  val dateView      = s"${env}/view/warehouse/${domain}/by-date"
  val catView       = s"${env}/view/warehouse/${domain}/by-cat"

  val cleaners      = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )

  val validators    = Validator.all(
    Validator.of(Fields.SubCat, Check.oneOf("M", "F")),
    Validator.by[Customer](_.acct.length == 7, "Customer accounts should always be a length of 5")
  )

  val filter        = RowFilter.keep

  load[Customer]("|", inputs, errors, Maestro.now(), cleaners, validators, filter)
    .map(split[Customer,(Customer, Customer, Customer)]) >>*
  (
    view(Partition.byDate(Fields.EffectiveDate), dateView),
    view(Partition.byFields2(Fields.Cat, Fields.SubCat), catView),
    view( Partition.byDate(Fields.EffectiveDate), dateView)
  )
}

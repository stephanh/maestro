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

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.core.codec._

import au.com.cba.omnia.maestro.example.thrift._

class CustomerCascade(args: Args) extends Maestro[Customer](args) {
  val env           = args("env")
  val domain        = "customer"
  val inputs        = Guard.expandPaths(s"${env}/source/${domain}/*")
  val outbound      = s"${env}/outbound/${domain}"
  val analytics     = s"${env}/view/warehouse/${domain}"
  val dateTable     = "by_date"
  val catTable      = "by_cat"
  def tableView(table: String) = s"${env}/view/warehouse/${domain}/$table"
  val errors        = s"${env}/errors/${domain}"

  val cleaners      = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )

  val validators    = Validator.all(
    Validator.of(Fields.CustomerSubCat, Check.oneOf("M", "F"))
    // , Validator.by[Customer](_.customerAcct.length == 6, "Customer accounts should always be a length of 5")
  )

  val filter        = RowFilter.keep
  val jobs = List(
    new Job(args) { 
      load[Customer]("|", inputs, errors, Maestro.now(), cleaners, validators, filter) |>
        ( view(env, Partition.byField(Fields.EffectiveDate), dateTable) _ &&&
          view(env, Partition.byFields2(Fields.CustomerCat, Fields.CustomerSubCat), tableView(catTable))
        )
    },
      hqlQuery(env, args, List(HivePartition.byField(Fields.EffectiveDate)), HivePartition.byField(Fields.CustomerCat)
      , s"INSERT OVERWRITE TABLE $dateTable PARTITION (id) SELECT id,name,address,age FROM $catTable")
    )
}

class SplitCustomerCascade(args: Args) extends Maestro[Customer](args) {
  val env           = args("env")
  val domain        = "customer"
  val inputs        = Guard.expandPaths(s"${env}/source/${domain}/*")
  val outbound      = s"${env}/outbound/${domain}"
  val analytics     = s"${env}/view/warehouse/${domain}"
  val dateView      = s"${env}/view/warehouse/${domain}/by-date"
  val catView       = s"${env}/view/warehouse/${domain}/by-cat"
  val errors        = s"${env}/errors/${domain}"
  
  val cleaners      = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )

  val validators    = Validator.all(
    Validator.of(Fields.CustomerSubCat, Check.oneOf("M", "F")),
    Validator.by[Customer](_.customerAcct.length == 7, "Customer accounts should always be a length of 5")
  )

  val filter        = RowFilter.keep
  
  val jobs = List(
    new Job(args) {
      load[Customer]("|", inputs, errors, Maestro.now(), cleaners, validators, filter)
        .map(split[Customer,(Customer, Customer, Customer)]) >>*
        (
          view(env, Partition.byDate(Fields.EffectiveDate), dateView),
          view(env, Partition.byFields2(Fields.CustomerCat, Fields.CustomerSubCat), catView),
          view(env, Partition.byDate(Fields.EffectiveDate), dateView)
        )
      }
    )
}

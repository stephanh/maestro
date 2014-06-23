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

package au.com.cba.omnia.maestro.core
package hive

import scalaz._, Scalaz._
import au.com.cba.omnia.maestro.core.partition.Partition
import org.apache.thrift.protocol.{TType, TField}
import com.twitter.scrooge.ThriftStruct
import au.com.cba.omnia.maestro.core.codec.Describe
import cascading.tap.hive.HiveTableDescriptor
import com.twitter.scalding.TupleSetter
import au.com.cba.omnia.beehaus.ParquetTableDescriptor
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeScheme
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf
import au.com.cba.omnia.ebenezer.scrooge.hive.Util._

case class TableDescriptor[A <: ThriftStruct : Manifest : Describe, B: Manifest: TupleSetter](database: String, partition: Partition[A, B]) {

  def createHiveDescriptor(): HiveTableDescriptor = {
    val describe = Describe.of[A]
    val (fields: List[TField]) = describe.metadata.map(x => x._2)
    val columnNames = describe.metadata.map(x => x._1).toArray
    val columnTypes: Array[String] = fields.map(f => mapType(f.`type`)).toArray
    new ParquetTableDescriptor(database, describe.name, columnNames, columnTypes, partition.fieldNames.toArray)
  }

  def tablePath(hiveConf: HiveConf) =
    s"${hiveConf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE)}/${database}/${name}"

  def name = {Describe.of[A].name}

  def partitions: List[(String, String)] = {
    val describe = Describe.of[A]
    val namesWithTypes = describe.metadata
    val partitionNames = partition.fieldNames
    // sys.error(partition)
    // TODO: should be described correctly
    partitionNames.map((name: String) => (name, mapType(
      namesWithTypes(name.drop(1).toInt - 1)._2.`type`)))
  }

  def qualifiedName = {database + "." + name}

  def createScheme() = new ParquetScroogeScheme[A]
}

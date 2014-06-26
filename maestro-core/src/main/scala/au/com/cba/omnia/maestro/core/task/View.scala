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
package task

import cascading.flow.FlowDef

import com.twitter.scalding._, TDsl._

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.hive.conf.HiveConf
import au.com.cba.omnia.ebenezer.scrooge.PartitionParquetScroogeSource

import au.com.cba.omnia.maestro.core.partition.Partition
import hive._
import scalding._
import codec._
import au.com.cba.omnia.ebenezer.scrooge.hive._
import com.twitter.scalding._
import org.apache.hadoop.hive.conf.HiveConf
import au.com.cba.omnia.ebenezer.scrooge.hive.HiveJob
import cascading.flow.FlowDef
import partition._
import com.twitter.scrooge.ThriftStruct

trait View {
  /** Partitions a pipe using the given partition scheme and writes out the data.*/
  def view[A <: ThriftStruct : Manifest : Describe, B: Manifest: TupleSetter]
    (env: String, partition: Partition[A, B], output: String)
    (pipe: TypedPipe[A])
    (implicit flowDef: FlowDef, mode: Mode): Unit = {
    val conf = new HiveConf()
    pipe
      .map(v => partition.extract(v) -> v)
      .write(PartitionHiveParquetScroogeSink[B, A](env, output, TableDescriptor("", partition).partitions, conf))
  }
}

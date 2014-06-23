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
import com.twitter.scalding.typed.EmptyTypedPipe

trait Query {
  def hqlQuery[A <: ThriftStruct : Manifest : Describe, B: Manifest : TupleSetter, C <: ThriftStruct : Manifest : Describe, D: TupleSetter : Manifest](env: String, args: Args, inputs: List[Partition[A, B]], output: Partition[C, D], query: String)
   (implicit flowDef: FlowDef, mode: Mode): TypedPipe[D] = {
    
    val conf = new HiveConf()
    val inputDescs = inputs.map(x => TableDescriptor(env, x))
    val outputDesc = TableDescriptor(env, output)
    
    // 1. Add the hive job to the flow def
    val inputSources: List[Source] = inputDescs.map(p =>
        PartitionHiveParquetScroogeSource[A](p.qualifiedName, p.tablePath(conf), List(), conf, flowDef))
    val outputSource: Source = PartitionHiveParquetScroogeSink[C, C](outputDesc.qualifiedName, outputDesc.tablePath(conf), List(), conf)
    new HiveJob(args, env, query, flowDef, mode, inputSources, outputSource)
   
    // 2. Return the null pipe
    EmptyTypedPipe(flowDef, mode)
 }
}

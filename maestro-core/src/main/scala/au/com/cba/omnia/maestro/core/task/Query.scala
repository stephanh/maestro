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

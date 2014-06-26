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

package au.com.cba.omnia.maestro

package object api {
  type Partition[A, B] = au.com.cba.omnia.maestro.core.partition.Partition[A, B]
  type Clean           = au.com.cba.omnia.maestro.core.clean.Clean
  type Validator[A]    = au.com.cba.omnia.maestro.core.validate.Validator[A]
  type RowFilter       = au.com.cba.omnia.maestro.core.filter.RowFilter

  val Partition     = au.com.cba.omnia.maestro.core.partition.Partition
  val HivePartition = au.com.cba.omnia.maestro.core.partition.HivePartition
  val Clean         = au.com.cba.omnia.maestro.core.clean.Clean
  val Validator     = au.com.cba.omnia.maestro.core.validate.Validator
  val Check         = au.com.cba.omnia.maestro.core.validate.Check
  val RowFilter     = au.com.cba.omnia.maestro.core.filter.RowFilter
  val Guard         = au.com.cba.omnia.maestro.core.hdfs.Guard
}

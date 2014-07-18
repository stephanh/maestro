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
package tributary

import org.specs2.mutable.Specification

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import com.cba.omnia.edge.hdfs.{Error, Hdfs, Ok, Result}

class PushSpec extends Specification {

  "1_Archive" should {

    """T1.1: Check if the file is archived """ in new IsolatedTest {
      val local = new File(testDir, "local.txt")
      local.createNewFile

      val archiveCheck = Push.archiveFile(local, new File(archiveDirS))
      archiveCheck mustEqual Ok(())

      val destFile = new File(archiveDirS, "local.txt")
      destFile.isFile must beTrue
    }

    """T1.2: Check if we can archive file in sub directory""" in new IsolatedTest {
      val local = new File(testDir, "local.txt")
      local.createNewFile
      val destDir = new File(archiveDirS, "subDir")

      val archiveCheck = Push.archiveFile(local, destDir)
      archiveCheck mustEqual Ok(())

      val destFile = new File(destDir, "local.txt")
      destFile.isFile must beTrue
    }

    """T1.3: We succeed with duplicate files""" in new IsolatedTest {
      val local = new File(testDir, "local.txt")
      local.createNewFile

      val destFile = new File(archiveDirS, "local.txt")
      destFile.createNewFile
      destFile.isFile must beTrue

      val archiveCheck2 = Push.archiveFile(local, new File(archiveDirS))
      archiveCheck2 mustEqual Ok(())
    }
  }

  "2_Push" should {

    """T2.1: Check if the file is copied to HDFS """ in new IsolatedTest {
      val src  = Data(new File(testDir, "local20140506.txt"), new File("foo/bar"))
      val dest = new Path(List(hdfsDirS, "foo", "bar", "local20140506.txt") mkString File.separator)
      val conf = new Configuration
      src.file.createNewFile

      val copyCheck = Push.push(src, archiveDirS, hdfsDirS).safe.run(conf)
      copyCheck mustEqual Ok(Copied(src.file, dest))

      val destExists = Hdfs.exists(dest).safe.run(conf)
      destExists mustEqual Ok(true)
    }

    """T2.2: Check if the SAP file is copied to HDFS """ in new IsolatedTest {
      val src  = Data(new File(testDir, "ZCR_DW01_E001_20140612_230441.DAT"), new File("."))
      val dest = new Path(hdfsDirS + File.separator + "ZCR_DW01_E001_20140612_230441.DAT")
      val conf = new Configuration
      src.file.createNewFile

      val copyCheck = Push.push(src, archiveDirS, hdfsDirS).safe.run(conf)
      copyCheck mustEqual Ok(Copied(src.file, dest))

      val destExists = Hdfs.exists(dest).safe.run(conf)
      destExists mustEqual Ok(true)
    }

    """T2.3: Check if duplicates cause failure """ in new IsolatedTest {
      val src  = Data(new File(testDir, "local20140506.txt"), new File("."))
      val dest = new Path(hdfsDirS + File.separator + "local20140506.txt")
      val conf = new Configuration
      src.file.createNewFile

      val copyCheck = Push.push(src, archiveDirS, hdfsDirS).safe.run(conf)
      copyCheck mustEqual Ok(Copied(src.file, dest))

      val duplicateCheck = Push.push(src, archiveDirS, hdfsDirS).safe.run(conf)
      duplicateCheck must beLike { case Error(_) => ok }
    }

    """T2.4: create an ingestion file in HDFS""" in new IsolatedTest {
      val src  = Data(new File(testDir, "local20140506.txt"), new File("."))
      val dest = new Path(hdfsDirS + File.separator + "local20140506.txt")
      val flag = new Path(hdfsDirS + File.separator + "_INGESTION_COMPLETE")
      val conf = new Configuration
      src.file.createNewFile

      val copyCheck = Push.push(src, archiveDirS, hdfsDirS).safe.run(conf)
      copyCheck mustEqual Ok(Copied(src.file, dest))

      val flagExists = Hdfs.exists(flag).safe.run(conf)
      flagExists mustEqual Ok(true)
    }

    """T2.5: fail when ingestion file already exists""" in new IsolatedTest {
      val src  = Data(new File(testDir, "local20140506.txt"), new File("."))
      val dest = new Path(hdfsDirS + File.separator + "local20140506.txt")
      val flag = new Path(hdfsDirS + File.separator + "_INGESTION_COMPLETE")
      val conf = new Configuration
      src.file.createNewFile
      Hdfs.create(flag).safe.run(conf)

      val copyCheck = Push.push(src, archiveDirS, hdfsDirS).safe.run(conf)
      copyCheck must beLike { case Error(_) => ok }

      val destExists = Hdfs.exists(dest).safe.run(conf)
      destExists mustEqual Ok(false)
    }
  }
}

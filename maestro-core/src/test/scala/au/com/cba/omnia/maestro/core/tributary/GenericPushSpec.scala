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

import com.cba.omnia.edge.hdfs.{Hdfs, Ok, Result}

class GenericPushSpec extends Specification {

  "1_Archive" should {

    """T1.1: Check if the file is archived """ in new IsolatedTest {
      val local = new File(testDir, "local.txt")
      local.createNewFile

      val archiveCheck = GenericPush.archiveFile(local, new File(archiveDirS))
      archiveCheck mustEqual Ok(())

      val destFile = new File(archiveDirS, "local.txt")
      destFile.isFile must beTrue
    }

    """T1.2: Check if we can archive file in sub directory""" in new IsolatedTest {
      val local = new File(testDir, "local.txt")
      local.createNewFile
      val destDir = new File(archiveDirS, "subDir")

      val archiveCheck = GenericPush.archiveFile(local, destDir)
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

      val archiveCheck2 = GenericPush.archiveFile(local, new File(archiveDirS))
      archiveCheck2 mustEqual Ok(())
    }

  }

  "2_Copy to HDFS" should {

    """T2.1: Check if the file is copied to HDFS """ in new IsolatedTest {
      val src = DataFile(new File(testDir, "local20140506.txt"), new File("."))
      src.file.createNewFile
      val conf = new Configuration

      val copyCheck = GenericPush.copyToHdfs(src, Hdfs.path(hdfsDirS)).safe.run(conf)
      copyCheck must beLike { case Ok(_) => ok }

      val destExists = Hdfs.exists(new Path(hdfsDirS + File.separator + "local20140506.txt")).safe.run(conf)
      destExists mustEqual Ok(true)
    }

    """T2.2: Check if the SAP file is copied to HDFS """ in new IsolatedTest {
      val src = DataFile(new File(testDir, "ZCR_DW01_E001_20140612_230441.DAT"), new File("."))
      src.file.createNewFile
      val conf = new Configuration

      val copyCheck = GenericPush.copyToHdfs(src, Hdfs.path(hdfsDirS)).safe.run(conf)
      copyCheck must beLike { case Ok(_) => ok }

      val destExists = Hdfs.exists(new Path(hdfsDirS + File.separator + "ZCR_DW01_E001_20140612_230441.DAT")).safe.run(conf)
      destExists mustEqual Ok(true)
    }

    """T2.3: Check if duplicates are detected """ in new IsolatedTest {
      val src = DataFile(new File(testDir, "local20140506.txt"), new File("foo"))
      src.file.createNewFile
      val conf = new Configuration

      val copyCheck = GenericPush.copyToHdfs(src, Hdfs.path(hdfsDirS)).safe.run(conf)
      copyCheck must beLike { case Ok(Pushed(_,_,Copied())) => ok }

      val duplicateCheck = GenericPush.copyToHdfs(src, Hdfs.path(hdfsDirS)).safe.run(conf)
      duplicateCheck must beLike { case Ok(Pushed(_,_,AlreadyExists())) => ok }
    }

  }

  "3_Process the file to HDFS" should {

    """T3.1: copy file which is not copied already""" in new IsolatedTest {
      val src = DataFile(new File(testDir, "local20140506.txt"), new File("foo/bar"))
      src.file.createNewFile
      val conf = new Configuration

      val processCheck = GenericPush.processTheFile(src, hdfsDirS, archiveDirS, "prefix").safe.run(conf)
      processCheck must beLike { case Ok(_) => ok }

      val destExists = Hdfs.exists(new Path(hdfsDirS
        + File.separator + "prefix"
        + File.separator + "foo"
        + File.separator + "bar"
        + File.separator + "local20140506.txt")).safe.run(conf)
      destExists mustEqual Ok(true)
    }

    """T3.2: Copy SAP file to HDFS""" in new IsolatedTest {
      val src = DataFile(new File(testDir, "ZCR_DW01_E001_20140612_230441.DAT"), new File("2014/6/12/23/4"))
      src.file.createNewFile
      val conf = new Configuration

      val processCheck = GenericPush.processTheFile(src, hdfsDirS, archiveDirS, ".").safe.run(conf)
      processCheck must beLike { case Ok(_) => ok }

      val destExists = Hdfs.exists(new Path(hdfsDirS
        + File.separator + "2014"
        + File.separator + "6"
        + File.separator + "12"
        + File.separator + "23"
        + File.separator + "4"
        + File.separator + "ZCR_DW01_E001_20140612_230441.DAT")).safe.run(conf)
      destExists mustEqual Result.ok(true)
    }
  }

  "4_Create ingestion file" should {
    """T4.1: create an ingestion file in HDFS""" in new IsolatedTest {
      val destDir = Hdfs.path(hdfsDirS)
      val conf = new Configuration

      val copyCheck = GenericPush.createFlag(destDir, "_INGESTION_COMPLETE").safe.run(conf)
      copyCheck mustEqual Ok(())

      val destExists = Hdfs.exists(new Path(hdfsDirS + File.separator + "_INGESTION_COMPLETE")).safe.run(conf)
      destExists mustEqual Ok(true)
    }

    """T4.2: succeed when ingestion file already exists""" in new IsolatedTest {
      val dest = Hdfs.path(hdfsDirS)
      val conf = new Configuration

      val copyCheck = GenericPush.createFlag(dest, "_INGESTION_COMPLETE").safe.run(conf)
      copyCheck mustEqual Ok(())

      val destExists = Hdfs.exists(new Path(hdfsDirS + File.separator + "_INGESTION_COMPLETE")).safe.run(conf)
      destExists mustEqual Ok(true)

      val copyCheck2 = GenericPush.createFlag(dest, "_INGESTION_COMPLETE").safe.run(conf)
      copyCheck2 mustEqual Ok(())
    }
  }
}

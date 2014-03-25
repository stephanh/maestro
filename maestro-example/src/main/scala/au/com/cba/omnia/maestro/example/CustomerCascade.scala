package au.com.cba.omnia.maestro.example

import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.example.thrift._
import com.twitter.scalding._
import au.com.cba.omnia.maestro.core.hive.TableDescriptor
import com.twitter.scalding.filecache.DistributedCacheFile
import org.apache.hadoop.hive.conf.HiveConf

class CustomerCascade(args: Args) extends Cascade(args) with MaestroSupport2[Customer, CustomerEnriched]{
  val maestro = Maestro(args)
 
  val env           = args("env")
  val domain        = "customer"
  val input         = s"/${env}/source/${domain}/*"
  val clean         = s"/${env}/processing/${domain}"
  val errors        = s"/${env}/errors/${domain}"

  val byDate        = TableDescriptor(env, Partition.byDate(Fields1.EffectiveDate))
  val byCategory    = TableDescriptor(env, Partition.byDate(Fields1.CustomerSubCat))
  val enriched      = TableDescriptor(env, Partition.byDate(Fields2.EffectiveDate))

  val bundle = DistributedCacheFile("/usr/local/lib/parquet-hive-bundle.jar")
  bundle.path

  val cleaners      = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )
  val validators    = Validator.all(
    Validator.of(Fields1.CustomerSubCat, Check.oneOf("M", "F")),
    Validator.by[Customer](_.customerAcct.length == 4, "Customer accounts should always be a length of 4")
  )
  val filter        = RowFilter.keep

  def jobs = Guard.onlyProcessIfExists(input) { paths => List(
    maestro.load[Customer]("|", paths, clean, errors, maestro.now(), cleaners, validators, filter)
  , maestro.view(byDate, clean)
  , maestro.view(byCategory, clean)
  , maestro.hqlQuery("Convert Customer", List(byDate), enriched,
      s"INSERT OVERWRITE TABLE ${enriched.qualifiedName} select CustomerID,EffectiveDate from ${byDate.qualifiedName}",
      Map(HiveConf.ConfVars.HIVEAUXJARS -> bundle.path))
  ) }
}

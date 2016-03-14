package md.fusionworks.vardictspark

import com.astrazeneca.vardict.VarDict
import htsjdk.samtools.SAMRecord
import md.fusionworks.vardictspark.spark.SparkContextFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.rdd.ADAMContext._
import Implicits._


/**
  * Created by antonstamov on 3/13/16.
  */
object Main extends App {
  val basePath = "/home/user/Data/chr20_dataset/raw"
  val bamPath = s"$basePath/dedupped_20.bam"
  val bedPath = s"$basePath/sample_dedupped_20.bed"
  val fastaPath = s"$basePath/human_b37_20.fasta"

  val regions = VarDictSpark.loadRegions(bedPath).collect()
  val fasta = VarDictSpark.loadFasta(fastaPath).persist(StorageLevel.MEMORY_AND_DISK)
  val sc = VarDictSpark.sc
  val samRDD = sc.loadSamRecordRDD(bamPath).persist(StorageLevel.MEMORY_AND_DISK)
  var vdElems = sc.parallelize(Seq.empty[VarDictRDDElem])
  regions.foreach { region =>
    val f = VarDictSpark.filterFasta(fasta, region).map(n => n.position -> n.value).collect().toMap
    val b = samRDD.filterSAMRecordRDD(region.start, region.end, region.chr).collect()

    vdElems = vdElems.union(sc.parallelize(Seq(VarDictRDDElem(region, f, b))))

  }
  vdElems.foreach { e =>
    println(e)
    VarDict.toVars(e.region.toVDRegion,e.bam,e.ref,Map("20"->20000000),)
    /**
      * TODO:
      * VarDict.toVars()
      * VarDict.vardict()
      * */

  }

}

case class VarDictRDDElem(region: Region, ref: Map[Long, Char], bam: Array[SAMRecord])

object VarDictSpark {

  val sc: SparkContext = SparkContextFactory.getSparkContext(Some("local[*]"))

  def loadRegions(path: String): RDD[Region] = {
    sc.textFile(path).map(Region.fromString)
  }


  def filterBam(bam: RDD[SAMRecord], region: Region) = ???

  def loadFasta(path: String): RDD[Nucleotide] = {
    sc.loadSequence(path)
      .flatMap { fragment =>
        fragment.getFragmentSequence.zipWithIndex.map { t =>
          Nucleotide(fragment.getContig.getContigName, t._2 + fragment.getFragmentStartPosition + 1, t._1)
        }
      }
  }

  def filterFasta(fasta: RDD[Nucleotide], region: Region) = {
    val start = region.start - 700
    val end = region.end + 700
    fasta.filter(n => n.chr == region.chr && n.position >= start && n.position <= end)
  }
}

case class Region(chr: String, start: Int, end: Int , gene: String/*, iStart: Int, iEnd: Int*/) {
  import com.astrazeneca.vardict.{Region => VDRegion}
  def toVDRegion:VDRegion = {
    new VDRegion(chr,start,end,gene)
  }
}

object Region {
  def fromString(s: String) = {
    val arr = s.split("\t")
    Region(arr(0), arr(1).toInt, arr(2).toInt, arr(3))
  }
}

case class Nucleotide(chr: String, position: Long, value: Char)
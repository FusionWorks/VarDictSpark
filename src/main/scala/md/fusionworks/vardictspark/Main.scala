package md.fusionworks.vardictspark

import java.util

import com.astrazeneca.vardict.{Main => VDMain, VarDict}
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
  val localArgs = Array(
    "-c", "1", "-S", "2", "-E", "3", "-s", "2", "-e", "3", "-g", "4",
    "-G", "/home/ubuntu/work/data/chr20_dataset/raw/human_b37_20.fasta",
    "-b", "/home/ubuntu/work/data/chr20_dataset/raw/dedupped_20.bam",
    "/home/ubuntu/work/data/chr20_dataset/raw/dedupped_20.bed"
  )
  val conf = VDMain.getConfigurationFromArgs(localArgs)
  val basePath = "s3n://fusiongene-na/chr20_dataset/raw"

  val bamPath = s"$basePath/dedupped_20.bam"
  val bedPath = s"$basePath/sample_dedupped_20_2.bed"
  val fastaPath = s"$basePath/human_b37_20.fasta"

  val outputPath = "s3n://fusiongene-na/vardict/vcf"

  val regions = VarDictSpark.loadRegions(bedPath)
  val fasta = VarDictSpark.loadFasta(fastaPath).persist(StorageLevel.MEMORY_AND_DISK)
  val sc = VarDictSpark.sc
  val samRDD = sc.loadSamRecordRDD(bamPath).persist(StorageLevel.MEMORY_AND_DISK)
  var fElems = sc.parallelize(Seq.empty[(Region, util.HashMap[Integer, Character])])
  var bElems = sc.parallelize(Seq.empty[(Region, List[SAMRecord])])
  val chrs = mapToJavaHashMap(samRDD.first()
    .getHeader.getSequenceDictionary.getSequences
    .map(s => s.getSequenceName -> s.getSequenceLength.asInstanceOf[Integer]).toMap)

  regions.toLocalIterator.foreach { region =>
    val f = mapToJavaHashMap(
      VarDictSpark.filterFasta(fasta, region).map(n => n.position -> n.value).collectAsMap()
    )
    fElems = fElems.union(sc.parallelize(Seq(region -> f)))

  }
//  fElems = fElems.persist(StorageLevel.MEMORY_AND_DISK)
  fasta.unpersist(blocking = false)

  regions.toLocalIterator.foreach { region =>
    val b = samRDD.filterSAMRecordRDD(region.start, region.end, region.chr).collect().toList
    bElems = bElems.union(sc.parallelize(Seq(region -> b)))

  }
//  bElems = bElems.persist(StorageLevel.MEMORY_AND_DISK)
  samRDD.unpersist(blocking = false)

  val vdElems = fElems.join(bElems).map(t => VarDictRDDElem(t._1, t._2._1, t._2._2)).persist(StorageLevel.MEMORY_AND_DISK)

  println("counting vdElems")
  println("counting vdElems")
  println("counting vdElems")
  println("counting vdElems")
  println(s"count = ${vdElems.count()}")

  val vars = vdElems.flatMap { e =>

    val splice: java.util.Set[String] = new java.util.HashSet[String]
    val sample = VarDict.getSampleNames(conf)._1
    val vars_ = VarDict.toVars(e.region.toVDRegion, e.bam, e.ref, chrs, sample, splice, 0, conf)
    VarDict.vardict(e.region.toVDRegion, vars_._2, sample, splice, conf)
  }.persist(StorageLevel.MEMORY_AND_DISK)

  println("counting vars")
  println("counting vars")
  println("counting vars")
  println("counting vars")
  println(s"count = ${vars.count()}")
  println("saving")

  vars.coalesce(1).saveAsTextFile(outputPath)

  /*implicit*/ def mapToJavaHashMap[K, V](map: scala.collection.Map[K, V]): util.HashMap[K, V] = {
    new util.HashMap[K, V](map)
  }

}

case class VarDictRDDElem(region: Region, ref: util.HashMap[Integer, Character], bam: List[SAMRecord])

object VarDictSpark {

  val sc: SparkContext = SparkContextFactory.getSparkContext()

  def loadRegions(path: String): RDD[Region] = {
    sc.textFile(path).map(Region.fromString)
  }


  def filterBam(bam: RDD[SAMRecord], region: Region) = ???

  def loadFasta(path: String): RDD[Nucleotide] = {
    sc.loadSequence(path)
      .flatMap { fragment =>
        fragment.getFragmentSequence.zipWithIndex.map { t =>
          Nucleotide(fragment.getContig.getContigName, (t._2 + fragment.getFragmentStartPosition + 1).toInt, t._1)
        }
      }
  }

  def filterFasta(fasta: RDD[Nucleotide], region: Region) = {
    val start = region.start - 700
    val end = region.end + 700
    fasta.filter(n => n.chr == region.chr && n.position >= start && n.position <= end)
  }
}

case class Region(chr: String, start: Int, end: Int, gene: String /*, iStart: Int, iEnd: Int*/) {

  import com.astrazeneca.vardict.{Region => VDRegion}

  def toVDRegion: VDRegion = {
    new VDRegion(chr, start, end, gene)
  }
}

object Region {
  def fromString(s: String) = {
    val arr = s.split("\t")
    Region(arr(0), arr(1).toInt, arr(2).toInt, arr(3))
  }
}

case class Nucleotide(chr: String, position: Integer, value: Character)
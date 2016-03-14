package md.fusionworks.vardictspark


import htsjdk.samtools.{SAMFileHeader, SAMRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.models.{RecordGroupDictionary, SAMFileHeaderWritable}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{AlignmentRecord, NucleotideContigFragment, Variant}
import org.codehaus.jackson.map.ObjectMapper
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, SAMRecordWritable}


object Implicits {
  implicit def dfToDataFrameFunctions(df: DataFrame): DataFrameFunctions = new DataFrameFunctions(df)

  implicit def toReferenceRDDFunctions(rdd: RDD[NucleotideContigFragment]): ReferenceRDDFunctions =
    new ReferenceRDDFunctions(rdd)

  implicit def toAlignmentRDDFunctions(rdd: RDD[AlignmentRecord]): AlignmentRDDFunctions =
    new AlignmentRDDFunctions(rdd)

  implicit def toSAMRecordRDDFunctions(rdd: RDD[SAMRecord]): SAMRecordRDDFunctions =
    new SAMRecordRDDFunctions(rdd)

  implicit def toVariantRDDFunctions(rdd: RDD[Variant]): VariantRDDFunctions =
    new VariantRDDFunctions(rdd)

  implicit def toAdamContextExt(sc: SparkContext): AdamContextExt = new AdamContextExt(new ADAMContext(sc))
}


class AdamContextExt(ac: ADAMContext) {
  private val sc = ac.sc

  def loadSamRecordRDD(filePath: String) = {
    val job = newJob(sc.hadoopConfiguration)
    val records = sc.newAPIHadoopFile(filePath, classOf[AnySAMInputFormat], classOf[LongWritable],
      classOf[SAMRecordWritable], ContextUtil.getConfiguration(job)).map(_._2.get())

    records
  }


  def newJob(config: Configuration): Job = {
    val jobClass: Class[_] = Class.forName("org.apache.hadoop.mapreduce.Job")
    try {
      // Use the getInstance method in Hadoop 2
      jobClass.getMethod("getInstance", classOf[Configuration]).invoke(null, config).asInstanceOf[Job]
    } catch {
      case ex: NoSuchMethodException =>
        // Drop back to Hadoop 1 constructor
        jobClass.getConstructor(classOf[Configuration]).newInstance(config).asInstanceOf[Job]
    }
  }

}


class DataFrameFunctions(df: DataFrame) {

  def filterReferenceDF(start: Long, end: Long, contigName: String) = {
    val firstRow = df.first()
    val contigLength = firstRow.getAs[Row]("contig").getAs[Long]("contigLength")
    val numberOfFragmentsInContig = firstRow.getAs[Integer]("numberOfFragmentsInContig")
    val fragmentLength = contigLength / numberOfFragmentsInContig

    df.filter(df("contig.contigName") === contigName)
      .filter(
        df("fragmentStartPosition") < end && df("fragmentStartPosition") != null &&
          df("fragmentStartPosition") + fragmentLength > start && df("fragmentStartPosition") != null
      )
  }

  def referenceDfToRDD = {
    df.toJSON.mapPartitions(partition => {
      val mapper = new ObjectMapper()
      partition.map(str => mapper.readValue(str, classOf[NucleotideContigFragment]))
    })
  }


  def filterAlignmentDF(start: Long, end: Long, contigName: String) = {
    df.filter(df("contig.contigName") === contigName)
      .filter(
        df("start") < end && df("start") != null &&
          df("end") > start && df("end") != null
      )
  }

  def alignmentDfToRDD = {
    df.toJSON.mapPartitions(partition => {
      val mapper = new ObjectMapper()
      partition.map(str => mapper.readValue(str, classOf[AlignmentRecord]))
    })
  }

  def filterVariantDF(start: Long, end: Long, contigName: String) = {
    df.filter(df("contig.contigName") === contigName)
      .filter(
        df("start") < end && df("start") != null &&
          df("end") > start && df("end") != null
      )
  }


  def variantsDfToRDD = {
    df.toJSON.mapPartitions(partition => {
      val mapper = new ObjectMapper()
      partition.map(str => mapper.readValue(str, classOf[Variant]))
    })
  }

}

class ReferenceRDDFunctions(rdd: RDD[NucleotideContigFragment]) {

  def toJBrowseFormat = {
    rdd.map(record => RecordConverter.referenceRecToJbFormat(record))
  }

}

class SAMRecordRDDFunctions(rdd: RDD[SAMRecord]) {
  def filterSAMRecordRDD(start: Long, end: Long, contigName: String) = {
    rdd
      .filter(_.getContig == contigName)
      .filter(r =>
        r.getStart < end &&
          r.getEnd > start
      )
  }
}

class AlignmentRDDFunctions(rdd: RDD[AlignmentRecord]) {

  def filterAlignmentRDD(start: Long, end: Long, contigName: String) = {
    rdd
      .filter(_.getContig != null)
      .filter(_.getContig.getContigName == contigName)
      .filter(r =>
        r.getStart < end &&
          r.getEnd > start
      )
  }

  def toSamRecordRDD(contigName: String): RDD[SAMRecord] = {
    val header = RecordConverter.getHeader(contigName, rdd)
    val converter = new AlignmentRecordConverter
    rdd.mapPartitions { case partition =>
      partition.map(record =>
        converter.convert(record, header.value, RecordGroupDictionary.fromSAMHeader(header.value.header))
      )
    }
  }

}

class VariantRDDFunctions(rdd: RDD[Variant]) {

  def toJBrowseFormat = {
    rdd.map(record => RecordConverter.variantRecToJbFormat(record))
  }
}

object RecordConverter {

  private var headerMap = Map[String, SAMFileHeader]()

  def getHeader(contigName: String, alignmentRDD: RDD[AlignmentRecord]): Broadcast[SAMFileHeaderWritable] = {
    val header = headerMap.get(contigName) match {
      case Some(h) => h
      case None =>
        val sd = alignmentRDD.adamGetSequenceDictionary()
        //        val rgd = alignmentRDD.recordG()
        val converter = new AlignmentRecordConverter
        //        val h = converter.createSAMHeader(sd, rgd)
        //        headerMap += (contigName -> h)
        headerMap(contigName)
    }

    alignmentRDD.context.broadcast(SAMFileHeaderWritable(header))
  }


  def referenceRecToJbFormat(referenceRecord: NucleotideContigFragment) = {
    Map(
      "seq" -> referenceRecord.getFragmentSequence,
      "flag" -> referenceRecord.getFragmentNumber.toString,
      "start" -> referenceRecord.getFragmentStartPosition.toString,
      "seq_id" -> referenceRecord.getContig.getContigName
    )
  }

  def variantRecToJbFormat(variantRecord: Variant) = {
    Map(
      "start" -> variantRecord.getStart,
      "end" -> variantRecord.getEnd,
      "seq_id" -> variantRecord.getContig.getContigName,
      "reference_allele" -> variantRecord.getReferenceAllele,
      "alternative_alleles" -> Map(
        "values" -> List(variantRecord.getAlternateAllele),
        "meta" -> Map(
          "description" -> "VCF ALT field, list of alternate non-reference alleles called on at least one of the samples"
        )
      ),
      "debug_to_string" -> variantRecord.toString
    )
  }
}
package md.fusionworks.vardictspark

import java.util

import htsjdk.samtools.SAMRecord
import com.astrazeneca.vardict.{Region => VDRegion}


trait Model

case class Region(chr: String, start: Int, end: Int, gene: String) {

  def toVDRegion: VDRegion = {
    new VDRegion(chr, start, end, gene)
  }
}

object Region {
  def fromString(s: String) = {
    val arr = s.split("\t")
    Region(arr(0), arr(1).toInt, arr(2).toInt, arr.lift(3).getOrElse(""))
  }
}

case class VarDictRDDElem(region: Region, ref: util.HashMap[Integer, Character], bam: List[SAMRecord])

case class Nucleotide(chr: String, position: Integer, value: Character)
package SparkTeraSort

import scala.reflect._

import com.google.common.primitives.UnsignedBytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
 * Created by nico on 25.03.15.
 */
object TeraAll {

  def main(args: Array[String]): Unit = {
    val dataSizeStr = args(0)
    val parts: Int = args(1).toInt
    val filesPath = args(2)

    val outputSizeInBytes = TeraGen.sizeStrToBytes(dataSizeStr)
    val size = TeraGen.sizeToSizeStr(outputSizeInBytes)

    // **************************************************
    // INITIALIZE
    // **************************************************
    val conf = new SparkConf()
	.setAppName(s"TeraSort ($size)")
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    //val recordsPerPartition = (100 * parts.toLong) / outputSizeInBytes
    val recordsPerPartition = (outputSizeInBytes / 100) / parts.toLong
    
    println("output sizes " + outputSizeInBytes + "b")
    println("parts " + parts.toLong)
    println("rec/Part " + recordsPerPartition)
    val numRecords  = outputSizeInBytes / TeraInputFormat.RECORD_LEN

    // **************************************************
    // GENERATE DATASET
    // **************************************************
    var t1 = System.nanoTime()
    val dataset = sc.parallelize(1 to parts, parts).mapPartitionsWithIndex { case (index, _) =>
      val one = new Unsigned16(1)

      val firstRecordNumber = new Unsigned16(index.toLong * recordsPerPartition.toLong)
      val recordsToGenerate = new Unsigned16(recordsPerPartition)

      val recordNumber = new Unsigned16(firstRecordNumber)
      val lastRecordNumber = new Unsigned16(firstRecordNumber)
      lastRecordNumber.add(recordsToGenerate)

      val rand = Random16.skipAhead(firstRecordNumber)

      val rowBytes: Array[Byte] = new Array[Byte](TeraInputFormat.RECORD_LEN)
      val key = new Array[Byte](TeraInputFormat.KEY_LEN)
      val value = new Array[Byte](TeraInputFormat.VALUE_LEN)

      Iterator.tabulate(recordsPerPartition.toInt) { offset =>
        Random16.nextRand(rand)
        TeraGen.generateRecord(rowBytes, rand, recordNumber)
        recordNumber.add(one)
        rowBytes.copyToArray(key, 0, TeraInputFormat.KEY_LEN)
        rowBytes.takeRight(TeraInputFormat.VALUE_LEN).copyToArray(value, 0,
          TeraInputFormat.VALUE_LEN)
        (key.clone(), value)
      }
    }


    // **************************************************
    // WRITE DATASET
    // **************************************************

    dataset.saveAsNewAPIHadoopFile[TeraOutputFormat](filesPath)
    var t2 = System.nanoTime()
    println("Number of records written: " + dataset.count())
    println("t_ioW(" + dataSizeStr + ") = " + (t2 - t1)/Math.pow(10,9) + "s")

    // **************************************************
    // RE-READ DATASET
    // **************************************************

    t1 = System.nanoTime()
    val d2 = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](filesPath).cache // dataset_raw
    d2.count()
    t2 = System.nanoTime()
    println("t_ioR(" + dataSizeStr + ") = " + (t2 - t1)/Math.pow(10,9) + "s")

    // **************************************************
    // SORT DATASET
    // **************************************************

    val sorted = d2.sortBy[Array[Byte]](f => f._1)(new Ordering[Array[Byte]] {
      implicit val caseInsensitiveOrdering = UnsignedBytes.lexicographicalComparator

      override def compare(x: Array[Byte], y: Array[Byte]): Int = UnsignedBytes.lexicographicalComparator.compare(x,y)
    }, classTag[Array[Byte]])
    sorted.saveAsNewAPIHadoopFile[TeraOutputFormat](filesPath+"_sorted")    
    t1 = System.nanoTime()

    // **************************************************
    // VALIDATE DATASET
    // **************************************************


    println("t_s(" + dataSizeStr + ") = " + (t1 - t2)/Math.pow(10,9) + "s")

    // TeraValidate.validate(sc, sorted)

  }

}

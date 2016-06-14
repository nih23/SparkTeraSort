/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package SparkTeraSort

import com.google.common.primitives.UnsignedBytes
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * An application that generates data according to the terasort spec and shuffles them.
 * This is a great example program to stress test Spark's shuffle mechanism.
 *
 * See http://sortbenchmark.org/
 */
object TeraSort {

  implicit val caseInsensitiveOrdering = UnsignedBytes.lexicographicalComparator

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("usage:")
      println("DRIVER_MEMORY=[mem] bin/run-example terasort.TeraSort " +
        "[master URI] [input-file] [output-file]")
      println(" ")
      println("example:")
      println("DRIVER_MEMORY=50g bin/run-example terasort.TeraSort " +
        "spark://141.76.91.165:7077 /home/myuser/terasort_in /home/myuser/terasort_out")
      System.exit(0)
    }

    // Process command line arguments
    val masterURI = args(0)
    val inputFile = args(1)
    val outputFile = args(2)

    val conf = new SparkConf()
      .setMaster(masterURI)
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName(s"TeraSort")
    val sc = new SparkContext(conf)

    val t0 = System.nanoTime()
    val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputFile).cache // dataset_raw
    // NOTE: THE FOLLOWING LINE IS A WORKAROUND FOR A BUG IN SPARK < 1.3 HADOOP FILE READER
    //val dataset = dataset_raw.map(f => (f._1.clone(), f._2)      )
    val t1 = System.nanoTime()

    val sorted = dataset.sortByKey()

    val t2 = System.nanoTime()

    //TeraValidate.validate(sc,sorted)
    sorted.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFile)
    val t3 = System.nanoTime()

    println("**********************")
    println("TeraSort/Spark summary")
    println("**********************")
    println("IO read  " + (t1 - t0)/Math.pow(10,9) + "s")
    println("Sort     " + (t2 - t1)/Math.pow(10,9) + "s")
    println("IO write " + (t3 - t2)/Math.pow(10,9) + "s")
  }
}
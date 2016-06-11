package com.kenjih

import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfter
import org.apache.spark.{ SparkConf, SparkContext }

class WordCountSpec extends FunSpec with BeforeAndAfter {

  val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
  val sc = new SparkContext(sparkConf)

  describe("word") {
    it("should be delimited by space") {
      val text = sc.makeRDD(Seq("a b c a a c a"))
      val count = WordCount.run(text).collect
      assert(count.contains(("a", 4)))
      assert(count.contains(("b", 1)))
      assert(count.contains(("c", 2)))
    }
    it("should be counted over lines") {
      val text = sc.makeRDD(Seq("a a", "b a c", "b a b"))
      val count = WordCount.run(text).collect
      assert(count.contains(("a", 4)))
      assert(count.contains(("b", 3)))
      assert(count.contains(("c", 1)))
    }
  }
}

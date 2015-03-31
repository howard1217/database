package org.apache.spark.sql.execution.joins.dns

import java.util.{HashMap => JavaHashMap, ArrayList => JavaArrayList}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.Queue
import scala.collection.mutable.HashSet
import scala.collection.JavaConversions._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, Projection, Expression}
import org.apache.spark.sql.execution.SparkPlan

/**
 * In this join, we are going to implement an algorithm similar to symmetric hash join.
 * However, instead of being provided with two input relations, we are instead going to
 * be using a single dataset and obtaining the other data remotely -- in this case by
 * asynchronous HTTP requests.
 *
 * The dataset that we are going to focus on reverse DNS, latitude-longitude lookups.
 * That is, given an IP address, we are going to try to obtain the geographical
 * location of that IP address. For this end, we are going to use a service called
 * telize.com, the owner of which has graciously allowed us to bang on his system.
 *
 * For that end, we have provided a simple library that makes asynchronously makes
 * requests to telize.com and handles the responses for you. You should read the
 * documentation and method signatures in DNSLookup.scala closely before jumping into
 * implementing this.
 *
 * The algorithm will work as follows:
 * We are going to be a bounded request buffer -- that is, we can only have a certain number
 * of unanswered requests at a certain time. When we initialize our join algorithm, we
 * start out by filling up our request buffer. On a call to next(), you should take all
 * the responses we have received so far and materialize the results of the join with those
 * responses and return those responses, until you run out of them. You then materialize
 * the next batch of joined responses until there are no more input tuples, there are no
 * outstanding requests, and there are no remaining materialized rows.
 *
 */
trait DNSJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val left: SparkPlan

  override def output = left.output

  @transient protected lazy val leftKeyGenerator: Projection =
    newProjection(leftKeys, left.output)

  // How many outstanding requests we can have at once.
  val requestBufferSize: Int = 300

  /**
   * The main logic for DNS join. You do not need to implement anything outside of this method.
   * This method takes in an input iterator of IP addresses and returns a joined row with the location
   * data for each IP address.
   *
   * If you find the method definitions provided to be counter-intuitive or constraining, feel free to change them.
   * However, note that if you do:
   *  1. we will have a harder time helping you debug your code.
   *  2. Iterators must implement next and hasNext. If you do not implement those two methods, your code will not compile.
   *
   * **NOTE**: You should return JoinedRows, which take two input rows and returns the concatenation of them.
   * e.g., `new JoinedRow(row1, row2)`
   *
   * @param input the input iterator
   * @return the result of the join
   */
  def hashJoin(input: Iterator[Row]): Iterator[Row] = {
    new Iterator[Row] {
      // IMPLEMENT ME
      val output: Queue[Row] = new Queue[Row]() 
      val requestBuffer: ConcurrentHashMap[Int, Row] = new ConcurrentHashMap[Int, Row]()
      val responseCache: ConcurrentHashMap[String, Row] = new ConcurrentHashMap[String, Row]()
      val responses: ConcurrentHashMap[Int, Row] = new ConcurrentHashMap[Int, Row]()
      val ipToRequestNum: ConcurrentHashMap[String, HashSet[Int]] = new ConcurrentHashMap[String, HashSet[Int]]()
      var i: Int = 0
      while (i < requestBufferSize && !input.isEmpty) makeRequest()

      /**
       * This method returns the next joined tuple.
       *
       * *** THIS MUST BE IMPLEMENTED FOR THE ITERATOR TRAIT ***
       */
      override def next() = {
        // IMPLEMENT ME
        if (hasNext) output.dequeue
        else null
      }

      /**
       * This method returns whether or not this iterator has any data left to return.
       *
       * *** THIS MUST BE IMPLEMENTED FOR THE ITERATOR TRAIT ***
       */
      override def hasNext(): Boolean = {
        // IMPLEMENT ME
        if (output.size > 0) return true
        if (input.hasNext) {
          makeRequest()
        }
        if (requestBuffer.size > 0) {
          while (responses.size == 0) {}
        }
        if (responses.size > 0) {
          val requestsIter: Iterator[Int] = responses.keySet().iterator()
          while (requestsIter.hasNext) {
            val requestNum: Int = requestsIter.next()
            val ip: String = leftKeyGenerator.apply(requestBuffer.get(requestNum)).getString(0)
            val requestNums: HashSet[Int] = ipToRequestNum.get(ip)
            for (x <- 1 to requestNums.size) {
              output += new JoinedRow(requestBuffer.get(requestNum), responses.get(requestNum))
            }
            responses.remove(requestNum)
            requestBuffer.remove(requestNum)
            ipToRequestNum.remove(ip)
          }
          return true
        }
        false
      }


      /**
       * This method takes the next element in the input iterator and makes an asynchronous request for it.
       */
      private def makeRequest() = {
        // IMPLEMENT ME
        val row: Row = input.next
        val ip: String = leftKeyGenerator.apply(row).getString(0)
        if (responseCache.get(ip) != null) {
          output += new JoinedRow(row, responseCache.get(ip))
        } else {
          if (ipToRequestNum.get(ip) != null) {
            ipToRequestNum.get(ip) += i
          } else {
            val nums: HashSet[Int] = new HashSet[Int]()
            nums += i
            ipToRequestNum.put(ip, nums)
            requestBuffer.put(i, row)
            DNSLookup.lookup(i, ip, responses, requestBuffer)
          }
          i = i + 1
        }
      }
    }
  }
}
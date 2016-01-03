package mq.util

import org.scalatest.FlatSpec

/**
 * Created by Bruce on 4/11/15.
 */
class HashRingTest extends FlatSpec {

  var nodeList : List[HashRingNode] = List()
  nodeList = HashRingNode("a", 50) :: nodeList
  nodeList = HashRingNode("b", 50) :: nodeList
  nodeList = HashRingNode("c", 50) :: nodeList
  nodeList = HashRingNode("d", 50) :: nodeList
  nodeList = HashRingNode("e", 50) :: nodeList
  val hashRing = new HashRing(nodeList)

  "The values on hash ring based on different key" should "unique" in {
    assert(hashRing.get("1").get !== hashRing.get("2").get)
  }

  "The values on hash ring based on same key" should " be same" in {
    assert(hashRing.get("1").get === hashRing.get("1").get)
  }

}

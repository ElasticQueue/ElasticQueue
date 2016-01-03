import org.scalatest.FlatSpec
import collection.mutable.Stack
/**
 * Created by Bruce on 4/11/15.
 */
class FirstSpec extends FlatSpec {
  // tests go here...
  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new Stack[Int]()
    stack.push(1)
    stack.push(2)
    assert(stack.pop() === 2)
    assert(stack.pop() === 1)
  }
}

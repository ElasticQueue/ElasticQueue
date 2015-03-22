package mq.consumer

import mq.{Message, BrokerActor}

/**
 * Created by Bruce on 3/1/15.
 */
class ConsolePrint extends BrokerActor {

  def process(m: Message) = {
      println(m)
  }

}

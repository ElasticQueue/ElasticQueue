package mq

import com.datastax.driver.core.Cluster

/**
  * Created by bruce on 4/4/16.
  */
object Cassandra {

  val cluster = {
    Cluster.builder()
      .addContactPoint("127.0.0.1")
      .build()
  }

  val session = cluster.connect("elasticqueue")
}

package main.scala

import java.sql.{Connection, DriverManager}

object ImpalaJdbcSelect {

  /** pass it a connectionString that looks like the part of the url after "jdbc:impala://" **/
  def main(args: Array[String]): Unit = {
    val url = if (args.nonEmpty) s"jdbc:impala://${args(0)}"
      else "jdbc:impala://cloudera-mgr.stage.atl.impactradius.net:25003;AuthMech=1;KrbRealm=STAGE.IMPACTRADIUS.COM;KrbHostFQDN=cloudera-mgr.stage.atl.impactradius.net;KrbServiceName=impala"

    var connection: Connection = null

    try {
      Class.forName("com.cloudera.impala.jdbc4.Driver")
      connection = DriverManager.getConnection(url)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * from irdw_prod.campaign_dim LIMIT 5;")
      println("I made a connection without freaking out. Let's see if I can get some data back.")
      while ( resultSet.next() ) {
        println(s"${resultSet.getString("campaign_id")} - ${resultSet.getString("campaign_name")} - ${resultSet.getString("campaign_state")}")
      }
    } catch {
      case e: Throwable => {
        println("No dice, cannot connect. Here's some errors.")
        e.printStackTrace()
      }
    }
    connection.close()
  }
}

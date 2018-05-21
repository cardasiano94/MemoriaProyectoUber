/*
 * Copyright (c) 2017 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package examples
// -----
import java.sql.{Connection,DriverManager}
// -----
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.util.ElasticSensitivity

/** A simple differential privacy example using elastic sensitivity.
  *
  * This example code supports queries that return a single column and single row. The code can be extended to support
  * queries returning multiple columns and rows by generating independent noise samples for each cell based the
  * appropriate column sensitivity.
  *
  * Caveats:
  *
  * Histogram queries (using SQL's GROUP BY) must be handled carefully so as not to leak information in the bin labels.
  * The analysis throws an error to warn about this, but this behavior can overridden if you know what you're doing.
  *
  * This example does not implement a privacy budget management strategy. Each query is executed using the full budget
  * value of EPSILON. Correct use of differential privacy requires allocating a fixed privacy from which a portion is
  * depleted to run each query. A privacy budget strategy depends on the problem domain and threat model and is
  * therefore beyond the scope of this tool.
  */
object ElasticSensitivityExample extends App {
  //--------------------------------------------
  // connect to the database named "mysql" on port 8889 of localhost
  val url = "jdbc:mysql://localhost:3306/Memoria"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "root1234"
  var connection:Connection = _
  var result = 0
  var z = Array("TABLE")
  var q = ""
  try {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    //++++++++++++++++++++++++++++
    var databaseMetaData = connection.getMetaData()
    var resultSet = databaseMetaData.getTables(null,null,null,z)
    System.out.println("Printing TABLE_TYPE \"TABLE\" ");
    System.out.println("----------------------------------");
    while(resultSet.next())
    {
      //Print
      System.out.println(resultSet.getString("TABLE_NAME"));
    }

    var columns = databaseMetaData.getColumns(null,null,"llamadas",null)
    while(columns.next())
    {
      var columnName = columns.getString("COLUMN_NAME");
      var datatype = columns.getString("DATA_TYPE");
      var columnsize = columns.getString("COLUMN_SIZE");
      var decimaldigits = columns.getString("DECIMAL_DIGITS");
      var isNullable = columns.getString("IS_NULLABLE");
      var is_autoIncrment = columns.getString("IS_AUTOINCREMENT");
      //Printing results
      System.out.println(columnName + "---" + datatype + "---" + columnsize + "---" + decimaldigits + "---" + isNullable + "---" + is_autoIncrment);
    }
    //++++++++++++++++++++++++++++

    val statement = connection.createStatement
    val rs = statement.executeQuery("SELECT COUNT(tipo_string) FROM llamadas WHERE tipo_string=\"O\";")
    while (rs.next) {
      //val user = rs.getString("numa_string")
      result = rs.getInt("COUNT(tipo_string)")
      println("result = %s".format(result))
    }
  } catch {
    case e: Exception => e.printStackTrace
  }
  connection.close
  //--------------------------------------------

  // Use the table schemas and metadata defined by the test classes
  System.setProperty("schema.config.path", "src/test/resources/schema.yaml")
  val database = Schema.getDatabase("test1")

  // example query: How many US customers ordered product #1?
  val query = """
    SELECT COUNT(*) FROM orders
    JOIN customers ON orders.customer_id = customers.customer_id
    WHERE orders.product_id = 1 AND customers.address LIKE '%United States%'
  """
  val query2 = """
    SELECT COUNT(*) FROM DataCDR
    JOIN MediacionVoiceCDR ON DataCDR.numa = MediacionVoiceCDR.numa
    WHERE MediacionVoiceCDR.numb = 1
  """
  val query3 = """
    SELECT COUNT(numa) FROM DataCDR
  """

  // query result when executed on the database
  val QUERY_RESULT = 100000
  val QUERY_RESULT2 = 10000

  // privacy budget
  val EPSILON = 0.1
  // delta parameter: use 1/n^2, with n = 100000
  val DELTA = 1 / (math.pow(100000,2))

  println(s"Query: $query2")
  println(s"Private result: $result\n")

  (1 to 10).foreach { i =>
    val noisyResult = ElasticSensitivity.addNoise(query3, database, result, EPSILON, DELTA)
    println(s"Noisy result (run $i): %.0f".format(noisyResult))
  }
}

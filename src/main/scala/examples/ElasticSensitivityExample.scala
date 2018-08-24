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
import scala.io.Source
import java.io._
// -----
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.util.ElasticSensitivity

import org.apache.jena.query._


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
  /**
  val model =  "vc-db-1.rdf"
  val dataset = DatasetFactory.create(model)
  val queryString = "SELECT ?x\nWHERE { ?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  \"John Smith\" }"
  val queryRDF = QueryFactory.create(queryString)

  try{
    val qexec = QueryExecutionFactory.create(queryRDF, dataset)
    val results = qexec.execSelect()
    while(results.hasNext()){
      val soln = results.nextSolution()
      val x = soln.get("x") ;       // Get a result variable by name.
      //val r = soln.getResource("x") ; // Get a result variable - must be a resource
      //val l = soln.getLiteral("x") ;   // Get a result variable - must be a literal
      System.out.println(x);
    }
  }catch {
    case e: Exception => e.printStackTrace
  }*/

  //--------------------------------------------
  // connect to the database named "mysql" on port 8889 of localhost
  val url = "jdbc:mysql://localhost:3306/TPC-H"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "root1234"
  var connection:Connection = _
  var result = 0.0
  var z = Array("TABLE")
  var q = ""
  //SELECT count(DISTINCT ONGs.idONG) FROM ONGs JOIN Donaciones ON ONGs.idONG = Donaciones.idONG WHERE Categoria LIKE 'SALUD' and MONTO_TOTAL_DONACION >4000000
  //SELECT count(*) FROM heroes_information JOIN super_hero_powers ON heroes_information.name = super_hero_powers.hero_names WHERE Publisher LIKE 'Marvel Comics' and Agility='False'
  //SELECT count(distinct openpowerlifting.Name) FROM meets JOIN openpowerlifting ON meets.MeetID = openpowerlifting.MeetID WHERE Age > 30 AND MeetCountry LIKE 'USA' AND Wilks > 120

  /**
  SELECT count(distinct pop.ID)
             FROM playerOnlyPersonal AS pop
                    JOIN playerattributedata AS pad ON pop.ID = pad.ID
                    JOIN playerplayingpositiondata1 AS pppd1 ON pop.ID = pppd1.ID
                    JOIN playerplayingpositiondata2 AS pppd2 ON pop.ID = pppd2.ID
                    JOIN playerdataMoney AS pdm ON pop.ID = pdm.ID
              WHERE pop.Age < 30 AND
             			   pad.Strength > 65

  SELECT count(distinct info1.name)
	FROM info1
		JOIN info2 ON info1.name = info2.name
        JOIN powers1 ON info1.name = powers1.hero_names
        JOIN powers2 ON info1.name = powers2.hero_names
	WHERE Publisher LIKE 'Marvel Comics'


  SELECT count(distinct orders.ORDERKEY)
	FROM orders
		JOIN customer ON orders.CUSTKEY = customer.CUSTKEY
    JOIN lineitem ON orders.ORDERKEY = lineitem.ORDERKEY
    JOIN partsupp ON lineitem.PARTKEY = partsupp.PARTKEY
    JOIN supplier ON supplier.SUPPKEY = partsupp.SUPPKEY


    */



  val definitiveQuery =
    """
      SELECT count(distinct orders.ORDERKEY)
      	FROM orders
      		JOIN customer ON orders.CUSTKEY = customer.CUSTKEY
          JOIN lineitem ON orders.ORDERKEY = lineitem.ORDERKEY
          JOIN partsupp ON lineitem.PARTKEY = partsupp.PARTKEY
          JOIN supplier ON supplier.SUPPKEY = partsupp.SUPPKEY
    """

  var tableNames: List[String] = List()
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
      tableNames = resultSet.getString("TABLE_NAME") :: tableNames
      //System.out.println(tableName);
    }
    //testing
    val pw = new PrintWriter(new File("schemazo.yaml" ))
    pw.write("---\n" + "databases:\n"+"- database: \"test\"\n" + "  dialect: \"hive\"\n" + "  namespace: \"public\"\n" + "  tables:\n")
    //endTesting
    for(tableName <- tableNames){
      pw.write("  - table: \"" +tableName + "\"\n" + "    columns:\n")
      var  columns = databaseMetaData.getColumns(null,null,tableName,null)
      while(columns.next())
      {
        var columnName = columns.getString("COLUMN_NAME");
        pw.write("    - name: \"" + columnName + "\"\n")
        var datatype = columns.getString("DATA_TYPE");
        var columnsize = columns.getString("COLUMN_SIZE");
        var decimaldigits = columns.getString("DECIMAL_DIGITS");
        var isNullable = columns.getString("IS_NULLABLE");
        var is_autoIncrment = columns.getString("IS_AUTOINCREMENT");
        //Printing results
        System.out.println(columnName + "---" + datatype + "---" + columnsize + "---" + decimaldigits + "---" + isNullable + "---" + is_autoIncrment);
      }
    }
    //++++++++++++++++++++++++++++
    pw.close
    val statement = connection.createStatement

    val rs = statement.executeQuery(definitiveQuery)
    while (rs.next) {
      //val user = rs.getString("numa_string")
      result = rs.getDouble(1)
      println("result = %s".format(result))
    }
  } catch {
    case e: Exception => e.printStackTrace
  }
  connection.close

  //--------------------------------------------

  // Use the table schemas and metadata defined by the test classes
  //System.setProperty("schema.config.path", "src/test/resources/schema.yaml")
  System.setProperty("schema.config.path", "schemaa.yaml")
  val database = Schema.getDatabase("test")
  

  // query result when executed on the database
  val QUERY_RESULT = 100000
  val QUERY_RESULT2 = 10000

  // privacy budget
  val EPSILON = 0.1
  // delta parameter: use 1/n^2, with n = 100000
  val DELTA = 1 / (math.pow(100000,2))

  println(s"Query: $definitiveQuery")
  println(s"Private result: $result\n")

  (1 to 10).foreach { i =>
    val noisyResult = ElasticSensitivity.addNoise(definitiveQuery, database, result, EPSILON, DELTA)
    println(s"Noisy result (run $i): %.0f".format(noisyResult))
  }
}

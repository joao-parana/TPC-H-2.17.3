
import $ivy.`org.apache.spark::spark-mllib:2.3.2`
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
// Para usar User-defined aggregate function - UDAF devemos 
// importar a lista abaixo:
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer,UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
// import org.apache.spark.sql.types.Metadata;
Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
var numCores = 3
val sparkBuilder: Builder = SparkSession.builder()
var sparkSession: SparkSession = null
sparkSession = sparkBuilder.master("local[" + numCores + "]").
          appName("MyApp").getOrCreate()
val spark = sparkSession
import spark.implicits._ // requisito para algumas funcionalidades
val sparkContext: SparkContext = spark.sparkContext

// diretório: dbgen/DATA 
val dir = "dbgen/DATA"

// files: customers.csv  lineitems.csv  orders.csv

val META_EMPTY = Metadata.empty

val customerSchema : StructType = StructType(Array(
  StructField("c_custkey", IntegerType, true, META_EMPTY),
  StructField("c_name", StringType, true, META_EMPTY),
  StructField("c_address", StringType, true, META_EMPTY),
  StructField("c_nationkey", IntegerType, true, META_EMPTY),
  StructField("c_phone", StringType, true, META_EMPTY),
  StructField("c_acctbal", DoubleType, true, META_EMPTY),
  StructField("c_mktsegment", StringType, true, META_EMPTY),
  StructField("c_comment", StringType, true, META_EMPTY)
  )
)

// val customers = spark.read.option("delimiter", "|").
//       option("inferSchema", "true").
//       option("header", "true").
//       csv(s"${dir}/customers.csv")

val customers = spark.read.option("delimiter", "|").
      option("inferSchema", "false").
      option("header", "true").
      schema(customerSchema).
      csv(s"${dir}/customers.csv")

val orderSchema : StructType = StructType(Array(
  StructField("o_orderkey", IntegerType, true, META_EMPTY),
  StructField("o_custkey", IntegerType, true, META_EMPTY),
  StructField("o_orderstatus", StringType, true, META_EMPTY),
  StructField("o_totalprice", DoubleType, true, META_EMPTY),
  StructField("o_orderdate", TimestampType, true, META_EMPTY),
  StructField("o_orderpriority", StringType, true, META_EMPTY),
  StructField("o_clerk", StringType, true, META_EMPTY),
  StructField("o_shippriority", IntegerType, true, META_EMPTY),
  StructField("o_comment", StringType, true, META_EMPTY)
  )
)

// val orders = spark.read.option("delimiter", "|").
//       option("inferSchema", "true").
//       option("header", "true").
//       csv(s"${dir}/orders.csv")

val orders = spark.read.option("delimiter", "|").
      option("inferSchema", "false").
      option("header", "true").
      schema(orderSchema).
      csv(s"${dir}/orders.csv")

val lineitemSchema : StructType = StructType(Array(
  StructField("l_orderkey", IntegerType, true, META_EMPTY),
  StructField("l_partkey", IntegerType, true, META_EMPTY),
  StructField("l_suppkey", IntegerType, true, META_EMPTY),
  StructField("l_linenumber", IntegerType, true, META_EMPTY),
  StructField("l_quantity", IntegerType, true, META_EMPTY),
  StructField("l_extendedprice", DoubleType, true, META_EMPTY),
  StructField("l_discount", DoubleType, true, META_EMPTY),
  StructField("l_tax", DoubleType, true, META_EMPTY),
  StructField("l_returnflag", StringType, true, META_EMPTY),
  StructField("l_linestatus", StringType, true, META_EMPTY),
  StructField("l_shipdate", TimestampType, true, META_EMPTY),
  StructField("l_commitdate", TimestampType, true, META_EMPTY),
  StructField("l_receiptdate", TimestampType, true, META_EMPTY),
  StructField("l_shipinstruct", StringType, true, META_EMPTY),
  StructField("l_shipmode", StringType, true, META_EMPTY),
  StructField("l_comment", StringType, true, META_EMPTY)
  )
)

val lineitems = spark.read.option("delimiter", "|").
      option("inferSchema", "false").
      option("header", "true").
      schema(lineitemSchema).
      csv(s"${dir}/lineitems.csv")

// LISTANDO CONTEUDO - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

val distinctCommentsOnCustomers = customers.select(customers("c_comment")).distinct
distinctCommentsOnCustomers.sample(false, 0.0001).show(false)
 
// +-----------------------------------------------------------------------------------------------------+
// |c_comment                                                                                            |
// +-----------------------------------------------------------------------------------------------------+
// |he requests. regular, even requests wake furiously. final, unusual                                   |
// |across the ironic deposits. blithely regular requests cajole foxes. furio                            |
// | blithely ironic theodolites                                                                         |
// |nd furiously daring accounts. slyly bold requests nod after the requests. pending as                 |
// |ly never bold accounts. even deposits hag                                                            |
// |poach. blithely final theodolites cajole furiously quickly bold accounts. slyl                       |
// |ges. quickly bold packages nod according to the even, express asymptotes. fu                         |
// |ons. accounts according to the deposits haggle bl                                                    |
// | the bold packages print before the                                                                  |
// |requests affix. blithely unusual hockey players haggle carefully blithely silent deposits. bold, unus|
// |nal, regular patterns. packages sleep slyly qui                                                      |
// +-----------------------------------------------------------------------------------------------------+

distinctCommentsOnCustomers.sample(false, 0.0001).orderBy(desc("c_comment")).show(false)


customers.sample(false, 0.0001).show(false)

// +---------+------------------+--------------------------------------+-----------+---------------+---------+------------+------------------------------------------------------------------------------------------------------------------+
// |c_custkey|c_name            |c_address                             |c_nationkey|c_phone        |c_acctbal|c_mktsegment|c_comment                                                                                                         |
// +---------+------------------+--------------------------------------+-----------+---------------+---------+------------+------------------------------------------------------------------------------------------------------------------+
// |6562     |Customer#000006562|6XINODN,YblT3W5FrWSo2voo7MeU5kv8hTni  |14         |24-485-841-2292|6057.4   |BUILDING    |s accounts. regular, ironic theodolites haggle                                                                    |
// |7028     |Customer#000007028|0JZFbmjuX3dWCUYkbFzC3,,o5BP3bDZNAoLLnG|12         |22-103-812-9864|4268.32  |AUTOMOBILE  |s haggle slyly bold deposits! furiously ironic accounts cajole al                                                 |
// |7419     |Customer#000007419|NtW1rlB6CnEZSQtCSv,JAX1a              |15         |25-792-319-3446|2789.17  |AUTOMOBILE  |against the blithely final excuses. blithely final                                                                |
// |15745    |Customer#000015745|OlV8518JgCSbV0ov6,KmhyZzjd3nWcRu6yT   |4          |14-627-830-8330|6053.84  |MACHINERY   |the quickly final notornis. ironic pinto beans use. platelets sleep car                                           |
// |17553    |Customer#000017553|567PjqLbOpbFhkgTcoEX                  |0          |10-588-200-2569|838.32   |MACHINERY   |blithely silent courts are final reque                                                                            |
// |19232    |Customer#000019232|Dm,H8XLcMu6l4fa3EWUVp8ot3Kt,GnEbmCxm  |12         |22-594-641-5150|5504.63  |HOUSEHOLD   |out the accounts. fluffily pending packages sleep blithely against the pinto beans.                               |
// |39259    |Customer#000039259|fnXF7iIQhuv93                         |4          |14-232-245-7530|4821.85  |FURNITURE   |quickly unusual requests sleep. special pinto be                                                                  |
// |56666    |Customer#000056666|9E3SBW6Dt4xNIf01ND1vFtzXBdd0          |9          |19-971-879-5564|8591.23  |MACHINERY   |. carefully pending packages wake blithely thin escapades. s                                                      |
// |81139    |Customer#000081139|bU ZjptwamqV4i                        |10         |20-444-570-2605|1735.54  |HOUSEHOLD   |nal instructions sleep quietly slyly final foxes. regular dep                                                     |
// |84624    |Customer#000084624|4n5cQY1EjTpF,WIcBkbB2xQdCo            |19         |29-584-848-9188|7024.18  |HOUSEHOLD   |ounts cajole alongside of the c                                                                                   |
// |85590    |Customer#000085590| F9pXzc8UL                            |15         |25-421-791-7184|5920.23  |BUILDING    |ts. carefully regular requests cajole quickly. slyly express inst                                                 |
// |89694    |Customer#000089694|HNwnTSIs B3fcMI8P04hA0SMP             |21         |31-972-476-7919|3459.13  |BUILDING    |ess requests nag blithely. ironic theodolites haggle slyly across the bold theodolites. enticingly regular request|
// |99191    |Customer#000099191|ktS,6XRJXIyxsQCaRDl4                  |1          |11-372-477-2586|3014.25  |MACHINERY   |ole slyly. packages cajole special accounts. final dugouts are blithely bold,                                     |
// |122515   |Customer#000122515|55RbPPbKBi C5MVV                      |18         |28-241-449-5135|840.95   |MACHINERY   |sual instructions about the final requests haggle furiously above th                                              |
// |127060   |Customer#000127060|rdiMd73, 9kL5Ef,LgBKsbI               |11         |21-875-654-1350|4415.76  |HOUSEHOLD   |sits haggle regular platelets. sly courts eat fluffily after t                                                    |
// |129716   |Customer#000129716|fQEu7c2pW21wbGVHr4px112rZoFoJH6tv     |20         |30-271-149-1830|1377.97  |BUILDING    |nd the quickly bold sauternes. slyly final accounts wake blithely. regular,                                       |
// |137041   |Customer#000137041| DaCLoBxpY,vy6VjdYdbjcI4ZlaRS5zpgLc   |20         |30-478-448-9817|7713.25  |MACHINERY   |ffily pending dependencies. pending requests use deposits. final, final                                           |
// |145848   |Customer#000145848|Ua0X5D7Fgg9PP 6oJ EL                  |10         |20-224-776-3790|2859.23  |AUTOMOBILE  |bold pinto beans use slyly against the slyly regular accounts. furiously express excuses was ac                   |
// +---------+------------------+--------------------------------------+-----------+---------------+---------+------------+------------------------------------------------------------------------------------------------------------------+

orders.sample(false, 0.0001).head(20).foreach(println)

// [18880,129856,F,158903.85,1994-10-25 00:00:00.0,4-NOT SPECIFIED,Clerk#000000470,0,ts cajole blithely slyly final deposits. blithely bold ideas grow along t]
// [50243,53134,F,86080.63,1994-10-25 00:00:00.0,4-NOT SPECIFIED,Clerk#000000341,0, haggle alongside of the instructions. regular, bold p]
// [61152,145646,O,61848.74,1998-01-10 00:00:00.0,5-LOW,Clerk#000000244,0,f the slyly express packages. r]
// [67463,116731,O,27100.15,1998-04-28 00:00:00.0,3-MEDIUM,Clerk#000000178,0,out the finally unusual requests haggle always slyly silent foxes. pending]
// [169414,96427,F,82896.92,1993-06-28 00:00:00.0,1-URGENT,Clerk#000000849,0,thely ironic sentiments cajole carefully after the accounts. fluffily iron]
// [213411,95926,F,123367.43,1993-05-26 00:00:00.0,4-NOT SPECIFIED,Clerk#000000376,0,ts nag slyly unusual dependencies. even ]
// [270789,137081,F,73883.34,1993-04-17 00:00:00.0,2-HIGH,Clerk#000000284,0,ly alongside of the furiously ]
// [295718,76979,O,238532.4,1998-02-23 00:00:00.0,4-NOT SPECIFIED,Clerk#000000998,0, ironic pinto beans. silent, regul]
// [368518,10636,O,116734.53,1995-04-23 00:00:00.0,2-HIGH,Clerk#000000750,0,nto beans maintain. fu]
// [379488,67102,F,199843.17,1994-08-11 00:00:00.0,2-HIGH,Clerk#000000054,0, packages haggle furiously slyly bold notorn]
// [409095,55399,O,26338.16,1996-10-01 00:00:00.0,4-NOT SPECIFIED,Clerk#000000143,0,luffily final ideas. blit]
// [461185,61301,F,92334.52,1994-05-02 00:00:00.0,5-LOW,Clerk#000000087,0,en instructions! orbits cajole carefully pend]
// [543847,23341,F,193825.72,1992-01-23 00:00:00.0,2-HIGH,Clerk#000000491,0,ect fluffily among the eve]
// [587748,140896,F,24268.72,1992-05-17 00:00:00.0,2-HIGH,Clerk#000000509,0,sits. deposits haggle. express deposits could sleep slyly blith]
// [605316,94243,O,112258.73,1998-02-12 00:00:00.0,5-LOW,Clerk#000000907,0, unusual, regular requests. carefully ]
// [617796,23395,F,21350.22,1992-05-31 00:00:00.0,2-HIGH,Clerk#000000748,0,against the blithely regular packages unw]
// [663427,126799,O,227671.6,1998-03-08 00:00:00.0,1-URGENT,Clerk#000000394,0,arefully even braids nod blith]
// [714087,118112,F,41251.87,1994-12-28 00:00:00.0,4-NOT SPECIFIED,Clerk#000000975,0,regular ideas haggle along the close ideas. furiously ironic ideas alongside]
// [763104,77290,O,129274.42,1997-10-22 00:00:00.0,5-LOW,Clerk#000000875,0,s about the blithely final foxes cajole quickly packages. final requ]
// [802885,134128,F,150210.09,1993-02-20 00:00:00.0,1-URGENT,Clerk#000000109,0,eas affix along the bold packages-- bl]

orders.show(10) 

// +----------+---------+-------------+------------+-------------------+---------------+---------------+--------------+--------------------+
// |o_orderkey|o_custkey|o_orderstatus|o_totalprice|        o_orderdate|o_orderpriority|        o_clerk|o_shippriority|           o_comment|
// +----------+---------+-------------+------------+-------------------+---------------+---------------+--------------+--------------------+
// |         1|    36901|            O|   173665.47|1996-01-02 00:00:00|          5-LOW|Clerk#000000951|             0|nstructions sleep...|
// |         2|    78002|            O|    46929.18|1996-12-01 00:00:00|       1-URGENT|Clerk#000000880|             0| foxes. pending a...|
// |         3|   123314|            F|   193846.25|1993-10-14 00:00:00|          5-LOW|Clerk#000000955|             0|sly final account...|
// |         4|   136777|            O|    32151.78|1995-10-11 00:00:00|          5-LOW|Clerk#000000124|             0|sits. slyly regul...|
// |         5|    44485|            F|    144659.2|1994-07-30 00:00:00|          5-LOW|Clerk#000000925|             0|quickly. bold dep...|
// |         6|    55624|            F|    58749.59|1992-02-21 00:00:00|4-NOT SPECIFIED|Clerk#000000058|             0|ggle. special, fi...|
// |         7|    39136|            O|   252004.18|1996-01-10 00:00:00|         2-HIGH|Clerk#000000470|             0|ly special requests |
// |        32|   130057|            O|   208660.75|1995-07-16 00:00:00|         2-HIGH|Clerk#000000616|             0|ise blithely bold...|
// |        33|    66958|            F|   163243.98|1993-10-27 00:00:00|       3-MEDIUM|Clerk#000000409|             0|uriously. furious...|
// |        34|    61001|            O|    58949.67|1998-07-21 00:00:00|       3-MEDIUM|Clerk#000000223|             0|ly final packages...|
// +----------+---------+-------------+------------+-------------------+---------------+---------------+--------------+--------------------+

val commentsOnOrder = orders.orderBy("o_comment").map((x) => x.getString(8))
// Mostra os 30 primeiros na ordem lexográfica
commentsOnOrder.show(30)
val distinctCommentsAndPriority = orders.select(orders("o_comment"), orders("o_orderpriority")).distinct 
val distinctComments = orders.select(orders("o_comment")).distinct 
distinctComments.count
distinctComments.show(10, false)
distinctComments.head(10)
distinctComments.sample(false, 0.0001).head(100).foreach(println)
distinctComments.first


lineitems.show(10)

// +----------+---------+---------+------------+----------+---------------+----------+-----+------------+------------+-------------------+-------------------+-------------------+-----------------+----------+--------------------+
// |l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|l_returnflag|l_linestatus|         l_shipdate|       l_commitdate|      l_receiptdate|   l_shipinstruct|l_shipmode|           l_comment|
// +----------+---------+---------+------------+----------+---------------+----------+-----+------------+------------+-------------------+-------------------+-------------------+-----------------+----------+--------------------+
// |         1|   155190|     7706|           1|        17|       21168.23|      0.04| 0.02|           N|           O|1996-03-13 00:00:00|1996-02-12 00:00:00|1996-03-22 00:00:00|DELIVER IN PERSON|     TRUCK|egular courts abo...|
// |         1|    67310|     7311|           2|        36|       45983.16|      0.09| 0.06|           N|           O|1996-04-12 00:00:00|1996-02-28 00:00:00|1996-04-20 00:00:00| TAKE BACK RETURN|      MAIL|ly final dependen...|
// |         1|    63700|     3701|           3|         8|        13309.6|       0.1| 0.02|           N|           O|1996-01-29 00:00:00|1996-03-05 00:00:00|1996-01-31 00:00:00| TAKE BACK RETURN|   REG AIR|riously. regular,...|
// |         1|     2132|     4633|           4|        28|       28955.64|      0.09| 0.06|           N|           O|1996-04-21 00:00:00|1996-03-30 00:00:00|1996-05-16 00:00:00|             NONE|       AIR|lites. fluffily e...|
// |         1|    24027|     1534|           5|        24|       22824.48|       0.1| 0.04|           N|           O|1996-03-30 00:00:00|1996-03-14 00:00:00|1996-04-01 00:00:00|             NONE|       FOB| pending foxes. s...|
// |         1|    15635|      638|           6|        32|       49620.16|      0.07| 0.02|           N|           O|1996-01-30 00:00:00|1996-02-07 00:00:00|1996-02-03 00:00:00|DELIVER IN PERSON|      MAIL|   arefully slyly ex|
// |         2|   106170|     1191|           1|        38|       44694.46|       0.0| 0.05|           N|           O|1997-01-28 00:00:00|1997-01-14 00:00:00|1997-02-02 00:00:00| TAKE BACK RETURN|      RAIL|ven requests. dep...|
// |         3|     4297|     1798|           1|        45|       54058.05|      0.06|  0.0|           R|           F|1994-02-02 00:00:00|1994-01-04 00:00:00|1994-02-23 00:00:00|             NONE|       AIR|ongside of the fu...|
// |         3|    19036|     6540|           2|        49|       46796.47|       0.1|  0.0|           R|           F|1993-11-09 00:00:00|1993-12-20 00:00:00|1993-11-24 00:00:00| TAKE BACK RETURN|      RAIL| unusual accounts...|
// |         3|   128449|     3474|           3|        27|       39890.88|      0.06| 0.07|           A|           F|1994-01-16 00:00:00|1993-11-22 00:00:00|1994-01-23 00:00:00|DELIVER IN PERSON|      SHIP|    nal foxes wake. |
// +----------+---------+---------+------------+----------+---------------+----------+-----+------------+------------+-------------------+-------------------+-------------------+-----------------+----------+--------------------+

// Algumas brincadeiras.

val someCustomersOnlyKeyAndComment = customers.select(customers("c_custkey"), customers("c_comment")).sample(false, 0.0001)

case class CustomerKeyAndComment(c_custkey:Int, c_comment:String){ }

someCustomersOnlyKeyAndComment.flatMap((c) => {
  val tokens = c.getString(1).split(" ").sorted
  // var ret = new ListBuffer[String]()
  tokens.map((e) => CustomerKeyAndComment(c.getInt(0), e)) 
}).filter(e => e.c_custkey < 26000).show(false) // .as[CustomerKeyAndComment].show(false)

customers.filter(x => x(0).asInstanceOf[Int] < 3).map(e => e(0).asInstanceOf[Int].toString() + ": " + e(7).asInstanceOf[String]).show(false) 

customers.flatMap((c) => {
  val tokens = c.getString(7).split(" ").sorted
  tokens.map((e) => Tuple2(c.getInt(0), e)) 
}).filter(e => e._1 < 3).show(false)

case class Customer(c_custkey: Int, c_name: String, c_address: String, c_nationkey: Int, c_phone: String, c_acctbal: Double, c_mktsegment: String, c_comment: String) {}

val customersFlatedOnComment = customers.flatMap((c) => {
  val tokens = c.getString(7).split(" ").sorted
  tokens.map((e) => Customer(c.getInt(0), c.getString(1), c.getString(2), c.getInt(3), c.getString(4), c.getDouble(5), c.getString(6), e)) 
}).filter(e => e.c_custkey < 4)
customersFlatedOnComment.show(false)


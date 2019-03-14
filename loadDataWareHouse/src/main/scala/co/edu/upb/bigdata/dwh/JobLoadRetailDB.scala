  package co.edu.upb.bigdata.dwh

  import java.util.Properties

  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.{DataFrame, SQLContext,Dataset}
  import org.apache.spark.{SparkConf, SparkContext}

  object JobLoadRetailDB {

    private var sparkContext: SparkContext = _
    private var sqlContext: SQLContext = _

    private final val FILE_SISTEM_HOME :String= "hdfs://localhost:8020/user/cloudera/"
    private final val DATAWAREHOUSE :String= FILE_SISTEM_HOME +"dataWareHouse/"
    private final val DATAMARK :String= FILE_SISTEM_HOME +"dataMark/"
    private final val PARQUET_EXT :String= ".parquet"

    private final val PWD_MYSQL :String= "cloudera"
    private final val USR_MYSQL :String= "root"
    private final val URL_MYSQL :String= "jdbc:mysql://localhost:3306/retail_db?useSSL=false"

    def main(args: Array[String]): Unit = {
      try{
        obtenerSparkContexto()
        cargarTablas()
        crearModeloEstrella()
      }finally {
        cerrarSparkSession()
      }
    }

    def cerrarSparkSession(): Unit = {
      sparkContext.stop()
    }

    def obtenerSparkContexto(): Unit = {
      val master: String = "local[*]"
      val appName: String = "MyApp"
      val conf: SparkConf = new SparkConf()
        .setMaster(master)
        .setAppName(appName)
        .set("spark.driver.allowMultipleContexts", "false")
        .set("spark.ui.enabled", "false")
      sparkContext = new SparkContext(conf)
      sqlContext = new SQLContext(sparkContext)
    }

    def cargarTablas(): Unit ={
      var tablas = Array("departments", "categories", "customers", "order_items", "orders", "products")
      tablas.foreach(e => transformarMysqlTable_ParquetFile(e, DATAWAREHOUSE))
    }

    private def transformarMysqlTable_ParquetFile(tableName :String, urlDfs :String): Unit ={
      val prop = new Properties()
      prop.put("user", USR_MYSQL)
      prop.put("password", PWD_MYSQL)

      var table: String = "retail_db."+tableName
      var urlD: String = urlDfs+tableName+PARQUET_EXT

      var dataFrm: DataFrame = sqlContext.read.jdbc(URL_MYSQL, table, prop)
      dataFrm.write.mode("overwrite").format("parquet").save(urlD)
    }

    def crearModeloEstrella(): Unit ={
      crearNodoEstados()
      crearNodoCiudades()
      crearNodoClientes()
      crearNodoDepartamentos()
      crearNodoProductos()
      crearNodoCategortias()
      crearNodoHechos()
      crearNodoTiempo()
    }

    def crearNodoEstados(): Unit ={
      val hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)
      var dataFrame: DataFrame = hiveContext.read.parquet(DATAWAREHOUSE+"orders"+PARQUET_EXT)
      var nodoEstado = dataFrame
        .selectExpr("(order_status) as status_name")
        .distinct()
        .withColumn("status_id", row_number().over(Window.orderBy("status_name")))
      //nodoEstado.show(100) // Solo para entorno desarrollo
      var urlD: String = DATAMARK+"status"+PARQUET_EXT
      nodoEstado.write.mode("overwrite").format("parquet").save(urlD)
    }

    def crearNodoCiudades(): Unit ={
      val hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)
      var dataFrame: DataFrame = hiveContext.read.parquet(DATAWAREHOUSE+"customers"+PARQUET_EXT)

      var nodoCiudad = dataFrame
        .selectExpr(
          "(customer_city) as city_name",
          "(customer_state) as city_state")
        .distinct()
        .withColumn("city_id",row_number().over(Window.orderBy("city_name")))
      //nodoCiudad.show(100)
      var urlD: String = DATAMARK+"cites"+PARQUET_EXT
      nodoCiudad.write.mode("overwrite").format("parquet").save(urlD)
    }

    def crearNodoClientes(): Unit ={
      val readData: DataFrame = sqlContext.read.parquet(DATAWAREHOUSE + "customers" + PARQUET_EXT)
      def aleatorio = new scala.util.Random()
      val maxAleatorio = 100
      val nextRandomIntUdf = udf(() => aleatorio.nextInt(maxAleatorio))
      val nodoCliente = readData.selectExpr(
        "customer_id",
        "customer_fname",
        "customer_lname",
        "concat(customer_fname,'.',customer_lname,'@upb.',customer_city,'com') as customer_email",
        "customer_street",
        "customer_city",
        "customer_state",
        "customer_zipcode" )
        .withColumn("customer_password", nextRandomIntUdf())
      //nodoCliente.show(100)
      var urlD: String = DATAMARK+"customers"+PARQUET_EXT
      nodoCliente.write.mode("overwrite").format("parquet").save(urlD)
    }

    def crearNodoDepartamentos(): Unit ={
      var dataFrame: DataFrame = sqlContext.read.parquet(DATAWAREHOUSE+"departments"+PARQUET_EXT)
      var nodoDepartamento = dataFrame
        .selectExpr(
          "department_id",
          "department_name")
      //nodoDepartamento.show(100)
      var urlD: String = DATAMARK+"departments"+PARQUET_EXT
      nodoDepartamento.write.mode("overwrite").format("parquet").save(urlD)
    }

    def crearNodoProductos(): Unit ={
      var dataFrame: DataFrame = sqlContext.read.parquet(DATAWAREHOUSE+"products"+PARQUET_EXT)
      var nodoProductos = dataFrame
        .selectExpr(
          "product_id",
          "product_category_id",
          "product_name",
          "product_description",
          "product_price",
          "product_image")
      //nodoProductos.show(100)
      var urlD: String = DATAMARK+"products"+PARQUET_EXT
      nodoProductos.write.mode("overwrite").format("parquet").save(urlD)
    }

    def crearNodoCategortias(): Unit ={
      var dataFrame: DataFrame = sqlContext.read.parquet(DATAWAREHOUSE+"categories"+PARQUET_EXT)
      var nodoCategory = dataFrame
        .selectExpr(
          "category_id",
          "category_department_id",
          "category_name")
      //nodoCategory.show(100)
      var urlD: String = DATAMARK+"categories"+PARQUET_EXT
      nodoCategory.write.mode("overwrite").format("parquet").save(urlD)
    }

    def crearNodoTiempo(): Unit ={
      var dataFrame: DataFrame = sqlContext.read.parquet(DATAWAREHOUSE+"orders"+PARQUET_EXT)
      def roundUp(d: Double) = math.ceil(d).toInt

      var nodoTiempo = dataFrame
        .selectExpr("(order_date) as date")
        .distinct()
        .withColumn("day",dayofmonth(col("date")))
        .withColumn("month",month(col("date")))
        .withColumn("year",year(col("date")))
        .withColumn("trimester", quarter(col("date")))
        .withColumn("semester", round(quarter(col("date"))/2))
      //nodoTiempo.show(100)
      var urlD: String = DATAMARK+"timer"+PARQUET_EXT
      nodoTiempo.write.mode("overwrite").format("parquet").save(urlD)
    }

    def crearNodoHechos(): Unit ={

      var departments: DataFrame = sqlContext.read.parquet(DATAWAREHOUSE+"departments"+PARQUET_EXT)
      departments.registerTempTable("departments")
      var categories: DataFrame = sqlContext.read.parquet(DATAWAREHOUSE+"categories"+PARQUET_EXT)
      categories.registerTempTable("categories")
      var customers: DataFrame = sqlContext.read.parquet(DATAWAREHOUSE+"customers"+PARQUET_EXT)
      customers.registerTempTable("customers")
      var order_items: DataFrame = sqlContext.read.parquet(DATAWAREHOUSE+"order_items"+PARQUET_EXT)
      order_items.registerTempTable("order_items")
      var orders: DataFrame = sqlContext.read.parquet(DATAWAREHOUSE+"orders"+PARQUET_EXT)
      orders.registerTempTable("orders")
      var products: DataFrame = sqlContext.read.parquet(DATAWAREHOUSE+"products"+PARQUET_EXT)
      products.registerTempTable("products")

      var cites: DataFrame = sqlContext.read.parquet(DATAMARK+"cites"+PARQUET_EXT)
      cites.registerTempTable("cites")

      val nodoHechos = sqlContext.sql(
        "select "+
          "(o.order_id) as order_id, "+
          "(c.customer_id) as customer_id, "+
          "(ci.city_id) as city_id, "+
          "(p.product_id) as product_id, "+
          "(ca.category_id) as category_id, "+
          "(d.department_id) as department_id, "+
          "(o.order_date) as order_date, "+
          "sum(oi.order_item_subtotal) as Val_Ventas, "+
          "sum(oi.order_item_quantity) as Cant_Prod "+
          "from "+
          "order_items oi, "+
          "orders o, "+
          "customers c, "+
          "products p, "+
          "categories ca, "+
          "departments d, "+
          "cites ci "+
          "where o.order_id = oi.order_item_order_id and "+
          "o.order_customer_id = c.customer_id and "+
          "c.customer_city = ci.city_name and "+
          "c.customer_state = ci.city_state and "+
          "oi.order_item_product_id = p.product_id and "+
          "ca.category_id = p.product_category_id and "+
          "ca.category_department_id = d.department_id "+
          "Group by "+
          "o.order_id, "+
          "c.customer_id, "+
          "ci.city_id, "+
          "p.product_id, "+
          "ca.category_id, "+
          "d.department_id, "+
          "o.order_date" )
        .withColumn("load_date", current_date())
      //nodoHechos.show(100)
      var urlD: String = DATAMARK+"hechos"+PARQUET_EXT
      nodoHechos.write.mode("overwrite").format("parquet").save(urlD)
    }
  }

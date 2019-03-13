# Data warehouse en Spark

Apartir de la base de datos retail_db contenida en mysql la cual accesamos por medio de una maquina virtual de cloudera crear un data warehouse en spark:

## 1. Proceso de cargar de tablas desde Mysql a archivos parquet en Hadoop

### 1.1 Conectando Apache-Spark a Mysql
```scala
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
var url="jdbc:mysql://quickstart:3306/retail_db"
val pro= new java.util.Properties
pro.setProperty("user","root")
pro.setProperty("password","cloudera")
```
### 1.2 Lectura de tablas de MySql desde Apache-Spark y escritura de archivos parquet en Hadoop
```scala
val departmentdf = sqlContext.read.jdbc(url,"departments",pro)
departmentdf.write.mode("overwrite").format("parquet").save("hdfs:///datawh/department.parquet")

val categoriesdf = sqlContext.read.jdbc(url,"categories",pro)
categoriesdf.write.mode("overwrite").format("parquet").save("hdfs:///datawh/categories.parquet")

val customersdf = sqlContext.read.jdbc(url,"customers",pro)
customersdf.write.mode("overwrite").format("parquet").save("hdfs:///datawh/customers.parquet")

val order_itemsdf = sqlContext.read.jdbc(url,"order_items",pro)
order_itemsdf.write.mode("overwrite").format("parquet").save("hdfs:///datawh/order_items.parquet")

val ordersdf = sqlContext.read.jdbc(url,"orders",pro)
ordersdf.write.mode("overwrite").format("parquet").save("hdfs:///datawh/orders.parquet")

val productsdf = sqlContext.read.jdbc(url,"products",pro)
productsdf.write.mode("overwrite").format("parquet").save("hdfs:///datawh/products.parquet")

```

### 1.3 Lectura de los archivos *.parquet 

```scala
val parquetdepar = sqlContext.read.parquet("/datawh/department.parquet")
val parquetcliente = sqlContext.read.parquet("/datawh/customers.parquet")
val parquetcategoria = sqlContext.read.parquet("/datawh/categories.parquet")
val parquetproducto = sqlContext.read.parquet("/datawh/products.parquet")
val parquetorder_items = sqlContext.read.parquet("/datawh/order_items.parquet")
val parquetorders = sqlContext.read.parquet("/datawh/orders.parquet")
```

### 1.4 Lectura y registro de tabla temporal a partir de un dataframe

```scala
parquetdepar.registerTempTable("departamento")
parquetcliente.registerTempTable("cliente")
parquetproducto.registerTempTable("producto")
parquetcategoria.registerTempTable("categoria")
parquetorder_items.registerTempTable("ordenItems")
parquetorders.registerTempTable("ordenes")

```

## 2. Modelo Entidad Relación de la base de datos Mysql

![alt text](recursos/ModeloER.png "Modelo-ER Retail_db")

## 3. Arquitectura a utilizar

![alt text](recursos/Arquitectura.png "Aquitectura Big Data")


## 4. Modelo estrella 

Creacion de modelo en estrella utilizando Apache-Spark y MySql.

* La tabla de hechos la tabla de hechos tiene la cantidad de productos vendidos, el valor de las ventas y fecha de carga de los registros. Las dimensiones del esquema en estrella deben ser las siguientes: 
* Cliente: Esta dimensión debe tener una columna que almacena el correo electrónico del cliente. La estructura de la columna es fname. lname@upb.{city}.com. Adicionalmente construya una columna de password cuyo valor es un número aleatorio entre 0 y 100. 
* Estado del pago
* Categoría 
* Producto
* Departamento
* Ciudad
* Tiempo: Esta dimensión debe tener una columna que muestre el dıa de la semana, el d ́ıa del mes, el mes, el trimestre y el sementre en que se realizó la compra. 

### 4.1 Modelo Estrella Apache-Spark

![alt text](recursos/Diagrama_Estrella.png "Modelo de Estrella")

## 5. Realizar la conexión de Mysql a Spark para crear las tablas en *.parquet.

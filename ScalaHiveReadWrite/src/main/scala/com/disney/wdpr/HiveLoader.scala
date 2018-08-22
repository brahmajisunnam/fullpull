package com.disney.wdpr

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._


class HiveLoader(tableName:String) {


	def tryLoading: Unit = {

		//-----------------Static values ----
		val tableRowKeyColumnName:String = "rowkey"
		val jobDTSColumn:String = "job_dts"
		val targetTable:String = s"ia_simba_rms.${tableName}"
		val sourceTable:String  = s"ia_simba_rms_raw.${tableName}"

		//-----------------------------------

		//Initialization of Spark Context
		val spark = SparkSession.builder()
			.master("local[*]")
			.appName("HiveQueryExecutor")
			.enableHiveSupport()
			.getOrCreate();

		val sc = spark.sparkContext

		val sqlContext = spark.sqlContext
		import sqlContext.implicits._
		sc.setLogLevel("ERROR");

		//Find the max value of dc_dts column in target.
		val maxJobDTSTarget = sqlContext.sql(s"""select coalesce(max(${jobDTSColumn}) ,'"1900/01/01 00:00:00"') as  max_dts from ia_simba_rms.alloc_detail""")
			.select("max_dts").collectAsList()(0)(0).toString()

		//source DataFrame for DeltaRows
		val sourceDF = spark.table(sourceTable)
		val targetDF = spark.table(targetTable)
		val deltaSQL:String  =buildDeltaSQL(sourceTable,sourceDF,targetDF,maxJobDTSTarget)

		val sourceDeltaRowsDF = sqlContext.sql(deltaSQL)

		sourceDeltaRowsDF.createOrReplaceTempView("source_delta_rows")
		targetDF.createOrReplaceTempView("target_full_rows")

		val joinQuery = s" SELECT  TGT.* FROM target_full_rows AS TGT inner join source_delta_rows on " + buildJoinCondition(getPKColumns(targetDF.schema))
		val dfTargetChanged = sqlContext.sql(joinQuery)
		val df_full_set = sourceDeltaRowsDF.union(dfTargetChanged)
		val pkColumns = getPKColumns(targetDF.schema).map(_.name).toSeq

		val window = Window.partitionBy(pkColumns.mkString(",")).orderBy("vrsn_strt_dts")

		val dfVersionedSet = df_full_set.withColumn("vrsn_end_dts", lead($"vrsn_strt_dts", 1, "9999-12-31 00:00:00.000").over(window))
		dfVersionedSet.write.insertInto(targetTable)

	}

	//Get list of PK columns based on position of vrsn_strt_dts
	def getPKColumns( tableFields: StructType):List[StructField] = {
		val columnList = tableFields.toList
		columnList.slice(1,columnList.map(_.name).indexOf("targetTable"))
	}

	def buildJoinCondition(pkColumnList:List[StructField]): String = {

		val stringBuilder = new StringBuffer()
		val pkColumnSize = pkColumnList.size();
		var columnCount  = 0

		for (col <- pkColumnList) {

			columnCount+=1

			if (columnCount< pkColumnSize){
				stringBuilder.append(s" target_full_rows.${col.name} = source_delta_rows.${col.name} AND" )
			}else{
				stringBuilder.append(s" target_full_rows.${col.name} = source_delta_rows.${col.name}" )
			}
		}

		stringBuilder.toString()
	}


	def getPKColumnWithCast(pkColumnList:List[StructField]):String = {

		val sql = new StringBuilder()

		sql.append( "\t"+ "concat_ws('-'"+"\n")
		pkColumnList.map(col => {
			sql.append("\t" +"\t" + s",cast(raw_data.message_value.${col.name} as varchar(${col.dataType.defaultSize * 3}))" + "\n")
		})
		sql.append("\t" +"\t" +",date_format(raw_data.message_value.dc_dts, 'yyyyMMddHHmmssS')")
		sql.append("\t" +") as rowkey" + "\n")

		sql.toString()
	}


	def buildDeltaSQL(tableName:String,sourceDF:sql.DataFrame,targetDF:sql.DataFrame,maxJobDTS:String ):String = {

		val sqlQuery = new StringBuilder()
		sqlQuery.append("SELECT " + "\n")
		val pkColumns = getPKColumns(targetDF.schema)
		sqlQuery.append(getPKColumnWithCast(pkColumns))

		var coulmnCounter = 0
		sourceDF.select("raw_data.message_value.*").schema.fields.foreach( field => {

			coulmnCounter+=1

			if (coulmnCounter == pkColumns.size){
				sqlQuery.append("\t" +"\t" +",raw_data.message_value.dc_dts as vrsn_strt_dts" +"\n")
				sqlQuery.append("\t" +"\t" +",cast('9999-12-31 00:00:00' as timestamp) as vrsn_end_dts" +"\n")
			}
			sqlQuery.append("\t" +"\t" +s",raw_data.message_value.${field.name}" +"\n")
		})

		sqlQuery.append("\t" +"\t" +s",raw_data.etl_metadata.source_schema" +"\n")
		sqlQuery.append("\t" +"\t" +s",raw_data.etl_metadata.source_table" +"\n")
		sqlQuery.append("\t" +"\t" +s",raw_data.etl_metadata.job_name" +"\n")
		sqlQuery.append("\t" +"\t" +s",raw_data.etl_metadata.job_id" +"\n")
		sqlQuery.append("\t" +"\t" +s",raw_data.etl_metadata.job_dts" +"\n")
		sqlQuery.append(s"FROM ${tableName}")
		sqlQuery.append(s" where raw_data.etl_metadata.job_dts > ${maxJobDTS}".replace("\"","'")  )

		sqlQuery.toString()
	}
}




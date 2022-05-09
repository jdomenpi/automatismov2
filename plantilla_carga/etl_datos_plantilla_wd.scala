// scalastyle:off


package com.santander.rrhh

import com.santander.common.TemplateRRHH
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{reverse, _}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession, _}


object etl_datos_pklowercase_wd extends TemplateRRHH {


  def main(args: Array[String]): Unit = {

    val className = this.getClass.getCanonicalName
    println(s" Running $className")
    super.main(args, this)
  }

  def apply(spark: SparkSession, args: Map[String, String]): Unit = {


    val SAVEMODE = SaveMode.Overwrite
    val format = "parquet"
    val dataDatePart = args("PARAM_FECHA_DATA_DATE")
    val HDFS_LANDING_WD_XML = args("HDFS_LANDING_WD_XML")
    val PARAM_BU_WORKDAY = args("PARAM_BU_WORKDAY")    
    val table = ".datos_pklowercase"
    val tableHist = ".datos_consolid_hist"
    val ruta = s"${HDFS_LANDING_WD_XML}weci/data_date_part=$dataDatePart/*/*.xml"
    
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.sql.crossJoin.enabled", "true")



    df_union.write.format(format).mode(SAVEMODE).insertInto(PARAM_BU_WORKDAY.concat(table))

  }
}

// scalastyle:on
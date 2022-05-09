#! /bin/ksh

export DIR_HOME=${param.RUTA_BUSINESS_WD}
. /$DIR_HOME/DM_RRHH_ETL_WD.cfg
cd $DIR_HOME

#ruta pro
export RUTA_LANDING_WD=${param.HDFS_LANDING_WD_XML}

. ${param.RUTA_BUSINESS_WD}/DM_RRHH_ETL_WD.cfg
#kinit
kinit ${PARAM_USUARIO}@${KERB_REALM} -k -t ${RUTA_KEYTAB};

export JAVA_HOME=${JAVA_HOME}
export SPARK_HOME=${SPARK_HOME}

export PROPERTIES_DATOS_PKUPERCASE_WD=datos_pklowercase_wd.prop
export PROPERTIES=DM_RRHH_ETL_WD.prop
properties_lectura=$(cat $PROPERTIES)
export properties_wd="$properties_lectura
PARAM_FECHA_DATA_DATE=$1"


echo "$properties_wd" > $PROPERTIES_DATOS_PKUPERCASE_WD

hdfs dfs -put -f $PROPERTIES_DATOS_PKUPERCASE_WD $PROPERTIES_DATOS_PKUPERCASE_WD

export EXEC=${EXEC}
export CORES=${CORES}
export DRIVER=${DRIVER}
export MEM=${MEM}
export BROADCAST=${BROADCAST}
export OVERHEAD=${OVERHEAD}

export BBDD=${param.PARAM_BU_WORKDAY}
export NODO=${impala.nodo}
export PORT=${impala.port}

export CLASE=etl_datos_pklowercase_wd

table=datos_pklowercase

spark2-submit --master yarn --deploy-mode cluster --num-executors $EXEC --executor-memory $MEM --executor-cores $CORES --driver-memory $DRIVER --conf spark.yarn.executor.memoryOverhead=$OVERHEAD \
--conf spark.sql.autoBroadcastJoinThreshold=$BROADCAST \
--class com.santander.rrhh.$CLASE  ${param.RUTA_BUSINESS_WD}/rrhh-spark-shade.jar  $PROPERTIES_DATOS_PKUPERCASE_WD > salida_$CLASE.log 2>error_$CLASE.log;

RESULT=$?

if [ ${RESULT} -ne 0 ]; then
  echo echo "Error en la llamada al script de hive job_datos_pklowercase_wd.sh: $RESULT"
  i=1
  until [ $i -gt 2 ]
  do

    spark2-submit --master yarn --deploy-mode cluster --num-executors $EXEC --executor-memory $MEM --executor-cores $CORES --driver-memory $DRIVER --conf spark.yarn.executor.memoryOverhead=$OVERHEAD \
          --conf spark.sql.autoBroadcastJoinThreshold=$BROADCAST \
          --class com.santander.rrhh.$CLASE  ${param.RUTA_BUSINESS_WD}/rrhh-spark-shade.jar  $PROPERTIES_DATOS_PKUPERCASE_WD > salida_$CLASE.log 2>error_$CLASE.log;

    RESULT=$?

    if [ ${RESULT} -ne 0 ]; then
      echo echo "Error en la llamada al script de hive job_datos_pklowercase_wd.sh: $RESULT"
      ((i=i+1))

    else
      validateTable=$(impala-shell -k -i $NODO:$PORT -d $BBDD -q "SHOW TABLES LIKE '$table'")
      if [[ -z $validateTable ]]; then
        echo "invalidate metadata"
        impala-shell -k -i $NODO:$PORT -q "invalidate metadata $BBDD.$table"
      else
        echo "refresh"
        impala-shell -k -i $NODO:$PORT -q "refresh $BBDD.$table PARTITION (data_date_part = '$1')"
        sleep 60
        impala-shell -k -i $NODO:$PORT -q "COMPUTE INCREMENTAL STATS $BBDD.$table PARTITION (data_date_part = '$1')"
      fi
      exit $RESULT
    fi
  done
else
  validateTable=$(impala-shell -k -i $NODO:$PORT -d $BBDD -q "SHOW TABLES LIKE '$table'")
  if [[ -z $validateTable ]]; then
    echo "invalidate metadata"
    impala-shell -k -i $NODO:$PORT -q "invalidate metadata $BBDD.$table"
  else
    echo "refresh"
    impala-shell -k -i $NODO:$PORT -q "refresh $BBDD.$table PARTITION (data_date_part = '$1')"
    sleep 60
    impala-shell -k -i $NODO:$PORT -q "COMPUTE INCREMENTAL STATS $BBDD.$table PARTITION (data_date_part = '$1')"
  fi
  exit $RESULT
fi

echo echo "Saliendo: $RESULT"
exit $RESULT





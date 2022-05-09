#! /bin/ksh

#. /SWIsban/bankpriv/rrhh/sw/Carga_Business/DM_RRHH_ETL.cfg
. ${param.RUTA_BUSINESS_WD}/DM_RRHH_ETL_WD.cfg

DIR_HOME=${RUTA_EXTRACTOR_WD}
FECHA=$1

#kinit
kinit ${PARAM_USUARIO}@${KERB_REALM} -k -t ${RUTA_KEYTAB};

##### Pone Cabeceras
cat ${RUTA_DESCARGAS_WD}/datos_pklowercase_wd_cab.txt > ${RUTA_DESCARGAS_WD}/datos_pklowercase_weci_wd.csv;

#Extracciones
beeline -u $PARAM_CONEXION_BEELINE -hiveconf PARAM_BU_WORKDAY=$PARAM_BU_WORKDAY -hiveconf FECHA=$FECHA --delimiterForDSV=';' --incremental=true --outputformat=dsv --showHeader=false -f ${DIR_HOME}/datos_extraccion_pklowercase_wd.hql | grep -v "\<0:.*\>"| sed 's/[\t]/;/g' >> ${RUTA_DESCARGAS_WD}/datos_pklowercase_weci_wd.csv 2> ${DIR_HOME}/error_datos_pklowercase_weci_wd.txt;
RESULT=$?
if [ ${RESULT} -ne 0 ]; then
		echo "Error en la extraccion: $RESULT"
		exit $RESULT
fi
#Limpiar caracteres especiales (^M y ") fichero_clean.csv
sed -e 's/\r//g' -e 's/\"//g'  -e 's/\d026//g'  ${RUTA_DESCARGAS_WD}/datos_pklowercase_weci_wd.csv > ${RUTA_DESCARGAS_WD}/datos_pklowercase_weci_wd_clean.csv;

#Eliminar Query antes del encabezado
egrep -v ". .> "  ${RUTA_DESCARGAS_WD}/datos_pklowercase_weci_wd_clean.csv > ${RUTA_DESCARGAS_WD}/datos_pklowercase_weci_wd.csv;
exit $RESULT
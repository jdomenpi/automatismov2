#! /bin/bash

#Variable que indica como de ok termina el script
RESULT=0

#Variable que recibe el nombre para los ficheros
var_nombre_original=$1

#Variable obligada en min
var_nombre=${var_nombre_original,,}

##Convertir lower convertir en min
lower=${var_nombre,,}

##Convertir upper convertir en may
upper=${var_nombre^^}

#Variable que indica los ficheros que se deben crear
echo "Si valor 2 recibe 2=0 --> crea todos los ficheros de la carga y descarga"
echo "Si valor 2 recibe 2=1 --> crea solo los ficheros de la carga"
echo "Si valor 2 recibe 2=2 --> crea solo los ficheros de la descarga"
echo "Si valor 2 recibe 2=3 --> crea solo el fichero .sh de la carga"
echo "Si valor 2 recibe 2=4 --> crea solo el fichero .hql de la carga"
echo "Si valor 2 recibe 2=5 --> crea solo el fichero .scala de la carga"
echo "Si valor 2 recibe 2=6 --> crea solo el fichero .sh de la descarga"
echo "Si valor 2 recibe 2=7 --> crea solo el fichero .hql de la descarga"
echo "Si valor 2 recibe 2=8 --> crea solo el fichero .txt de la descarga"
var_ficheros=$2

##########################################################
##Joan Doemench

#Metodos de automatismo

echo "Iniciamos automatismo"

if [ $var_ficheros -eq 0 ];then

	echo "Creando todos los ficheros de la carga y descarga con parametro 2=$2"
	
	##########################################################
	#Ficheros de la carga

	echo "Creando el fichero job_datos_${var_nombre}_wd.sh"

	cp plantilla_carga/job_datos_plantilla_wd.sh  archivos_salida/job_datos_${var_nombre}_wd.sh

	##Expresion de remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/job_datos_${var_nombre}_wd.sh

	##Expresion de remplazo may

	sed -i 's/PKUPERCASE/'${upper}'/g' archivos_salida/job_datos_${var_nombre}_wd.sh

	echo "Creando el fichero datos_${var_nombre}_wd.hql"

	cp plantilla_carga/datos_plantilla_wd.hql  archivos_salida/datos_${var_nombre}_wd.hql

	##Expresión remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/datos_${var_nombre}_wd.hql

	echo "Creando el fichero etl_datos_${var_nombre}_wd.scala"

	cp plantilla_carga/etl_datos_plantilla_wd.scala  archivos_salida/etl_datos_${var_nombre}_wd.scala

	##Expresión remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/etl_datos_${var_nombre}_wd.scala

	##########################################################
	#Ficheros de la descarga

	echo "Creando el fichero job_extraccion_datos_${var_nombre}_wd.sh"  

	cp plantilla_descarga/job_extraccion_datos_plantilla_wd.sh  archivos_salida/job_extraccion_datos_${var_nombre}_wd.sh

	##Expresion remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/job_extraccion_datos_${var_nombre}_wd.sh

	echo "Creando el fichero datos_extraccion_${var_nombre}_wd.hql" 

	cp plantilla_descarga/datos_extraccion_plantilla_wd.hql  archivos_salida/datos_extraccion_${var_nombre}_wd.hql

	##Expression remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/datos_extraccion_${var_nombre}_wd.hql

	echo "Creando el fichero datos_${var_nombre}_wd_cab.txt"

	cp plantilla_descarga/datos_plantilla_wd_cab.txt  archivos_salida/datos_${var_nombre}_wd_cab.txt

	
elif [ $var_ficheros -eq 1 ];then

	echo "Creando solo los ficheros de la carga con parametro 2=$2"
		
	##########################################################
	#Ficheros de la carga

	echo "Creando el fichero job_datos_${var_nombre}_wd.sh"

	cp plantilla_carga/job_datos_plantilla_wd.sh  archivos_salida/job_datos_${var_nombre}_wd.sh

	##Expresion de remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/job_datos_${var_nombre}_wd.sh

	##Expresion de remplazo may

	sed -i 's/PKUPERCASE/'${upper}'/g' archivos_salida/job_datos_${var_nombre}_wd.sh

	echo "Creando el fichero datos_${var_nombre}_wd.hql"

	cp plantilla_carga/datos_plantilla_wd.hql  archivos_salida/datos_${var_nombre}_wd.hql

	##Expresión remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/datos_${var_nombre}_wd.hql

	echo "Creando el fichero etl_datos_${var_nombre}_wd.scala"

	cp plantilla_carga/etl_datos_plantilla_wd.scala  archivos_salida/etl_datos_${var_nombre}_wd.scala

	##Expresión remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/etl_datos_${var_nombre}_wd.scala

		
elif [ $var_ficheros -eq 2 ];then

	echo "Creando solo los ficheros de la descarga con parametro 2=$2"
	
	##########################################################
	#Ficheros de la descarga

	echo "Creando el fichero job_extraccion_datos_${var_nombre}_wd.sh"

	cp plantilla_descarga/job_extraccion_datos_plantilla_wd.sh  archivos_salida/job_extraccion_datos_${var_nombre}_wd.sh

	##Expresion remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/job_extraccion_datos_${var_nombre}_wd.sh

	echo "Creando el fichero datos_extraccion_${var_nombre}_wd.hql"

	cp plantilla_descarga/datos_extraccion_plantilla_wd.hql  archivos_salida/datos_extraccion_${var_nombre}_wd.hql

	
	##Expression remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/datos_extraccion_${var_nombre}_wd.hql

	echo "Creando el fichero datos_${var_nombre}_wd_cab.txt"

	cp plantilla_descarga/datos_plantilla_wd_cab.txt  archivos_salida/datos_${var_nombre}_wd_cab.txt
	
	
elif [ $var_ficheros -eq 3 ];then

	echo "Creando solo el fichero .sh de la carga con parametro 2=$2"
	
	##########################################################
	#Ficheros de la carga
	
	echo "Creando el fichero job_datos_${var_nombre}_wd.sh"

	cp plantilla_carga/job_datos_plantilla_wd.sh  archivos_salida/job_datos_${var_nombre}_wd.sh

	##Expresion de remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/job_datos_${var_nombre}_wd.sh

	##Expresion de remplazo may

	sed -i 's/PKUPERCASE/'${upper}'/g' archivos_salida/job_datos_${var_nombre}_wd.sh

	
elif [ $var_ficheros -eq 4 ];then

	echo "Creando solo el fichero .hql de la carga con parametro 2=$2"
		
	##########################################################
	#Ficheros de la carga
	
	echo "Creando el fichero datos_${var_nombre}_wd.hql"

	cp plantilla_carga/datos_plantilla_wd.hql  archivos_salida/datos_${var_nombre}_wd.hql

	##Expresión remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/datos_${var_nombre}_wd.hql
	
elif [ $var_ficheros -eq 5 ];then

	echo "Creando solo el fichero .scala de la carga con parametro 2=$2"
		
	##########################################################
	#Ficheros de la carga
	
	echo "Creando el fichero etl_datos_${var_nombre}_wd.scala"

	cp plantilla_carga/etl_datos_plantilla_wd.scala  archivos_salida/etl_datos_${var_nombre}_wd.scala

	##Expresión remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/etl_datos_${var_nombre}_wd.scala

	
elif [ $var_ficheros -eq 6 ];then

	echo "Creando solo el fichero .sh de la descarga con parametro 2=$2"
		
	##########################################################
	#Ficheros de la descarga

	echo "Creando el fichero job_extraccion_datos_${var_nombre}_wd.sh"

	cp plantilla_descarga/job_extraccion_datos_plantilla_wd.sh  archivos_salida/job_extraccion_datos_${var_nombre}_wd.sh

	##Expresion remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/job_extraccion_datos_${var_nombre}_wd.sh
	
elif [ $var_ficheros -eq 7 ];then

	echo "Creando solo el fichero .hql de la descarga con parametro 2=$2"
		
	##########################################################
	#Ficheros de la descarga

	echo "Creando el fichero datos_extraccion_${var_nombre}_wd.hql"

	cp plantilla_descarga/datos_extraccion_plantilla_wd.hql  archivos_salida/datos_extraccion_${var_nombre}_wd.hql

	
	##Expression remplazo min

	sed -i 's/pklowercase/'${lower}'/g' archivos_salida/datos_extraccion_${var_nombre}_wd.hql
	
elif [ $var_ficheros -eq 8 ];then

	echo "Creando solo el fichero .txt de la descarga con parametro 2=$2"
		
	##########################################################
	#Ficheros de la descarga

	echo "Creando el fichero datos_${var_nombre}_wd_cab.txt"

	cp plantilla_descarga/datos_plantilla_wd_cab.txt  archivos_salida/datos_${var_nombre}_wd_cab.txt

else
	echo "Problema detectado en la creación de los ficheros"
	$RESULT=1
	exit $RESULT
fi


##########################################################
#Salida

echo echo "Saliendo: $RESULT"
exit $RESULT



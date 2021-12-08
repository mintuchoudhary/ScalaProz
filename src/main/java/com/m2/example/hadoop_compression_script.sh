TIMESTAMP=$(date +'%Y%m%d_%H%M%S')

LOG_FILE=hadoop_compress_utility_$TIMESTAMP.log
COMPONENT=HADOOP-COMPRESS-APP


if (( $# < 1 || $# > 2));
then
        echo "USAGE MANDATORY: hadoop_compression_script.sh <HDFSFILEPATH> [optional:COMPRESSIONTYPE:gzip/none]" >> $LOG_FILE 2>&1
        exit 1
fi

HDFS_PATH=$1
COMPRESSION_TYPE=$2


HADOOP_JAR=$(ls /user/home/mintu/lib/*hadoop-compression-utility-*.jar | tail -1)
 
echo "Start of CCO_TRADE_INGESTION_SH" >> $LOG_FILE 2>&1

/usr/bin/spark2-submit \
 --files=${HIVE_SITE},${LOG_FILE} \
 --jars=${VCLIB_SPARK2_EXTRA} \
 --conf "spark.executor.extraJavaOptions=-DCOMPONENT=$COMPONENT -DENVIRONMENT=$ENVIRONMENT -DCONNECTION=$CONNECTION -DORC_USER=$ORC_USER -DORC_PASS=$ORC_PASS -DDRIVER=$DRIVER -DSCHM_OWNER=$SCHM_OWNER -DHIVE=$HIVE -Dvolcker.crypto.masterKey.path=$MASTER_KEY_PATH -Xss10240k" \
 --conf "spark.driver.extraJavaOptions=-DCOMPONENT=$COMPONENT -DENVIRONMENT=$ENVIRONMENT -DCONNECTION=$CONNECTION -DORC_USER=$ORC_USER -DORC_PASS=$ORC_PASS -DDRIVER=$DRIVER -DSCHM_OWNER=$SCHM_OWNER -DHIVE=$HIVE -Dvolcker.crypto.masterKey.path=$MASTER_KEY_PATH -Xss10240k" \
 --master yarn \
 --deploy-mode cluster \
 --verbose \
 --class com.volcker.compress.FileToBeCompression ${HADOOP_JAR} $HDFS_PATH $COMPRESSION_TYPE >> $LOG_FILE 2>&1

EXIT_CODE=$?
echo "spark2-submit exit code: ${EXIT_CODE}"

exit $EXIT_CODE

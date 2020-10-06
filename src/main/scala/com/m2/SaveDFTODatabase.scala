package com.db

import org.apache.spark.sql.SaveMode

object SaveDFTODatabase {
/*
  finalDF.write.mode(SaveMode.Append).jdbc(dbConfig.url, SemConstants.MIS_VRI_BCF, daoConfigToConnProp(dbConfig))
  finalDF.write.format("jdbc")
    .option("url",dbConfig.url)
    .option("driver",dbConfig.driver)
    .option("dbtable",SemConstants.MIS_VRI_BCF)
    .option("user",dbConfig.user)
    .option("password", if (dbConfig.decrypt) getEncrypter.decrypt(dbConfig.pwd) else dbConfig.pwd)
    .mode(SaveMode.Overwrite)
    .save()*/
}

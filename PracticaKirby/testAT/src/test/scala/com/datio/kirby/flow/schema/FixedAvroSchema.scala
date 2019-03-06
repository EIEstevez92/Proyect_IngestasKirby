package com.datio.kirby.flow.schema

import org.apache.spark.sql.types.{StructField, _}

trait FixedAvroSchema {
  val fixedAvroSchema = StructType(
    Seq(
      StructField("cod_entalfa", StringType, nullable = true),
      StructField("cod_paisoalf", StringType, nullable = true),
      StructField("cod_idcontra", StringType, nullable = true),
      StructField("fec_cierre", StringType, nullable = true),
      StructField("cod_pgofipro", StringType, nullable = true),
      StructField("cod_pgbancom", StringType, nullable = true),
      StructField("cod_banorign", StringType, nullable = true),
      StructField("cod_canal_dv", StringType, nullable = true),
      StructField("fec_altacto", StringType, nullable = true),
      StructField("cod_pgccontr", StringType, nullable = true),
      StructField("fec_vencto", StringType, nullable = true),
      StructField("fec_original", StringType, nullable = true),
      StructField("fec_cancel", StringType, nullable = true),
      StructField("cod_ofiorigi", StringType, nullable = true),
      StructField("cod_pgoficom", StringType, nullable = true),
      StructField("cod_diisoalf", StringType, nullable = true),
      StructField("cod_pro_plat", StringType, nullable = true),
      StructField("cod_tipopd", StringType, nullable = true),
      StructField("cod_situcto", StringType, nullable = true),
      StructField("cod_apliremi", StringType, nullable = true),
      StructField("cod_apli_pro", StringType, nullable = true),
      StructField("cod_matrusua", StringType, nullable = true),
      StructField("cod_motcance", StringType, nullable = true),
      StructField("cod_sprodcom", StringType, nullable = true),
      StructField("cod_idprodto", StringType, nullable = true),
      StructField("cod_ofertac", StringType, nullable = true),
      StructField("cod_comprod", StringType, nullable = true),
      StructField("cod_servcrdr", IntegerType, nullable = true),
      StructField("cod_prodfin", IntegerType, nullable = true),
      StructField("cod_sqregist", StringType, nullable = true),
      StructField("cod_sec_int", IntegerType, nullable = true),
      StructField("aud_tim", StringType, nullable = true),
      StructField("cod_prov_apl", StringType, nullable = true),
      StructField("cod_sec_int_size_1", IntegerType, nullable = true)
    )
  )
}
package com.datio.kirby.input

import org.apache.spark.sql.types._

/**
  * Created by antoniomartin on 06/10/16.
  */
object Schemas {

  private val originName = "originName"
  private val logicalFormat = "logicalFormat"
  val schemaMae = StructType(Array(
    StructField("cod_paisoalf", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_paisoalf").build())
    ,StructField("cod_entalfa", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_entalfa").build())
    ,StructField("cod_idcontra", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_idcontra").build())
    ,StructField("cod_pgofipro", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgofipro").build())
    ,StructField("cod_paisfol", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_paisfol").build())
    ,StructField("cod_pgbancsb", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgbancsb").build())
    ,StructField("cod_pgcofici", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgcofici").build())
    ,StructField("cod_pgccontr", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgccontr").build())
    ,StructField("cod_folio", DoubleType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_folio").build()), //DecimalType
    StructField("cod_ctaextno", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_ctaextno").build())
    ,StructField("cod_idprodto", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_idprodto").build())
    ,StructField("cod_prodfin", IntegerType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_prodfin").build())
    ,StructField("cod_situcto", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_situcto").build())
    ,StructField("cod_situmora", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_situmora").build())
    ,StructField("cod_situdw", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_situdw").build())
    ,StructField("cod_diisoalf", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_diisoalf").build())
    ,StructField("cod_idiomiso", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_idiomiso").build())
    ,StructField("xti_clasecto", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_clasecto").build())
    ,StructField("qnu_firmas", IntegerType, nullable = true, new MetadataBuilder().
      putString(originName, "qnu_firmas").build())
    ,StructField("xti_admonpg", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_admonpg").build())
    ,StructField("xti_multidiv", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_multidiv").build())
    ,StructField("xti_bloqueo", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_bloqueo").build())
    ,StructField("cod_canal_dv", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_canal_dv").build())
    ,StructField("cod_matricu", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_matricu").build())
    ,StructField("cod_pgpaisco", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgpaisco").build())
    ,StructField("cod_pgbancom", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgbancom").build())
    ,StructField("cod_pgoficom", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgoficom").build())
    ,StructField("cod_pgofible", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgofible").build())
    ,StructField("fec_altapro", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_altapro").build())
    ,StructField("fec_autopro", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_autopro").build())
    ,StructField("fec_imprpro", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_imprpro").build())
    ,StructField("fec_altacto", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_altacto").build())
    ,StructField("fec_vigorcto", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_vigorcto").build())
    ,StructField("fec_ultmod", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_ultmod").build())
    ,StructField("fec_vencto", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_vencto").build())
    ,StructField("fec_cancel", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_cancel").build())
    ,StructField("cod_tipocor", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_tipocor").build())
    ,StructField("cod_nivconfi", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_nivconfi").build())
    ,StructField("xti_tituliza", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_tituliza").build())
    ,StructField("aud_usuario", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "aud_usuario").build())
    ,StructField("aud_timfmod", TimestampType, nullable = true, new MetadataBuilder().
      putString(originName, "aud_timfmod").build())
    ,StructField("cod_pgdestin", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgdestin").build())
    ,StructField("cod_pgsubdes", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgsubdes").build())
    ,StructField("cod_garcoble", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_garcoble").build())
    ,StructField("cod_admonges", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_admonges").build())
    ,StructField("cod_medioen", IntegerType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_medioen").build())
    ,StructField("cod_ccolect", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_ccolect").build())
    ,StructField("cod_paisaud", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_paisaud").build())
    ,StructField("cod_entiaud", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_entiaud").build())
    ,StructField("cod_oficaud", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_oficaud").build())
    ,StructField("cod_medio_dv", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_medio_dv").build())
    ,StructField("fec_admonpg", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_admonpg").build())
    ,StructField("hms_admonpg", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "hms_admonpg").build())
    ,StructField("xti_pdte_reg", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_pdte_reg").build())
    ,StructField("xti_epa", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_epa").build())))

  val schemaGood = StructType(Array(
    StructField("cod_paisoalf", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_paisoalf").build())
    ,StructField("cod_entalfa", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_entalfa").build())
    ,StructField("cod_idcontra", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_idcontra").build())
    ,StructField("cod_pgofipro", DoubleType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgofipro").build())
    ,StructField("cod_paisfol", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_paisfol").build())
    ,StructField("cod_pgbancsb", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgbancsb").build())
    ,StructField("cod_pgcofici", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgcofici").build())
    ,StructField("cod_pgccontr", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgccontr").build())
    ,StructField("cod_folio", DoubleType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_folio").build()), //DecimalType
    StructField("cod_ctaextno", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_ctaextno").build())
    ,StructField("cod_idprodto", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_idprodto").build())
    ,StructField("cod_prodfin", IntegerType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_prodfin").build())
    ,StructField("cod_situcto", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_situcto").build())
    ,StructField("cod_situmora", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_situmora").build())
    ,StructField("cod_situdw", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_situdw").build())
    ,StructField("cod_diisoalf", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_diisoalf").build())
    ,StructField("cod_idiomiso", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_idiomiso").build())
    ,StructField("xti_clasecto", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_clasecto").build())
    ,StructField("qnu_firmas", IntegerType, nullable = true, new MetadataBuilder().
      putString(originName, "qnu_firmas").build())
    ,StructField("xti_admonpg", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_admonpg").build())
    ,StructField("xti_multidiv", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_multidiv").build())
    ,StructField("xti_bloqueo", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_bloqueo").build())
    ,StructField("cod_canal_dv", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_canal_dv").build())
    ,StructField("cod_matricu", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_matricu").build())
    ,StructField("cod_pgpaisco", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgpaisco").build())
    ,StructField("cod_pgbancom", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgbancom").build())
    ,StructField("cod_pgoficom", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgoficom").build())
    ,StructField("cod_pgofible", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgofible").build())
    ,StructField("fec_altapro", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_altapro").build())
    ,StructField("fec_autopro", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_autopro").build())
    ,StructField("fec_imprpro", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_imprpro").build())
    ,StructField("fec_altacto", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_altacto").build())
    ,StructField("fec_vigorcto", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_vigorcto").build())
    ,StructField("fec_ultmod", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_ultmod").build())
    ,StructField("fec_vencto", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_vencto").build())
    ,StructField("fec_cancel", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_cancel").build())
    ,StructField("cod_tipocor", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_tipocor").build())
    ,StructField("cod_nivconfi", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_nivconfi").build())
    ,StructField("xti_tituliza", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_tituliza").build())
    ,StructField("aud_usuario", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "aud_usuario").build())
    ,StructField("aud_timfmod", TimestampType, nullable = true, new MetadataBuilder().
      putString(originName, "aud_timfmod").build())
    ,StructField("cod_pgdestin", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgdestin").build())
    ,StructField("cod_pgsubdes", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_pgsubdes").build())
    ,StructField("cod_garcoble", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_garcoble").build())
    ,StructField("cod_admonges", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_admonges").build())
    ,StructField("cod_medioen", IntegerType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_medioen").build())
    ,StructField("cod_ccolect", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_ccolect").build())
    ,StructField("cod_paisaud", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_paisaud").build())
    ,StructField("cod_entiaud", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_entiaud").build())
    ,StructField("cod_oficaud", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_oficaud").build())
    ,StructField("cod_medio_dv", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "cod_medio_dv").build())
    ,StructField("fec_admonpg", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "fec_admonpg").build())
    ,StructField("hms_admonpg", TimestampType, nullable = true, new MetadataBuilder().
      putString(originName, "hms_admonpg").build())
    ,StructField("xti_pdte_reg", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_pdte_reg").build())
    ,StructField("xti_epa", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "xti_epa").build())))

  val schemaBad = StructType(Array(
    StructField("cod_paisoalf", DoubleType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_paisoalf").build())
    ,StructField("cod_entalfa", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_entalfa").build())
    ,StructField("cod_idcontra", DoubleType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_idcontra").build())
    ,StructField("cod_pgofipro", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_pgofipro").build())
    ,StructField("cod_paisfol", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_paisfol").build())
    ,StructField("cod_pgbancsb", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_pgbancsb").build())
    ,StructField("cod_pgcofici", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_pgcofici").build())
    ,StructField("cod_pgccontr", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_pgccontr").build())
    ,StructField("cod_folio", DoubleType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_folio").build())
    ,StructField("cod_ctaextno", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_ctaextno").build())
    ,StructField("cod_idprodto", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_idprodto").build())
    ,StructField("cod_prodfin", IntegerType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_prodfin").build())
    ,StructField("cod_situcto", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_situcto").build())
    ,StructField("cod_situmora", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_situmora").build())
    ,StructField("cod_situdw", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_situdw").build())
    ,StructField("cod_diisoalf", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_diisoalf").build())
    ,StructField("cod_idiomiso", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_idiomiso").build())
    ,StructField("xti_clasecto", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "xti_clasecto").build())
    ,StructField("qnu_firmas", IntegerType, nullable = false, new MetadataBuilder().
      putString(originName, "qnu_firmas").build())
    ,StructField("xti_admonpg", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "xti_admonpg").build())
    ,StructField("xti_multidiv", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "xti_multidiv").build())
    ,StructField("xti_bloqueo", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "xti_bloqueo").build())
    ,StructField("cod_canal_dv", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_canal_dv").build())
    ,StructField("cod_matricu", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_matricu").build())
    ,StructField("cod_pgpaisco", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_pgpaisco").build())
    ,StructField("cod_pgbancom", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_pgbancom").build())
    ,StructField("cod_pgoficom", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_pgoficom").build())
    ,StructField("cod_pgofible", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_pgofible").build())
    ,StructField("fec_altapro", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "fec_altapro").build())
    ,StructField("fec_autopro", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "fec_autopro").build())
    ,StructField("fec_imprpro", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "fec_imprpro").build())
    ,StructField("fec_altacto", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "fec_altacto").build())
    ,StructField("fec_vigorcto", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "fec_vigorcto").build())
    ,StructField("fec_ultmod", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "fec_ultmod").build())
    ,StructField("fec_vencto", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "fec_vencto").build())
    ,StructField("fec_cancel", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "fec_cancel").build())
    ,StructField("cod_tipocor", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_tipocor").build())
    ,StructField("cod_nivconfi", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_nivconfi").build())
    ,StructField("xti_tituliza", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "xti_tituliza").build())
    ,StructField("aud_usuario", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "aud_usuario").build())
    ,StructField("aud_timfmod", TimestampType, nullable = false, new MetadataBuilder().
      putString(originName, "aud_timfmod").build())
    ,StructField("cod_pgdestin", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_pgdestin").build())
    ,StructField("cod_pgsubdes", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_pgsubdes").build())
    ,StructField("cod_garcoble", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_garcoble").build())
    ,StructField("cod_admonges", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_admonges").build())
    ,StructField("cod_medioen", IntegerType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_medioen").build())
    ,StructField("cod_ccolect", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_ccolect").build())
    ,StructField("cod_paisaud", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_paisaud").build())
    ,StructField("cod_entiaud", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "cod_entiaud").build())
    ,StructField("xti_pdte_reg", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "xti_pdte_reg").build())
    ,StructField("xti_epa", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "xti_epa").build())))

  val schemaWithDistribution = StructType(Array(
    StructField("cod_paisoalf", StringType, nullable = true, new MetadataBuilder().
      putString(logicalFormat, "ALPHANUMERIC(1)").putString(originName, "cod_paisoalf").build())
    ,StructField("cod_entalfa", StringType, nullable = true, new MetadataBuilder().
      putString(logicalFormat, "ALPHANUMERIC(3)").putString(originName, "cod_entalfa").build())
    ,StructField("cod_idcontra", StringType, nullable = true, new MetadataBuilder().
      putString(logicalFormat, "ALPHANUMERIC(5)").putString(originName, "cod_idcontra").build())
    ,StructField("cod_pgofipro", StringType, nullable = true, new MetadataBuilder().
      putString(logicalFormat, "ALPHANUMERIC(8)").putString(originName, "cod_pgofipro").build())
    ,StructField("cod_paisfol", StringType, nullable = true, new MetadataBuilder().
      putString(logicalFormat, "ALPHANUMERIC(12)").putString(originName, "cod_paisfol").build())
    ,StructField("cod_pgbancsb", StringType, nullable = true, new MetadataBuilder().
      putString(logicalFormat, "ALPHANUMERIC(21)").putString(originName, "cod_pgbancsb").build())
    ,StructField("cod_pgcofici", StringType, nullable = true, new MetadataBuilder().
      putString(logicalFormat, "ALPHANUMERIC(2)").putString(originName, "cod_pgcofici").build())
    ,StructField("cod_pgccontr", StringType, nullable = true, new MetadataBuilder().
      putString(logicalFormat, "ALPHANUMERIC(4)").putString(originName, "cod_pgccontr").build())
    ,StructField("cod_folio", DoubleType, nullable = true, new MetadataBuilder().
      putString(logicalFormat, "DECIMAL(6)").putString(originName, "cod_folio").build())))

  val schemaWithDecimal = StructType(Array(
    StructField("field1", StringType, nullable = true, new MetadataBuilder().
      putString(logicalFormat, "ALPHANUMERIC(4)").putString(originName, "field1").build())
    ,StructField("field2", StringType, nullable = true, new MetadataBuilder().
      putString(logicalFormat, "ALPHANUMERIC(6)").putString(originName, "field2").build())
    ,StructField("field3", StringType, nullable = true, new MetadataBuilder().
      putString(logicalFormat, "ALPHANUMERIC(10)").putString(originName, "field3").build())))

  val schemaParquet = StructType(Array(
    StructField("name", StringType, nullable = true, new MetadataBuilder().putString(originName, "name").build())
    ,StructField("age", IntegerType, nullable = true, new MetadataBuilder().putString(originName, "age").build())))

  val schemaParquetNonNullable = StructType(Array(
    StructField("name", StringType, nullable = false),
    StructField("age", IntegerType, nullable = false)))

  val schemaParquetBad = StructType(Array(
    StructField("name", IntegerType, nullable = true, new MetadataBuilder().putString(originName, "name").build())
    ,StructField("age", StringType, nullable = true, new MetadataBuilder().putString(originName, "age").build())))

  val schemaBooksGood = StructType(Array(
    StructField("author", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "author").build())
    ,StructField("title", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "title").build())
    ,StructField("genre", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "genre").build())
    ,StructField("price", DoubleType, nullable = true, new MetadataBuilder().
      putString(originName, "price").build())
    ,StructField("publish_date", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "publish_date").build())
    ,StructField("description", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "description").build())))

  val schemaInstrumentsGood = StructType(Array(
    StructField("instrument_id", IntegerType, nullable = true, new MetadataBuilder().
      putString(originName, "instrument_id").build())
    ,StructField("rating_attribute_type_code", IntegerType, nullable = true, new MetadataBuilder().
      putString(originName, "rating_attribute_type_code").build())
    ,StructField("rating_attribute_type_text", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "rating_attribute_type_text").build())
    ,StructField("moodys_rating_id", IntegerType, nullable = true, new MetadataBuilder().
      putString(originName, "moodys_rating_id").build())
    ,StructField("effective_date", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "effective_date").build())
    ,StructField("termination_date", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "termination_date").build())
    ,StructField("rating_attribute_code", IntegerType, nullable = true, new MetadataBuilder().
    putString(originName, "rating_attribute_code").build())
    ,StructField("rating_attribute_text", StringType, nullable = true, new MetadataBuilder().
    putString(originName, "rating_attribute_text").build())))

  val schemaXmlAcceptanceInputGood = StructType(Array(
    StructField("Organization_ID", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_ID").build())
    ,StructField("Organization_CFG_Indicator", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_CFG_Indicator").build())
    ,StructField("Organization_PFG_Indicator", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_PFG_Indicator").build())
    ,StructField("Organization_SFG_Indicator", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_SFG_Indicator").build())
    ,StructField("Issuer_CFG_Indicator", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Issuer_CFG_Indicator").build())
    ,StructField("Issuer_PFG_Indicator", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Issuer_PFG_Indicator").build())
    ,StructField("Issuer_SFG_Indicator", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Issuer_SFG_Indicator").build())
    ,StructField("Issuer_Indicator", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Issuer_Indicator").build())
    ,StructField("Organization_Type_Code", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Type_Code").build())
    ,StructField("Organization_Type_Code", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Type_Code").build())
    ,StructField("Organization_Type_Text", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Type_Text").build())
    ,StructField("County", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "County").build())
    ,StructField("County_Name", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "County_Name").build())
    ,StructField("State", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "State").build())
    ,StructField("Moodys_Legal_Name", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Moodys_Legal_Name").build())
    ,StructField("Deal_ID", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Deal_ID").build())
    ,StructField("Organization_Currency_Code", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Currency_Code").build())
    ,StructField("Organization_Currency_Name", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Currency_Name").build())
    ,StructField("Organization_Currency_Short_Description", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Currency_Short_Description").build())
    ,StructField("Organization_Currency_ISO_Code", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Currency_ISO_Code").build())
    ,StructField("Product_Line_Description", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Product_Line_Description").build())
    ,StructField("Domicile_Country_Code", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Domicile_Country_Code").build())
    ,StructField("Domicile_Country_Name", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Domicile_Country_Name").build())
    ,StructField("Organization_Parent_Number", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Parent_Number").build())
    ,StructField("Organization_Parent_Name", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Parent_Name").build())
    ,StructField("Organization_Ultimate_Parent_Number", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Ultimate_Parent_Number").build())
    ,StructField("Organization_Ultimate_Parent_Name", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Ultimate_Parent_Name").build())
    ,StructField("Organization_Ratings", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Ratings").build())
    ,StructField("Organization_Identifiers", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Identifiers").build())
    ,StructField("Organization_Attributes", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Attributes").build())
    ,StructField("Organization_Market", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Market").build())
    ,StructField("Organization_Aliass", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "Organization_Aliass").build())
  ))


}
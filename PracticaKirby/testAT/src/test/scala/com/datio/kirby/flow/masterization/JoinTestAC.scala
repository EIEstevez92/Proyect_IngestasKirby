package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object JoinTestAC extends FunSuite with Matchers {

  def check(dfResult: DataFrame): Unit = {

    assert(dfResult.columns.toSet === Set("field1PK", "field2PK", "field3", "t1_field1PK", "t1_field2PK"))
    assert(dfResult.count === 11)
  }

  def check2(dfResult: DataFrame): Unit = {

    assert(dfResult.columns.toSet === Set("field1PK","field2PK","field3","field4","date","baddate","t1_field1PK","t1_field2PK","t1_field3","t1_field4","t1_date","t1_baddate"))
    assert(dfResult.count === 10)
  }

  def check3(dfResult: DataFrame): Unit = {

    assert(dfResult.columns.toSet === Set("field1PK", "field2PK", "field3", "t1_field1PK", "t1_field2PK", "t2_field1PK", "t2_field2PK"))
    assert(dfResult.count === 1)
  }

  def check4(dfResult: DataFrame): Unit = {

    assert(dfResult.columns.toSet === Set("cod_geve_trn", "xti_orig_rui", "cod_dependac", "cod_detalcto", "cod_canal_dv", "cod_folioct2", "cod_anno_ej", "cod_folioctt", "cod_user", "cod_pais_ic2", "cod_pers_c2", "cod_refaplic", "cod_id_rop", "cod_dependa2", "cod_paisej", "cod_pan", "cod_subtrn", "cod_ofialfa2", "cod_detalct2", "cod_tiptrn", "cod_ofictom", "cod_pgccontr", "cod_persctpn", "cod_pais_ica", "qnu_detareme", "cod_enta_ict", "cod_pers_ide", "cod_tip_ref", "cod_idcontra", "cod_fun_neg", "cod_serv_dv", "cod_eve_trn", "cod_tpiclori", "cod_pers_tar", "cod_pgccont2", "cod_det_ope1", "fec_soli_trn", "cod_sec_int", "cod_st_operc", "hms_soli_trn", "cod_retortr", "cod_entalf1", "cod_enta_ctr", "cod_pais_trs", "cod_ofialfac", "cod_pais_ict", "cod_deve_trn", "cod_enta_tar", "cod_idcontr2", "cod_medio_dv", "aud_tim", "cod_enta_trs", "cod_sop_fis", "cod_pais_ctr", "cod_cargousu", "cod_erravi", "cod_diisoalf", "cod_pais_c1", "cod_enta_ica", "cod_idrecori", "cod_tip_nat", "cod_documps", "cod_errrop", "fec_ini_proc", "cod_paisoalf", "cod_ofictrn", "cod_trnfims", "cod_enta_c2", "cod_enta_ic2", "cod_entalfa", "cod_pais_ide", "cod_mes_ej", "cod_idextno", "cod_tipident", "cod_contorig", "cod_num_trn", "cod_enta_per", "cod_entalf2", "imp_trans", "cod_pers_ctr", "cod_pers_trs", "cod_comercc", "cod_severr", "cod_pais_c2", "cod_pers_c1", "cod_pais_per", "cod_pmit_trn", "cod_idcontrt", "fec_cierre", "cod_id_rel", "cod_pais_tar", "cod_apli_trn", "cod_npuesto", "cod_entalfat", "month", "cod_vertrn", "cod_enta_c1", "cod_det_ope2", "cod_enta_ide"))
    assert(dfResult.count === 900)
  }


}

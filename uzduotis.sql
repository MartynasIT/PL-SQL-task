create or replace package apps.uzduotis is

/*
  Globalios paprogramës
*/
procedure get_default_values(
  p_return_status out varchar2,
  p_return_msg out varchar2,
  p_laikotarpis out varchar2,
  p_laikotarpis_user out varchar2);

procedure main(
  p_req_status out varchar2,
  p_req_msg out varchar2,
  p_slaptumo_zyma in varchar2,
  p_laikotarpis in varchar2,
  p_padalinys_kc in varchar2,
  p_drg_id in number);

end ATS919$;
/

create or replace package body apps.uzduotis is
/*
  Iðimtys
*/
/*
  Tipai
*/
/*
  Konstantos
*/
PACKAGE_NAME constant varchar2(30) := 'ATS919$';
/*
  Globalûs kintamieji
*/
/*
  Iðankstiniai paprogramiø prototipai
*/
/*
  Paprogramës
*/
procedure get_default_values(
  p_return_status out varchar2,
  p_return_msg out varchar2,
  p_laikotarpis out varchar2,
  p_laikotarpis_user out varchar2) is

  ROUTINE constant varchar2(65) := PACKAGE_NAME||'.GET_DEFAULT_VALUES';
  l_kal_rec dk_apskaitos_kalendorius%rowtype := dk005_utl$.get_prev_apk_rec(sysdate);
begin
  p_return_status := cdf_api_utl$.RETURN_STATUS_SUCCESS;
  p_return_msg := '';
  
  if l_kal_rec.apk_id is not null then
    p_laikotarpis := l_kal_rec.laikotarpis;
    p_laikotarpis_user := l_kal_rec.laikotarpis;
  end if;

exception
  when cdf_error_utl$.e_internal_exception then
    cdf_error_utl$.raise_error;
  when others then
    cdf_error_utl$.output_unexp_exception(
      p_routine => ROUTINE);
end;

procedure gather_data(
  p_laikotarpis in varchar2,
  p_padalinys_kc in varchar2,
  p_drg_id in number) is

  ROUTINE constant varchar2(65) := PACKAGE_NAME||'.GATHER_DATA';
  l_apk_rec dk_apskaitos_kalendorius%rowtype := dk005_utl$.get_apk_rec_by_laik(p_laikotarpis);
  l_laikotarpis_prev dk_apskaitos_kalendorius.laikotarpis%type := to_char(add_months(to_date(p_laikotarpis, 'YYYY-MM'), -1), 'YYYY-MM');
  l_apk_rec_prev dk_apskaitos_kalendorius%rowtype := dk005_utl$.get_apk_rec_by_laik(l_laikotarpis_prev);
begin
  cdf_output_utl$.log('- gather_data...');

  insert into ats919_report_data(
    level_nr,
    drg_id,
    maa_grupe,
    padalinys_kc,
    maa_apsk_spec)
  select                                -- Atranka 1.	MAA grupës
    1,    
    drg.drg_id,
    drg.gr_pavadinimas,
    dfv_pad.value_text padalinys_kc,
    per.pavarde_vardas
  from
    turt_darb_grupes drg,
    bndr_padal_dfv dfv_pad,
    per_asmenys per
  where
    (drg.drg_id = p_drg_id or
    p_drg_id is null) and
    per.asm_id (+) = drg.drg_asm_id_mat_atsak and
    (drg.padalinys = p_padalinys_kc or
    p_padalinys_kc is null) and
    drg.rusis = 'MAT_ATSAK_VIDINE' and
    drg.slaptumo_lygis = cdf_context$.slaptumo_lygis and
    dfv_pad.value_key(+) = drg.padalinys;
    
  insert into ats919_report_data(
    level_nr,
    padalinys_kc,
    maa_grupe,
    operacijos_nr,
    buh_sask_ervis,
    buh_sask_stekas,
    tipas,
    rinkmenos_timestamp,
    opr_busena,
    likutis_pradziai,
    pajamos_per_laik,
    islaidos_per_laik,
    likutis_pabaigai,
    pastabos,
    maa_apsk_spec)
  select                        -- Atranka 2.	Ataskaitinio laikotarpio 6-KAM formø teikimo operacijos
    2 level_nr,
    red.padalinys_kc,
    red.maa_grupe,
    dot.opr_nr operacijos_nr,
    dte.buh_sask_ervis,
    dte.buh_sask_stekas,
    vsv_tip.value_text tipas,
    to_char(atc.creation_date, 'YYYY-MM-DD HH24:MI:SS') rinkmenos_timestamp,
    dfv_bus.busena_vart_dsp opr_busena,
    dte.laik_likutis_pradz_suma likutis_pradziai,
    dte.laik_pajamos_suma pajamos_per_laik,
    dte.laik_islaidos_suma islaidos_per_laik,
    dte.laik_likutis_pab_suma likutis_pabaigai,
    dte.pastabos,
    red.maa_apsk_spec
  from
    ats919_report_data red,
    turt_dok_teikimai dot,
    turt_dok_teik_eilutes dte,
    cdf_vset_values_v vsv_tip,
    cdf_dok_attachments atc,
    bndr_vart_busenos_dfv dfv_bus
  where
    red.level_nr = 1 and
    dot.dot_drg_id = red.drg_id and
    dot.busenos_kodas <> 'AN' and
    dot.laikotarpis = p_laikotarpis and
    dte.dte_dot_id = dot.dot_id and
    vsv_tip.value_set_code = 'TURT052_EIL_TIPAS' and
    vsv_tip.value_key = dte.tipas and
    atc.source_table(+) = 'TURT_DOK_TEIK_EILUTES' and
    atc.source_id(+) = dte.dte_id and
    dfv_bus.busena_vart = dot.busenos_kodas_vart;
    
  insert into ats919_report_data(
    level_nr,
    padalinys_kc,
    maa_grupe,
    buh_sask_ervis,
    buh_sask_stekas,
    tipas,
    likutis_pabaigai_pr_laik,
    maa_apsk_spec)
  select                      -- Atranka 3.	Ankstesnio laikotarpio 6-KAM formø teikimo operacijos
    3 level_nr,
    red.padalinys_kc,
    red.maa_grupe,
    dte.buh_sask_ervis,
    dte.buh_sask_stekas,
    vsv_tip.value_text tipas,
    dte.laik_likutis_pab_suma likutis_pabaigai_pr_laik,
    red.maa_apsk_spec
  from
    ats919_report_data red,
    turt_dok_teikimai dot,
    turt_dok_teik_eilutes dte,
    cdf_vset_values_v vsv_tip
  where
    red.level_nr = 1 and
    dot.dot_drg_id = red.drg_id and
    dot.busenos_kodas <> 'AN' and
    dot.laikotarpis = l_laikotarpis_prev and
    dte.dte_dot_id = dot.dot_id and
    vsv_tip.value_set_code = 'TURT052_EIL_TIPAS' and
    vsv_tip.value_key = dte.tipas;
    
  insert into ats919_report_data(
    level_nr,
    padalinys_kc,
    maa_grupe,
    buh_sask_ervis,
    buh_sask_stekas,
    tipas, 
    ikelimas_atask_men,
    maa_apsk_spec)
  select                  -- Atranka 4.	Ataskaitinio laikotarpio detaliø likuèiø ákëlimo operacijos
    3 level_nr,
    red.padalinys_kc,
    red.maa_grupe,
    dfv_stk.ervis_buh_saskaita,
    del.buh_sask_kodas buh_sask_stekas,
    decode(dle.ar_kelti_ervis_flg,
      'Y', 'eRVIS duomenys',
      'Iðoriniai duomenys') tipas,
    sum(dle.suma) ikelimas_atask_men,
    red.maa_apsk_spec
  from
    ats919_report_data red,
    turt_det_likuciai del,
    turt_det_likuciu_eil dle,
    bndr_buh_sask_stekas_dfv dfv_stk
  where
    red.level_nr = 1 and
    del.del_drg_id = red.drg_id and
    del.busenos_kodas_sist = 'PAT' and
    del.likuciu_data between l_apk_rec.galioja_nuo and l_apk_rec.galioja_iki and
    dle.dle_del_id = del.del_id and
    dfv_stk.value_key = del.buh_sask_kodas
  group by
    red.maa_grupe,
    del.buh_sask_kodas,
    dfv_stk.ervis_buh_saskaita,
    dle.ar_kelti_ervis_flg,
    red.padalinys_kc,
    red.maa_apsk_spec;
    
  insert into ats919_report_data(
    level_nr,
    padalinys_kc,
    maa_grupe,
    buh_sask_ervis,
    buh_sask_stekas,
    tipas, 
    ikelimas_praeit_men,
    maa_apsk_spec)
  select                         -- Atranka 5.	Ankstesnio laikotarpio detaliø likuèiø ákëlimo operacijos
    3 level_nr,
    red.padalinys_kc,
    red.maa_grupe,
    dfv_stk.ervis_buh_saskaita,
    del.buh_sask_kodas buh_sask_stekas,
    decode(dle.ar_kelti_ervis_flg,
      'Y', 'eRVIS duomenys',
      'Iðoriniai duomenys') tipas,
    sum(dle.suma) ikelimas_praeit_men,
    red.maa_apsk_spec
  from
    ats919_report_data red,
    turt_det_likuciai del,
    turt_det_likuciu_eil dle,
    bndr_buh_sask_stekas_dfv dfv_stk
  where
    red.level_nr = 1 and
    del.del_drg_id = red.drg_id and 
    del.busenos_kodas_sist = 'PAT' and
    del.likuciu_data between l_apk_rec_prev.galioja_nuo and l_apk_rec_prev.galioja_iki and
    dle.dle_del_id = del.del_id and
    dfv_stk.value_key = del.buh_sask_kodas
  group by
    red.maa_grupe,
    del.buh_sask_kodas,
    dfv_stk.ervis_buh_saskaita,
    dle.ar_kelti_ervis_flg,
    red.padalinys_kc,
    red.maa_apsk_spec;
    
  insert into ats919_report_data(
    level_nr,
    padalinys_kc,
    maa_grupe,
    buh_sask_ervis,
    buh_sask_stekas,
    tipas,
    opr_kiekis_per_laik,
    maa_apsk_spec)
  select                              -- Atranka 6.	Ataskaitinio laikotarpio turto operacijos
    4 level_nr,
    red.padalinys_kc,
    red.maa_grupe,
    opr.buh_sask buh_sask_ervis,
    null buh_sask_stekas,
    'eRVIS duomenys' tipas,
    count(opr.opr_id),
    red.maa_apsk_spec
  from
    ats919_report_data red,
    turt_operacijos opr
  where
    red.level_nr = 1 and
    (opr.opr_drg_id = red.drg_id or
    opr.opr_drg_id_priimanti = red.drg_id) and
    opr.apsk_data between l_apk_rec.galioja_nuo and l_apk_rec.galioja_iki and
    opr.busenos_kodas = 'PAT' and
    (opr.opr_grupe = 'PAJAMAVIMAS' or
    exists(
      select
        1
      from
        dk_apskaitos_eilutes ape
      where
        ape.saltinis = 'TURT_OPR' and
        ape.saltinio_id = opr.opr_id) or
    opr.opr_drg_id <> opr.opr_drg_id_priimanti)
  group by
    red.maa_grupe,
    opr.buh_sask,
    red.padalinys_kc,
    red.maa_apsk_spec;
    
  update ats919_report_data red                                  -- Eiluèiø grupavimas
  set
    red.group_columns_data = 
      nvl(red.maa_grupe, cdf_api_utl$.miss_char) ||
      nvl(red.buh_sask_stekas, cdf_api_utl$.miss_char) || 
      nvl(red.buh_sask_ervis, cdf_api_utl$.miss_char) || 
      nvl(red.tipas, cdf_api_utl$.miss_char) ||
      nvl(red.padalinys_kc, cdf_api_utl$.miss_char) ||
      nvl(red.maa_apsk_spec, cdf_api_utl$.miss_char)
  where
    level_nr in (
      2,
      3,
      4);
      
  declare                         
    cursor c_red is
    select
      5 level_nr,
      row_number() over (
        order by 
          '1') group_id,        
      red.padalinys_kc,
      red.maa_grupe,
      red.buh_sask_ervis,
      red.buh_sask_stekas,
      red.tipas,
      sum(red.opr_kiekis_per_laik) opr_kiekis_per_laik,
      sum(red.likutis_pabaigai_pr_laik) likutis_pabaigai_pr_laik,
      sum(red.ikelimas_praeit_men) ikelimas_praeit_men,
      sum(red.ikelimas_atask_men) ikelimas_atask_men,
      sum(red.likutis_pradziai) likutis_pradziai,
      sum(red.pajamos_per_laik) pajamos_per_laik,
      sum(red.islaidos_per_laik) islaidos_per_laik,
      sum(red.likutis_pabaigai) likutis_pabaigai,
      red.group_columns_data,
      red.maa_apsk_spec
    from
      ats919_report_data red
    where
      level_nr in (
        2,
        3,
        4)
    group by
      red.maa_grupe,
      red.buh_sask_stekas,
      red.buh_sask_ervis,
      red.tipas,
      red.padalinys_kc,
      red.group_columns_data,
      red.maa_apsk_spec;
    
    type t_red_tbl is table of c_red%rowtype index by binary_integer;
    type t_upd_rec is record(
      group_id ats919_report_data.group_id%type,
      group_columns_data ats919_report_data.group_columns_data%type);
    type t_upd_tbl is table  of t_upd_rec index by binary_integer;
    
    l_red_tbl t_red_tbl;
    l_upd_tbl t_upd_tbl;
  begin
    open c_red;
    fetch c_red bulk collect into l_red_tbl;
    close c_red;
    
    forall i in l_red_tbl.first .. l_red_tbl.last
    insert into ats919_report_data(
      level_nr,
      padalinys_kc,
      maa_grupe,
      buh_sask_ervis,
      buh_sask_stekas,
      tipas,
      opr_kiekis_per_laik,
      likutis_pabaigai_pr_laik,
      ikelimas_praeit_men,
      ikelimas_atask_men,
      likutis_pradziai,
      pajamos_per_laik,
      islaidos_per_laik,
      likutis_pabaigai,
      group_columns_data,
      group_id,
      maa_apsk_spec)
    values(
      l_red_tbl(i).level_nr,
      l_red_tbl(i).padalinys_kc,
      l_red_tbl(i).maa_grupe,
      l_red_tbl(i).buh_sask_ervis,
      l_red_tbl(i).buh_sask_stekas,
      l_red_tbl(i).tipas,
      l_red_tbl(i).opr_kiekis_per_laik,
      l_red_tbl(i).likutis_pabaigai_pr_laik,
      l_red_tbl(i).ikelimas_praeit_men,
      l_red_tbl(i).ikelimas_atask_men,
      l_red_tbl(i).likutis_pradziai,
      l_red_tbl(i).pajamos_per_laik,
      l_red_tbl(i).islaidos_per_laik,
      l_red_tbl(i).likutis_pabaigai,
      l_red_tbl(i).group_columns_data,
      l_red_tbl(i).group_id,
      l_red_tbl(i).maa_apsk_spec)
    returning 
      group_id,
      group_columns_data
    bulk collect into
      l_upd_tbl;
    
    forall i in l_upd_tbl.first .. l_upd_tbl.last
    update ats919_report_data red
    set
      group_id = l_upd_tbl(i).group_id
    where
      level_nr in (
        2,
        3,
        4) and 
      red.group_columns_data = l_upd_tbl(i).group_columns_data;
  end;
  
  merge into ats919_report_data red      -- Priskiriami 2 atrankos (6KAM) nesugrupuoti (varchar2) laukai
  using (
    select
      distinct 
      red_grp.group_id,
      red_grp.operacijos_nr,
      red_grp.rinkmenos_timestamp,
      red_grp.opr_busena,
      red_grp.pastabos
    from
      ats919_report_data red_grp
    where
      red_grp.level_nr = 2) red_sub
  on (
    red.group_id = red_sub.group_id)
  when matched then
    update
    set
      red.operacijos_nr = red_sub.operacijos_nr,
      red.rinkmenos_timestamp = red_sub.rinkmenos_timestamp,
      red.opr_busena = red_sub.opr_busena,
      red.pastabos = red_sub.pastabos
    where
      red.level_nr = 5;
      
/*
  Priskiriamas 6 atrankos laukas 'operacijø kiekis per laikotarpá' visoms eilutëms pagal buh_sask_ervis
  (buh_sask_stekas yra null), jei yra kelios, toms eilutëms priskiriama ta pati reikðmë
*/  
  merge into ats919_report_data red
  using (
    select 
      distinct 
      red_grp.opr_kiekis_per_laik,
      red_grp.maa_grupe,
      red_grp.buh_sask_ervis,
      red_grp.tipas,
      red_grp.padalinys_kc,
      red_grp.maa_apsk_spec
    from
      ats919_report_data red_grp
    where
      red_grp.level_nr = 4) red_sub
  on (
    red.maa_grupe = red_sub.maa_grupe and
    red.buh_sask_ervis = red_sub.buh_sask_ervis and
    red.tipas = red_sub.tipas and
    red.padalinys_kc = red_sub.padalinys_kc and
    nvl(red.maa_apsk_spec, cdf_api_utl$.miss_num) = nvl(red_sub.maa_apsk_spec, cdf_api_utl$.miss_num))
  when matched then
    update
    set
      red.opr_kiekis_per_laik = red_sub.opr_kiekis_per_laik
    where
      red.level_nr = 5;

/*
  Jei 6 atrankos eilutës yra priskiriamos kaþkurioms 6KAM arba importo eilutëms pagal buh_sask_ervis,
  ðiø eiluèiø atskirai neturi rodyti. Jei 6KAM arba importo eiluèiø nebuvo atrinkta, 6 atrankos eilutës
  atvaizduojamos ataskaitoje su uþpildyta buh_sask_ervis ir neuþpildyta buh_sask_stekas sàskaitomis
*/
  delete ats919_report_data red
  where
    red.level_nr = 5 and
    red.buh_sask_stekas is null and
    exists (
      select
        1
      from
        ats919_report_data red_sub
      where
        red_sub.maa_grupe = red.maa_grupe and
        red_sub.buh_sask_ervis = red.buh_sask_ervis and
        red_sub.tipas = red.tipas and
        red_sub.padalinys_kc = red.padalinys_kc and
        red_sub.level_nr in (
          2,
          3)
    );

exception
  when cdf_error_utl$.e_internal_exception then
    cdf_error_utl$.raise_error;
  when others then
    cdf_error_utl$.output_unexp_exception(
      p_routine => ROUTINE);
end;

/*
  Formuoja XML bylà
*/
procedure create_xml( 
  p_laikotarpis in varchar2,
  p_slaptumo_zyma in number) is
  
  ROUTINE constant varchar2(65) := PACKAGE_NAME||'.CREATE_XML';
  
begin
  cdf_output_utl$.log('Pildome XML bylà...');
  cdf_report_utl$.open_root_node(PACKAGE_NAME);

  cdf_report_utl$.add_parameter(
    p_name => 'ATASK_LAIKAS',
    p_value => to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS'));

  cdf_report_utl$.add_parameter(
    p_name => 'ATASK_LAIKOTARPIS',
    p_value => p_laikotarpis);
    
  cdf_ats_utl$.export_slaptumo_zyma(p_slaptumo_zyma);

/*
  Ataskaitos detalûs duomenys
*/
  declare
    l_rec_count int;
    l_sql_statement cdf_db_utl$.t_max_plsql_varchar2;
  begin
    l_sql_statement := '
      select
        red.padalinys_kc,
        red.maa_grupe,
        red.maa_apsk_spec,
        red.operacijos_nr,
        red.buh_sask_ervis,
        red.buh_sask_stekas,
        red.tipas,
        red.rinkmenos_timestamp,
        cdf_format_utl$.number_to_excel_number(red.opr_kiekis_per_laik) opr_kiekis_per_laik,
        cdf_format_utl$.number_to_excel_number(red.likutis_pabaigai_pr_laik) likutis_pabaigai_pr_laik,
        cdf_format_utl$.number_to_excel_number(red.ikelimas_praeit_men) ikelimas_praeit_men,
        cdf_format_utl$.number_to_excel_number(red.ikelimas_atask_men) ikelimas_atask_men,
        red.opr_busena,
        cdf_format_utl$.number_to_excel_number(red.likutis_pradziai) likutis_pradziai,
        cdf_format_utl$.number_to_excel_number(red.pajamos_per_laik) pajamos_per_laik,
        cdf_format_utl$.number_to_excel_number(red.islaidos_per_laik) islaidos_per_laik,
        cdf_format_utl$.number_to_excel_number(red.likutis_pabaigai) likutis_pabaigai,
        red.pastabos
      from
        ats919_report_data red
      where
        red.level_nr = 5
      order by
        red.padalinys_kc,
        red.maa_grupe,
        red.buh_sask_stekas,
        red.tipas';

    l_rec_count := cdf_report_utl$.export(
      p_node_name => 'TABLE',
      p_detail_node_name => 'TABLE_ROW',
      p_sql_query => l_sql_statement);
    cdf_db_utl$.hide(l_rec_count);
  end;

  cdf_report_utl$.close_root_node;
  cdf_output_utl$.log('Pildome XML bylà...OK');
exception
  when cdf_error_utl$.e_internal_exception then
    cdf_error_utl$.raise_error;
  when others then
    cdf_error_utl$.output_unexp_exception(
      p_routine => ROUTINE);
end create_xml;

procedure main(
  p_req_status out varchar2,
  p_req_msg out varchar2,
  p_slaptumo_zyma in varchar2,
  p_laikotarpis in varchar2,
  p_padalinys_kc in varchar2,
  p_drg_id in number) is

  ROUTINE constant varchar2(65) := PACKAGE_NAME||'.MAIN';
begin
  cdf_request_utl$.pre_process(PACKAGE_NAME);
  cdf_output_utl$.log('- main...');

  delete ats919_report_data;
  
  gather_data(
    p_laikotarpis => p_laikotarpis,
    p_padalinys_kc => p_padalinys_kc,
    p_drg_id => p_drg_id);

  create_xml(
    p_laikotarpis => p_laikotarpis,
    p_slaptumo_zyma => p_slaptumo_zyma );

  if p_slaptumo_zyma = '20' then
    cdf_request_utl$.assign_report_layout('ATS919_RN');
  else
    cdf_request_utl$.assign_report_layout('ATS919');
  end if;

  cdf_output_utl$.log('- main...OK');
  cdf_request_utl$.post_process();
exception
  when cdf_error_utl$.e_internal_exception then
    p_req_status := cdf_request_utl$.C_REQ_STATUS_ERROR;
    p_req_msg := cdf_error_utl$.get_error_stack_formatted();
  when others then
    p_req_status := cdf_request_utl$.C_REQ_STATUS_ERROR;
    p_req_msg := cdf_error_utl$.get_error_stack_unexp(
      p_routine => ROUTINE);
end main;

end uzduotis$;
/


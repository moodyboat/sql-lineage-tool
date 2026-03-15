-- 贷款发生明细台账
with tt as(
select t.*,z.f_wbbh,
           z.f_wbdm,
           z.f_wbmc,
           to_char(z.f_hl) f_hl,
           z.f_wbbjf,
           z.f_gbszm,
           z.f_bzdw,
           z.f_dyhsbh,
           z.f_hszz,
           h.* from (
select  c.ejdw,cc.cust_code,
  --二级单位
  cc.cust_name,--客户名称
  CASE WHEN jj.cust_loan_subtype = 'PJDK_LN' THEN nvl((SELECT BL.CONT_CODE
                         FROM lc00059999.BL_BILL_CONT BL,  lc00059999.PJKPZB Z,  lc00059999.PJYFKC P
                        WHERE BL.CONT_ID = Z.ACCEPTANCE_CONT
                          AND Z.PJKPZB_NM = P.PJYFKC_KPZBNM
                          AND P.PJYFKC_NM = ht.PRE_BSNS_CODE),nvl(kc.pjyfkc_pjbh,jj.due_bill_code)) ELSE ht.cont_code END cont_code,--合同编号
  nvl(zq.ext_cont_no,' ') ext_cont_no,--展望合同编号
  jj.cust_loan_subtype,--业务子类型
  case when ht.cust_loan_subtype =  'PJDK_LN'then nvl((SELECT BL.amount
                         FROM lc00059999.BL_BILL_CONT BL,  lc00059999.PJKPZB Z,  lc00059999.PJYFKC P
                        WHERE BL.CONT_ID = Z.ACCEPTANCE_CONT
                          AND Z.PJKPZB_NM = P.PJYFKC_KPZBNM
                          AND P.PJYFKC_NM = ht.PRE_BSNS_CODE),PJYFKC_PMJE) else ht.totamt end as totamt ,
  jj.curcd,--币种
   nvl(kc.pjyfkc_pjbh,jj.due_bill_code) as DUE_BILL_CODE,--借据编号
   BK.DUE_BILL_ID,
  (case when ht.cust_loan_subtype in ('PJDK_LN','LN_OTH000222') then PJYFKC_PMJE ELSE jj.loan_amt end)/<!JEDW!> as jjje,--借据金额
  case when jj.grant_date between '<!KSSJ!>' and '<!JSSJ!>' then JJ.LOAN_AMT/<!JEDW!> else 0 end as ffje,--发放金额
  case when jj.grant_date between '<!KSSJ!>' and '<!JSSJ!>' then jj.grant_date ELSE '' end  as grant_date,--发放日期
  to_char(round(jj.intrate,4) ,'90.9999') as intrate,--发放利率
  case when zq.old_cont_end_date between '<!KSSJ!>' and '<!JSSJ!>'then (case when zq.loan_bal=0 then zq.loan_amt else zq.loan_bal end)/<!JEDW!> else null end as zqje,
  --展期金额 待确认,
  case when zq.old_cont_end_date between '<!KSSJ!>' and '<!JSSJ!>' then zq.old_cont_end_date else '' end as old_cont_end_date,--展期日期
  to_char(round(zq.ext_rate,4),'90.9999') as ext_rate,--展期利率
  decode(bk.bzns_type, 'DGXD02', BK.BZNS_AMT, 0)/<!JEDW!> as hkje,--还款金额
  decode(bk.bzns_type, 'DGXD02', BK.BZNS_DATE, '') as hkrq,--还款日期
  JJ.FACT_END_DATE,--到期日期
  (case when zq.ext_cont_no is null 
   then (CASE WHEN ht.cust_loan_subtype = 'PJDK_LN' THEN KC.PJYFKC_DQRQ ELSE JJ.FACT_END_DATE END) 
    ELSE zq.NEW_CONT_END_DATE end) as dqrq,--到期日期
 ((NVL(jj.LOAN_BAL, 0) +
       NVL((SELECT SUM(BZNS_AMT)
            FROM dw_xd_corp_loan_stdbook BOOK
            WHERE BOOK.DUE_BILL_ID = jj.DUE_BILL_ID
                AND BOOK.BZNS_DATE > '<!JSSJ!>'
               AND (BOOK.REVER_STAT = '0' OR BOOK.REVER_STAT IS NULL)
               AND BOOK.BZNS_TYPE = 'DGXD02'),
            0) +        NVL((SELECT SUM(BZNS_AMT) --还款发生冲抹，且在截止之前还款的
             FROM dw_xd_corp_loan_stdbook BOOK
             WHERE BOOK.DUE_BILL_ID = jj.DUE_BILL_ID
                 AND BOOK.BZNS_TYPE = 'DGXD02'
                 AND BOOK.REVER_DATE > BOOK.BZNS_DATE -- 冲抹日期  >  还款业务日期
                 AND BOOK.BZNS_DATE  <= '<!JSSJ!>'   -- 截止日期  >= 还款业务日期
                 AND BOOK.REVER_DATE > '<!JSSJ!>'   -- 冲抹日期  > 截止日期
                 AND BOOK.REVER_TYPE = '2'
                 AND BOOK.REVER_STAT = '2'
                 AND BOOK.BZNS_TYPE = 'DGXD02'), 0)*(-1)
         ) )/<!JEDW!>  LOAN_BAL, --贷款借据余额
  --贷款余额
  (select nvl(sum(bk.bzns_amt), 0)
    from dw_xd_corp_loan_stdbook bk
   where bk.due_bill_id = jj.due_bill_id
     and BK.BZNS_TYPE IN ('DGXD13', 'DGXD27', 'DGXD28', 'DGXD30')
     AND REVER_TYPE is NULL
     and bk.BZNS_DATE between '<!KSSJ!>' and '<!JSSJ!>')/<!JEDW!>  as yzflx,--已支付利息
  (CASE WHEN ht.cust_loan_subtype = 'PJDK_LN' THEN PJYFKC_CPRXHH ELSE NVL((select MAX(cust_settle_actno) from lc00059999.corp_loan_repay_apply clra 
where clra.due_bill_id = jj.due_bill_id 
and bk.bzns_amt = clra.repay_amt),JJ.RECV_ACTNO) END) as grant_settle_actno,--结算账号
  case
                         when jj.grant_date < '<!KSSJ!>'
                           then jj.loan_amt/ <!JEDW!>
                             else
                               0 
                               end 
                             -
                       (nvl((select sum(s.bzns_amt)
                                    from dw_xd_corp_loan_stdbook s
                                   where s.due_bill_id = jj.due_bill_id
                                     and s.bzns_date < '<!KSSJ!>'
                                     and s.bzns_type = 'DGXD02'
                                     and s.rever_type is null),
                                  0) ) / <!JEDW!>  jjqcye,--借据期初余额
                             bk.bzns_type,
                             
       (CASE WHEN ht.cust_loan_subtype = 'PJDK_LN' 
       AND (case when jj.grant_date between '<!KSSJ!>' and '<!JSSJ!>' then jj.grant_date ELSE '' end ) 
       <= (SELECT MAX(BZNS_DATE) FROM dw_xd_corp_loan_stdbook STD 
          WHERE STD.DUE_BILL_ID = BK.DUE_BILL_ID AND BZNS_TYPE IN ('DGXD48','DGXD02'))
       AND (SELECT SUM(DECODE(BZNS_TYPE,'DGXD02',-BZNS_AMT,BZNS_AMT)) JE FROM dw_xd_corp_loan_stdbook STD 
          WHERE STD.DUE_BILL_ID = BK.DUE_BILL_ID AND BZNS_TYPE IN ('DGXD48','DGXD02') AND BZNS_DATE = jj.grant_date) !=0
       THEN ((SELECT SUM(BZNS_AMT) FROM dw_xd_corp_loan_stdbook  STD
            WHERE STD.DUE_BILL_ID = BK.DUE_BILL_ID AND BZNS_TYPE = 'DGXD48'
            AND BZNS_DATE <= '<!JSSJ!>') - NVL((SELECT SUM(BZNS_AMT) FROM dw_xd_corp_loan_stdbook  STD
            WHERE STD.DUE_BILL_ID = BK.DUE_BILL_ID AND BZNS_TYPE = 'DGXD02'
            AND BZNS_DATE <= '<!JSSJ!>'),0))/ <!JEDW!>  ELSE STD.BZNS_AMT/ <!JEDW!> END) YQJE,
         
     (CASE WHEN ht.cust_loan_subtype = 'PJDK_LN' 
       AND (case when jj.grant_date between '<!KSSJ!>' and '<!JSSJ!>' then jj.grant_date ELSE '' end)  
       <= (SELECT MAX(BZNS_DATE) FROM dw_xd_corp_loan_stdbook STD 
          WHERE STD.DUE_BILL_ID = BK.DUE_BILL_ID AND BZNS_TYPE IN ('DGXD48','DGXD02'))
       AND (SELECT SUM(DECODE(BZNS_TYPE,'DGXD02',-BZNS_AMT,BZNS_AMT)) JE FROM dw_xd_corp_loan_stdbook STD 
          WHERE STD.DUE_BILL_ID = BK.DUE_BILL_ID AND BZNS_TYPE IN ('DGXD48','DGXD02') AND BZNS_DATE = jj.grant_date) !=0
       THEN case when jj.grant_date between '<!KSSJ!>' and '<!JSSJ!>' then jj.grant_date ELSE '' end ELSE STD.BZNS_DATE END) YQRQ
from dw_xd_corp_loan_stdbook bk
  join lc00059999.corp_loan_due_bill jj on bk.due_bill_id = jj.due_bill_id and jj.cust_loan_kind in ('LN_ZY','LN_SDL','ICF_FL','ICF_BLN','LN_OTH') 
  left join lc00059999.corp_loan_cont_base ht on  jj.cont_id = ht.cont_id
  left join lc00059999.cust_corp_info cc on bk.cust_code = cc.cust_code
  left join lc00059999.corp_loan_extend_term zq on zq.due_bill_id = jj.due_bill_id
  LEFT JOIN lc00059999.PJYFKC KC ON KC.PJYFKC_NM = ht.PRE_BSNS_CODE
  left join (  
  select b.yjdw as ejdw,cust_code from lc00019999.dwd_cust_corp_info_his b where  b.start_date<=greatest('<!JSSJ!>','20220101') and b.end_date>greatest('<!JSSJ!>','20220101')    
  )c on bk.cust_code=c.cust_code
  LEFT JOIN (
select DUE_BILL_ID,(SELECT DISTINCT BZNS_DATE FROM dw_xd_corp_loan_stdbook WHERE BZNS_TYPE = 'DGXD03' AND DUE_BILL_ID = A.DUE_BILL_ID ) BZNS_DATE, 
   sum(DECODE(BZNS_TYPE,'DGXD02',-BZNS_AMT,BZNS_AMT)) BZNS_AMT
from dw_xd_corp_loan_stdbook A
where DUE_BILL_ID IN (select DUE_BILL_ID from dw_xd_corp_loan_stdbook WHERE REMARK LIKE '%转逾期%' AND BZNS_DATE BETWEEN '<!KSSJ!>' AND '<!JSSJ!>')
and  BZNS_DATE BETWEEN '<!KSSJ!>' AND '<!JSSJ!>'
AND BZNS_TYPE IN ('DGXD02','DGXD03')
AND STDBOOK_SN >= (SELECT MIN(STDBOOK_SN) FROM dw_xd_corp_loan_stdbook WHERE REMARK LIKE '%转逾期%' AND BZNS_DATE BETWEEN '<!KSSJ!>' AND '<!JSSJ!>' AND DUE_BILL_ID =A.DUE_BILL_ID)
group by DUE_BILL_ID
  ) STD 
  ON STD.DUE_BILL_ID = BK.DUE_BILL_ID
where jj.due_bill_id = bk.due_bill_id
  and bk.bzns_type in ('DGXD01','DGXD02','DGXD48') 
  and (nvl(bk.rever_type,'99') not in ('1','2') or (nvl(bk.rever_type,'99') in ('1','2') and bk.rever_stat <> '2') OR (nvl(bk.rever_type,'99') ='2' and bk.rever_stat = '2' AND BK.BZNS_DATE = '<!JSSJ!>'))
  and bk.bzns_date between  '<!KSSJ!>' and '<!JSSJ!>' --业务日期
  and   ( cc.cust_name like '%<!KHMC!>%' or '<!KHMC!>'  is null) --客户名称
  and (ht.cont_code like '%<!HTBH!>%' or '<!HTBH!>'  is null)
  and  bk.bzns_date>='20210101'
  and (zq.apply_date = (select max(apply_date) from lc00059999.corp_loan_extend_term z where z.due_bill_id = bk.due_bill_id) or zq.apply_date is null)  
   union all
 select ejdw,
       cust_code,
       cust_name,
       cont_code,
       ext_cont_no,
       cust_loan_subtype,
       totamt,
       curcd,
       DUE_BILL_CODE,
       DUE_BILL_ID,
       jjje,
       ffje,
       '' as grant_date,
       to_char(round(intrate,4),'90.9999'),
       zqje,
       old_cont_end_date,
       to_char(round(ext_rate,4),'90.9999'),
       hkje,
       hkrq,
       FACT_END_DATE,
       dqrq,
       jjqcye as LOAN_BAL,
       yzflx,
       grant_settle_actno,
       jjqcye,
       '期初' as bzns_type,
       YQJE,
       YQRQ
  from (select c.ejdw,
               cc.cust_code,
               cc.cust_name,
CASE WHEN jj.cust_loan_subtype = 'PJDK_LN' THEN nvl((SELECT BL.CONT_CODE
                         FROM lc00059999.BL_BILL_CONT BL,  lc00059999.PJKPZB Z,  lc00059999.PJYFKC P
                        WHERE BL.CONT_ID = Z.ACCEPTANCE_CONT
                          AND Z.PJKPZB_NM = P.PJYFKC_KPZBNM
                          AND P.PJYFKC_NM = ht.PRE_BSNS_CODE),nvl(kc.pjyfkc_pjbh,jj.due_bill_code)) ELSE ht.cont_code END cont_code,
               nvl(zq.ext_cont_no,' ') ext_cont_no,
               DECODE(ht.CUST_LOAN_KIND,
                      'LN_ZY',
                      '自营',
                      'LN_SDL',
                      '自营',
                      'LN_OTH',
                      '垫款',
                      'ICF_BLN',
                      '买方信贷',
                      'ICF_FL',
                      '保理') CUST_LOAN_KIND,
               jj.cust_loan_subtype,
               case when ht.cust_loan_subtype =  'PJDK_LN'then nvl((SELECT BL.amount
                         FROM lc00059999.BL_BILL_CONT BL,  lc00059999.PJKPZB Z,  lc00059999.PJYFKC P
                        WHERE BL.CONT_ID = Z.ACCEPTANCE_CONT
                          AND Z.PJKPZB_NM = P.PJYFKC_KPZBNM
                          AND P.PJYFKC_NM = ht.PRE_BSNS_CODE),PJYFKC_PMJE) else ht.totamt end as totamt ,
               jj.curcd,
               jj.due_bill_code,
               (case
                       when ht.cust_loan_subtype in ('PJDK_LN', 'LN_OTH000222') then
                        PJYFKC_PMJE
                       ELSE
                        jj.loan_amt
                     end) / <!JEDW!> as jjje,
               (CASE
                 WHEN jj.GRANT_DATE >= '<!KSSJ!>' THEN
                  0
                 ELSE
                  least(NVL((SELECT SUM(S.BZNS_AMT)
                              FROM dw_xd_corp_loan_stdbook S
                             WHERE S.DUE_BILL_ID = jj.DUE_BILL_ID
                               AND S.BZNS_DATE >= '<!KSSJ!>'
                               AND S.BZNS_TYPE IN ('DGXD02', 'DGXD11')
                               AND S.REVER_TYPE IS NULL),
                            0) -
                        nvl((SELECT SUM(S.BZNS_AMT)
                              FROM dw_xd_corp_loan_stdbook S
                             WHERE S.DUE_BILL_ID = jj.DUE_BILL_ID
                               AND s.BZNS_DATE < '<!KSSJ!>'
                               AND s.REVER_DATE >= '<!KSSJ!>'
                               AND S.BZNS_TYPE IN ('DGXD02', 'DGXD11')
                               AND s.REVER_STAT = '2'
                               AND s.REVER_TYPE = '2'),
                            0) + NVL(jj.LOAN_BAL, 0),
                        jj.LOAN_AMT)
               END) / <!JEDW!> jjqcye,
               0 as ffje,
               jj.GRANT_DATE,
               jj.intrate,--发放利率
               case when zq.old_cont_end_date between '<!KSSJ!>' and '<!JSSJ!>'then (case when zq.loan_bal=0 then zq.loan_amt else zq.loan_bal end)/<!JEDW!> else null end as zqje,
  case when zq.old_cont_end_date between '<!KSSJ!>' and '<!JSSJ!>' then zq.old_cont_end_date else '' end as old_cont_end_date,--展期日期
               zq.ext_rate, --展期利率
               0 as hkje,
               '' as hkrq,
               JJ.FACT_END_DATE, --到期日期
               case
                 when zq.ext_cont_no is null then
                  jj.FACT_END_DATE
                 when zq.ext_cont_no is not null then
                  zq.NEW_CONT_END_DATE
               end as dqrq, --到期日期
               ((select nvl(sum(bk.bzns_amt), 0)
                         from dw_xd_corp_loan_stdbook bk
                        where bk.due_bill_id = jj.due_bill_id
                          and BK.BZNS_TYPE IN ('DGXD13', 'DGXD27', 'DGXD28', 'DGXD30')
                          AND REVER_TYPE is NULL
                          and bk.BZNS_DATE between '<!KSSJ!>' and '<!JSSJ!>')) /
                     <!JEDW!> as yzflx, --已支付利息
                jj.recv_actno  as grant_settle_actno,--结算账号
               jj.due_bill_id,
               
               (CASE WHEN ht.cust_loan_subtype = 'PJDK_LN' 
                AND jj.GRANT_DATE 
                <= (SELECT MAX(BZNS_DATE) FROM dw_xd_corp_loan_stdbook STD
                    WHERE STD.DUE_BILL_ID = JJ.DUE_BILL_ID
                      AND BZNS_TYPE IN ('DGXD48','DGXD02'))
                AND (SELECT SUM(DECODE(BZNS_TYPE,'DGXD02',-BZNS_AMT,BZNS_AMT)) JE FROM dw_xd_corp_loan_stdbook STD 
                    WHERE STD.DUE_BILL_ID = JJ.DUE_BILL_ID AND BZNS_TYPE IN ('DGXD48','DGXD02') AND BZNS_DATE = jj.grant_date) !=0 
               THEN ((SELECT SUM(BZNS_AMT) FROM dw_xd_corp_loan_stdbook  STD
            WHERE STD.DUE_BILL_ID = JJ.DUE_BILL_ID AND BZNS_TYPE = 'DGXD48'
            AND BZNS_DATE <= '<!JSSJ!>') - NVL((SELECT SUM(BZNS_AMT) FROM dw_xd_corp_loan_stdbook  STD
            WHERE STD.DUE_BILL_ID = JJ.DUE_BILL_ID AND BZNS_TYPE = 'DGXD02'
            AND BZNS_DATE <= '<!JSSJ!>'),0)) / <!JEDW!> ELSE STD.BZNS_AMT / <!JEDW!> END) YQJE,
                       
                (CASE WHEN ht.cust_loan_subtype = 'PJDK_LN' 
                 AND jj.GRANT_DATE 
                 <= (SELECT MAX(BZNS_DATE) FROM dw_xd_corp_loan_stdbook STD
                     WHERE STD.DUE_BILL_ID = JJ.DUE_BILL_ID
                       AND BZNS_TYPE IN ('DGXD48','DGXD02')) 
                 AND (SELECT SUM(DECODE(BZNS_TYPE,'DGXD02',-BZNS_AMT,BZNS_AMT)) JE FROM dw_xd_corp_loan_stdbook STD 
                     WHERE STD.DUE_BILL_ID = JJ.DUE_BILL_ID AND BZNS_TYPE IN ('DGXD48','DGXD02') AND BZNS_DATE = jj.grant_date) !=0 
                 THEN case when jj.grant_date between '<!KSSJ!>' and '<!JSSJ!>' then jj.grant_date
                 ELSE  ''  end ELSE STD.BZNS_DATE END) YQRQ
          FROM lc00059999.CORP_LOAN_CONT_BASE ht
         INNER JOIN lc00059999.CORP_LOAN_DUE_BILL jj
            ON ht.CONT_ID = jj.CONT_ID
         INNER JOIN lc00059999.CUST_CORP_INFO cc
            ON cc.CUST_CODE = ht.CUST_CODE
          left join (select b.yjdw as ejdw, cust_code
                      from lc00019999.dwd_cust_corp_info_his b
                     where b.start_date <= greatest('<!JSSJ!>','20220101')
                       and b.end_date > greatest('<!JSSJ!>','20220101')) c
            on ht.cust_code = c.cust_code
          left join lc00059999.corp_loan_extend_term zq
            on zq.due_bill_id = jj.due_bill_id
          LEFT JOIN lc00059999.PJYFKC KC
            ON KC.PJYFKC_NM = ht.PRE_BSNS_CODE
          LEFT JOIN (select DUE_BILL_ID,(SELECT DISTINCT BZNS_DATE FROM dw_xd_corp_loan_stdbook WHERE BZNS_TYPE = 'DGXD03' AND DUE_BILL_ID = A.DUE_BILL_ID ) BZNS_DATE, 
   sum(DECODE(BZNS_TYPE,'DGXD02',-BZNS_AMT,BZNS_AMT)) BZNS_AMT
from dw_xd_corp_loan_stdbook A
where DUE_BILL_ID IN (select DUE_BILL_ID from dw_xd_corp_loan_stdbook WHERE REMARK LIKE '%转逾期%' AND BZNS_DATE BETWEEN '<!KSSJ!>' AND '<!JSSJ!>')
and  BZNS_DATE BETWEEN '<!KSSJ!>' AND '<!JSSJ!>'
AND BZNS_TYPE IN ('DGXD02','DGXD03')
AND STDBOOK_SN >= (SELECT MIN(STDBOOK_SN) FROM dw_xd_corp_loan_stdbook WHERE REMARK LIKE '%转逾期%' AND BZNS_DATE BETWEEN '<!KSSJ!>' AND '<!JSSJ!>' AND DUE_BILL_ID =A.DUE_BILL_ID)
group by DUE_BILL_ID) STD
                    ON STD.DUE_BILL_ID = JJ.DUE_BILL_ID
         WHERE jj.BILL_STAT IN ('2', '4', '5')
           AND ht.CUST_LOAN_KIND != 'LN_WD')
 WHERE (jjqcye != 0 OR yzflx != 0)
   AND GRANT_DATE < '<!KSSJ!>'
   and (cust_name like '%<!KHMC!>%' or '<!KHMC!>'  is null) 
     and (cont_code like '%<!HTBH!>%' or '<!HTBH!>'  is null)  
   and due_bill_id not in
       (select due_bill_id
          from dw_xd_corp_loan_stdbook bk
         where bk.bzns_type =  'DGXD02'
           and nvl(bk.rever_type, '99') != '1'
           and bk.bzns_date between '<!KSSJ!>' and '<!JSSJ!>')  
  )t left join lc00059999.LSWBZD z on t.curcd=z.F_WBBH
left join ( select pd.id keyid,(select type_name||'-'||pd.type_name from lc00059999.prd_business_type pe where pe.id=pd.super_id) valuezd from lc00059999.prd_business_type pd where pd.super_id in 

('LN_ZY','LN_SDL','ICF_FL','ICF_BLN','LN_OTH')
  
) h on t.cust_loan_subtype=h.keyid

),
t1 as (
select tt.EJDW                 ,
tt.CUST_CODE            ,
tt.CUST_NAME            ,
tt.CONT_CODE            ,
tt.EXT_CONT_NO          ,
tt.CUST_LOAN_SUBTYPE    ,
to_char(round(tt.TOTAMT/<!JEDW!>,2),'999999999990.99') as totamt               ,
tt.CURCD                ,
tt.DUE_BILL_CODE        ,
TT.DUE_BILL_ID          ,
tt.JJJE                 ,
tt.FFJE                 ,
to_char(to_date(tt.GRANT_DATE,'yyyy/mm/dd') ,'yyyy/mm/dd') GRANT_DATE         ,
tt.INTRATE              ,
tt.ZQJE                 ,
to_char(to_date(tt.OLD_CONT_END_DATE,'yyyy/mm/dd') ,'yyyy/mm/dd')  OLD_CONT_END_DATE   ,
tt.EXT_RATE             ,
tt.HKJE                 ,
to_char(to_date(tt.HKRQ,'yyyy/mm/dd') ,'yyyy/mm/dd') HKRQ                 ,
tt.FACT_END_DATE        ,
to_char(to_date(tt.DQRQ,'yyyy/mm/dd') ,'yyyy/mm/dd') DQRQ                 ,
tt.LOAN_BAL             ,
tt.YZFLX                ,
tt.GRANT_SETTLE_ACTNO   ,
tt.JJQCYE               ,
tt.BZNS_TYPE            ,
tt.F_WBBH               ,
tt.F_WBDM               ,
tt.F_WBMC               ,
tt.F_HL                 ,
tt.F_WBBJF              ,
tt.F_GBSZM              ,
tt.F_BZDW               ,
tt.F_DYHSBH             ,
tt.F_HSZZ               ,
tt.KEYID                ,
tt.VALUEZD              ,
case
  when tt.valuezd like '%银团%' or tt.valuezd like '%自营%'
    then '1'  --自营
  when tt.valuezd like '%买方信贷%'
    then '2'  --买方信贷
  when tt.valuezd LIKE '%垫款%'
    then '3'  --垫款
  else
     '4'
end ywlx,
TT.YQJE,
to_char(to_date(REPLACE(tt.YQRQ,' ',''),'yyyy/mm/dd') ,'yyyy/mm/dd') YQRQ
from tt
   left join dim_cydw_yjdw dd
on  tt.ejdw = dd.YJDWMC


        )
select * from (
select  t1.* from t1
 join (select due_bill_code , sum(hkje) hkje from t1 group by due_bill_code) temp
  on t1.due_bill_code = temp.due_bill_code
    WHERE  
     (BZNS_TYPE not in ( 'DGXD01','DGXD48')
    or
     (
     BZNS_TYPE in ('DGXD01','DGXD48') and temp.hkje = 0
     ))     
union all
select 
'合计'as EJDW, 
'hj001' CUST_CODE, 
' ' CUST_NAME, 
' ' CONT_CODE, 
' ' EXT_CONT_NO, 
' ' CUST_LOAN_SUBTYPE, 
to_char((select sum(totamt) from (select distinct cont_code,totamt from t1 where t1.ywlx = '<!YWLX!>'   or  '<!YWLX!>' is null)),'999999999990.99') as totamt,
' ' CURCD, 
' ' DUE_BILL_CODE,
' ' DUE_BILL_ID,
(select sUM(jjje) from (select distinct due_bill_code,jjje from t1 where t1.ywlx = '<!YWLX!>'   or  '<!YWLX!>' is null)) as jjje,
(select sum(ffje) from (select distinct DUE_BILL_ID,ffje from t1 where t1.ywlx = '<!YWLX!>'   or  '<!YWLX!>' is null)) as ffje,
' ' GRANT_DATE, 
' ' INTRATE, 
(select sum(zqje) from (select distinct EXT_CONT_NO,zqje from t1 where t1.ywlx = '<!YWLX!>'   or  '<!YWLX!>' is null )) as ZQJE, 
' ' OLD_CONT_END_DATE, 
' ' EXT_RATE, 
(select sum(hkje) from t1 where t1.ywlx = '<!YWLX!>'   or  '<!YWLX!>' is null) hkje,
' ' HKRQ, 
' ' FACT_END_DATE, 
' ' DQRQ, 
(select sum(loan_bal) from (select distinct due_bill_code,loan_bal from t1 where t1.ywlx = '<!YWLX!>'   or  '<!YWLX!>' is null)) as loan_bal,
(select sum(yzflx) from (select distinct due_bill_code,yzflx from t1 where t1.ywlx = '<!YWLX!>'   or  '<!YWLX!>' is null)) as yzflx,
' ' GRANT_SETTLE_ACTNO, 
(select sum(jjqcye) from (select distinct due_bill_code,jjqcye from t1 where t1.ywlx = '<!YWLX!>'   or  '<!YWLX!>' is null)) as jjqcye,
' ' BZNS_TYPE, 
' ' F_WBBH, 
' ' F_WBDM, 
' ' F_WBMC, 
' ' F_HL,
' ' F_WBBJF,
' '  F_GBSZM,
' '  F_BZDW,
' '  F_DYHSBH,
' '  F_HSZZ,
' '  KEYID,
' '  VALUEZD,
'0' as YWLX,
(select sum(YQJE) from (select distinct due_bill_code,YQJE from t1 where t1.ywlx = '<!YWLX!>'   or  '<!YWLX!>' is null)) as YQJE,
' '  YQRQ
from 
dual
)

     where  (ywlx = '0'or ywlx = '<!YWLX!>' or '<!YWLX!>' is null )
;
-- 委托贷款资金来源余额表

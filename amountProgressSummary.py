#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/5
# @Author  : maxiaolei
# @Site    : 
# @File    : 资金方进件进度日报——韩璐
import datetime
import pandas as pd
from sqlalchemy import create_engine

from html_template_helper import render_template

con = create_engine('mysql+pymysql://maxiaolei:maxiaolei@123@10.253.169.47:3306/fqmall_ht_prod?charset=utf8')
connFq = con.connect()

def gen_amountProgressSummary_report():

   #直投数据
    amountProgressSummarySql='''
select 
organization.organization ,-- `机构(直投)`,
applyPeriod.applyPeriod,-- 期数,
product.product,-- 产品,
IFNULL(t1.limitAmount,0) limitAmount,-- 额度配置 ,
IFNULL(t2.useAmount,0) useAmount,-- 资金方进件金额,
IFNULL(t2.loanAmount,0) loanAmount,-- 已放款金额,
IFNULL(t2.waitApplyAmount,0) waitApplyAmount,-- 待资金方进件金额,
IFNULL(t2.applyNotLoanAmount,0) applyNotLoanAmount,-- 进件未放款金额,
IFNULL(t3.waittingCheckAmount,0) waittingCheckAmount,-- 待分案金额,
IFNULL(t3.beingCheckAmount,0) beingCheckAmount,-- 审核中金额,
IFNULL(t3.waittingSignAmount,0) waittingSignAmount,-- 待签约金额,
IFNULL(t3.applyFailure,0) applyFailure-- 进件失败金额
from 
(select '汇有财' organization union select '盈衍' union select '笑脸' ) organization
CROSS join 
(select 3 applyPeriod union SELECT 6) applyPeriod
CROSS join 
(select '嗨秒贷' product union SELECT 'VIPD') product
left join 
(select 
ila.inv_code,
case 
when ila.inv_code = 10010 then '汇有财'
when ila.inv_code = 10003 then '盈衍'
when ila.inv_code = 10002 then '笑脸'
end as organization,
ila.period as applyPeriod,
ila.industry_code,
case 
when ila.industry_code='MDCP' then '嗨秒贷'
when ila.industry_code='VIPD' then 'VIPD'
end product,
IFNULL(sum(ila.sb_amount)/10000,0) limitAmount
from inv_limit_amount ila where ila.limit_date= IF(HOUR(NOW())=0,DATE_FORMAT(date_sub(DATE(NOW()),interval 1 day) ,'%%Y-%%m-%%d'),DATE_FORMAT(NOW() ,'%%Y-%%m-%%d'))
GROUP BY  
organization,applyPeriod,product
) t1  on organization.organization=t1.organization and applyPeriod.applyPeriod=t1.applyPeriod and product.product=t1.product
left join 
(SELECT
ibh.inv_username,
case 
when ibh.inv_username = 10010 then '汇有财'
when ibh.inv_username = 10003 then '盈衍'
when ibh.inv_username = 10002 then '笑脸'
end as organization,
ibh.apply_period as applyPeriod,
iap.industry_code,
case 
when iap.industry_code='MDCP' then '嗨秒贷'
when iap.industry_code='VIPD' then 'VIPD'
end product,
sum(case when ibh.bus_type = 1000   then iap.apply_amount/10000 else 0 end) useAmount, 
sum(case when ibh.bus_type = 3000   then iap.apply_amount/10000 else 0 end) loanAmount,
sum(case when iap.`status`=0 then iap.apply_amount/10000 else 0 end) waitApplyAmount,
sum(case when ibh.bus_type = 1000 AND NOT EXISTS (SELECT ibh2.hy_application_no FROM inv_bus_history ibh2 WHERE ibh.hy_application_no = ibh2.hy_application_no AND ibh2.bus_type in(2000,2001)) then iap.apply_amount/10000 else 0 end) applyNotLoanAmount

FROM inv_bus_history ibh
inner join inv_application_pay iap on ibh.hy_application_no = iap.hy_application_no 
WHERE ibh.create_date >= IF(HOUR(NOW())=0,DATE_FORMAT(date_sub(DATE(NOW()),interval 1 day) ,'%%Y-%%m-%%d'),DATE_FORMAT(NOW() ,'%%Y-%%m-%%d'))
group by ibh.inv_username,organization,applyPeriod,industry_code
) t2 on organization.organization=t2.organization and applyPeriod.applyPeriod=t2.applyPeriod and product.product=t2.product
left join 
(select 
case 
when lp.INVESTOR_NAME='djs_finance' then '汇有财'
when lp.INVESTOR_NAME='yingyan_finance' then '盈衍'
when lp.INVESTOR_NAME='face_finance' then '笑脸' end as organization,
dap.app_install_ment applyPeriod,
dap.HY_INDUSTRY_CODE,
case 
when dap.HY_INDUSTRY_CODE='MDCP'  then  '嗨秒贷'
when dap.HY_INDUSTRY_CODE='VIPD' then 'VIPD' end product,
sum(case when  dap.`STATUS` in ('STATUS07','STATUS68') then dap.APPLY_AMOUNT/10000 else 0 end) waittingCheckAmount, 
sum(case when  dap.`STATUS` in ('STATUS08','STATUS69') then dap.APPLY_AMOUNT/10000 else 0 end) beingCheckAmount, 
sum(case when  dap.`STATUS` ='STATUS36' then dap.APPLY_AMOUNT/10000 else 0 end) waittingSignAmount,
sum(case when iap.`status` IN ( 11, 12 ) and  dap.`STATUS` = "STATUS43"  then dap.APPLY_AMOUNT/ 10000 else 0 end) applyFailure
from d_application_pay dap 
inner join loan_product lp on dap.app_creditproduct_id = lp.id
inner join inv_application_pay iap on iap.hy_application_no = dap.app_application_no 
where lp.INVESTOR_NAME in ('djs_finance','yingyan_finance','face_finance')
and dap.HY_INDUSTRY_CODE in ('MDCP','VIPD')
and dap.`STATUS` in ('STATUS07','STATUS68','STATUS08','STATUS69','STATUS36','STATUS43')
GROUP BY organization,applyPeriod,dap.HY_INDUSTRY_CODE,product) t3
on organization.organization=t3.organization and applyPeriod.applyPeriod=t3.applyPeriod and product.product=t3.product
'''
    #债转数据
    ownAmountProgressSummarySql='''
SELECT 
organization.organization,-- `机构(债转)`,
applyPeriod.applyPeriod,-- 期数,
product.product,-- 产品,
IFNULL(t1.limitAmount,0) limitAmount,-- 额度配置,
IFNULL(t4.waittingCheckAmount,0) waittingCheckAmount,-- 待分案金额,
IFNULL(t4.beingCheckAmount,0) beingCheckAmount,-- 审核中金额,
IFNULL(t4.waittingSignAmount,0) waittingSignAmount,-- 待签约金额,
IFNULL(t2.waitLoanAmount,0) waitLoanAmount,-- 待放款金额,
IFNULL(t3.loanAmount,0) loanAmount,-- 当日已放款金额,
IFNULL(t5.paymentFailure,0) paymentFailure,-- 当日代付失败金额,
IFNULL(t2.waitServiceCheckAmount,0) waitServiceCheckAmount-- 财务及客服处理金额

from 
(select '点金石' organization ) organization
CROSS join 
(select 3 applyPeriod union SELECT 6) applyPeriod
CROSS join 
(select '嗨秒贷' product union SELECT 'VIPD') product
left join (
-- limitAmount
select 
'点金石' organization,
case 
when lid.hy_industry_code = 'MDCP'  then '嗨秒贷'
when lid.hy_industry_code='VIPD' then 'VIPD'
end product,
lid.period applyPeriod,
sum( lid.limit_amount)/ 10000 limitAmount

from limit_info_day lid where lid.limit_date = IF(HOUR(NOW())=0,DATE_FORMAT(date_sub(DATE(NOW()),interval 1 day) ,'%%Y-%%m-%%d'),DATE_FORMAT(NOW() ,'%%Y-%%m-%%d'))
GROUP BY product,applyPeriod) t1
on organization.organization=t1.organization and applyPeriod.applyPeriod=t1.applyPeriod and product.product=t1.product
left join
(
-- waitLoanAmount  waitServiceCheckAmount
SELECT
'点金石' organization,
dap.app_install_ment applyPeriod,
CASE WHEN dap.HY_INDUSTRY_CODE = 'MDCP' THEN '嗨秒贷' 
		 WHEN dap.HY_INDUSTRY_CODE = 'VIPD' THEN 'VIPD' 
END product,
sum( CASE WHEN c.tc_status = 'APRO' AND c.TC_BANK_CODE IN (
		 SELECT DISTINCT t.bank_code FROM inv_withhold_bank t WHERE t.available = 1 AND ( end_issue_date < NOW( ) OR 		 end_issue_date IS NULL ) ) 
		 THEN c.TC_AMOUNT ELSE 0 END ) / 10000 AS waitLoanAmount,
sum(CASE WHEN c.tc_status IN ( 'APRD', 'ADFN' ) THEN c.TC_AMOUNT ELSE 0 END ) / 10000 AS waitServiceCheckAmount
FROM cust_tocash c
INNER JOIN d_application_pay dap ON c.app_No = dap.app_application_no 
WHERE c.tc_status IN ( 'APRD', 'ADFN', 'APRO' ) AND c.pay_mark = 'Y' AND c.tocash_type = 'HTYSBZD' AND c.hy_industry_code != 'THFQ'
group by applyPeriod,product
) t2  on organization.organization=t2.organization and applyPeriod.applyPeriod=t2.applyPeriod and product.product=t2.product

left join(
-- loanAmount
SELECT
	'点金石' organization,
  dap.app_install_ment applyPeriod,
CASE WHEN dap.HY_INDUSTRY_CODE = 'MDCP' THEN '嗨秒贷' 
		 WHEN dap.HY_INDUSTRY_CODE = 'VIPD' THEN 'VIPD' 
		 END product,
IFNULL( sum( iha.apply_amount ) / 10000, 0 ) 'loanAmount' 
FROM
	inv_bus_history ibh
	inner join inv_hy_application iha on ibh.hy_application_no = iha.hy_application_no 
	inner join d_application_pay dap on iha.hy_application_no = dap.app_application_no 
	inner join loan_product lp on dap.app_creditproduct_id = lp.id 
WHERE
	dap.HY_INDUSTRY_CODE in ('MDCP','VIPD')
	AND ibh.bus_type = 3000 
	AND ibh.inv_username = 10000 
	AND ibh.create_date >=IF(HOUR(NOW())=0,DATE_FORMAT(date_sub(DATE(NOW()),interval 1 day) ,'%%Y-%%m-%%d'),DATE_FORMAT(NOW() ,'%%Y-%%m-%%d')) 
	group by applyPeriod,product
) t3 on organization.organization=t3.organization and applyPeriod.applyPeriod=t3.applyPeriod and product.product=t3.product

left join
(
-- waittingCheckAmount beingCheckAmount waittingSignAmount
SELECT
	'点金石' organization,
  dap.app_install_ment applyPeriod,
CASE WHEN dap.HY_INDUSTRY_CODE = 'MDCP' THEN '嗨秒贷' 
		 WHEN dap.HY_INDUSTRY_CODE = 'VIPD' THEN 'VIPD' 
		 END product,
sum(case when dap.`STATUS` IN ( "STATUS07", "STATUS68" ) then  dap.APPLY_AMOUNT  else 0 end) / 10000 as waittingCheckAmount,
sum(case when dap.`STATUS` IN ( "STATUS08","STATUS69" ) then  dap.APPLY_AMOUNT  else 0 end) / 10000 as beingCheckAmount,
sum(case when dap.`STATUS` = "STATUS36"  then  dap.APPLY_AMOUNT  else 0 end) / 10000 as waittingSignAmount
FROM d_application_pay dap
inner join loan_product lp on dap.app_creditproduct_id = lp.id  
WHERE lp.INVESTOR_NAME = 'david_fu' and dap.`STATUS` IN ( "STATUS07", "STATUS68", "STATUS08","STATUS69" ,"STATUS36") and dap.HY_INDUSTRY_CODE in ('MDCP','VIPD')
group by organization,applyPeriod,product) t4
on organization.organization=t4.organization and applyPeriod.applyPeriod=t4.applyPeriod and product.product=t4.product
	 
 left join (
-- 	 paymentFailure
 SELECT
	'点金石' organization,
  dap.app_install_ment applyPeriod,
CASE WHEN dap.HY_INDUSTRY_CODE = 'MDCP' THEN '嗨秒贷' 
		 WHEN dap.HY_INDUSTRY_CODE = 'VIPD' THEN 'VIPD' 
		 END product,
	IFNULL( sum( kdf.AMOUNT ) / 10000, 0 ) paymentFailure
FROM kq_daifu kdf 
inner join d_application_pay dap on  dap.app_application_no = kdf.app_no 
WHERE dap.HY_INDUSTRY_CODE in ('MDCP','VIPD') 
and kdf.COLLECTION_STATUS='WHFA'
and kdf.COLLECTION_DATE >= IF(HOUR(NOW())=0,DATE_FORMAT(date_sub(DATE(NOW()),interval 1 day) ,'%%Y-%%m-%%d'),DATE_FORMAT(NOW() ,'%%Y-%%m-%%d'))
group by organization,applyPeriod,product) t5
on organization.organization=t5.organization and applyPeriod.applyPeriod=t5.applyPeriod and product.product=t5.product
    '''
    today=datetime.datetime.today()
    year = str(today.year)
    month = str(today.month)
    day = str(today.day)
    hour = str(today.hour)

    amountProgressSummaryData = pd.read_sql(amountProgressSummarySql, connFq)


    amountProgressSummaryDataSum=amountProgressSummaryData.sum()
    amountProgressSummaryDataSum['organization']='合计'
    amountProgressSummaryDataSum['applyPeriod'] = '-'
    amountProgressSummaryDataSum['product'] = '-'
    amountProgressSummaryData=amountProgressSummaryData.append(amountProgressSummaryDataSum,ignore_index=True)

    ownAmountProgressSummaryData = pd.read_sql(ownAmountProgressSummarySql, connFq)
    ownAmountProgressSummaryDataSum=ownAmountProgressSummaryData.sum()
    ownAmountProgressSummaryDataSum['organization'] = '合计'
    ownAmountProgressSummaryDataSum['applyPeriod'] = '-'
    ownAmountProgressSummaryDataSum['product'] = '-'
    ownAmountProgressSummaryData=ownAmountProgressSummaryData.append(ownAmountProgressSummaryDataSum,ignore_index=True)
    return year, month, day, hour, amountProgressSummaryData, ownAmountProgressSummaryData


def send_email(year, month, day, hour, amountProgressSummaryData, ownAmountProgressSummaryData):
    import yagmail
    subject = '资金方进件进度日报'
    receiver=['maxiaolei@dpandora.cn']
    cc = ['maxiaolei@dpandora.cn']
    yag = yagmail.SMTP(user="maxiaolei@dpandora.cn", password="1qazXSW@", host="smtp.exmail.qq.com", port="465")

    amountProgressSummaryDataValue = []
    for index, row in amountProgressSummaryData.iterrows():
        amountProgressSummaryDataValue.append({"organization": row['organization'],
                           "applyPeriod": row['applyPeriod'],
                           "product": row['product'],
                           "limitAmount": row['limitAmount'],
                           "waittingCheckAmount": row['waittingCheckAmount'],
                           "beingCheckAmount": row['beingCheckAmount'],
                           "waittingSignAmount": row['waittingSignAmount'],
                           "waitApplyAmount": row['waitApplyAmount'],
                           "useAmount": row['useAmount'],
                           "loanAmount": row['loanAmount'],
                           "applyNotLoanAmount": row['applyNotLoanAmount'],
                           "applyFailure": row['applyFailure']})

    ownAmountProgressSummaryDataValue = []
    for index, row in ownAmountProgressSummaryData.iterrows():
        ownAmountProgressSummaryDataValue.append({"organization": row['organization'],
                           "applyPeriod": row['applyPeriod'],
                           "product": row['product'],
                           "limitAmount": row['limitAmount'],
                           "waittingCheckAmount": row['waittingCheckAmount'],
                           "beingCheckAmount": row['beingCheckAmount'],
                           "waittingSignAmount": row['waittingSignAmount'],
                           "waitLoanAmount": row['waitLoanAmount'],
                           "loanAmount": row['loanAmount'],
                           "paymentFailure": row['paymentFailure'],
                           "waitServiceCheckAmount": row['waitServiceCheckAmount']})

    data_dic = {"year": year, "month": month, "day": day, "hour": hour,
                "amountProgressSummaryDataValue": amountProgressSummaryDataValue,
                "ownAmountProgressSummaryData": ownAmountProgressSummaryDataValue}

    html = render_template('amountProgressSummary.html', data_dic)
    html = html.replace('\n', '')
    #html=html[0:1000]
    #with open('templates/amountProgressSummaryResult', 'w') as fileobject:  # 使用‘w’来提醒python用写入的方式打开
    #    fileobject.write(html)
    yag.send(to=receiver, subject=subject, contents=html, attachments=None, cc=cc)
    yag.close()


def send_amountProgressSummary():
    year, month, day, hour, amountProgressSummaryData, ownAmountProgressSummaryData= gen_amountProgressSummary_report()
    send_email(year, month, day, hour, amountProgressSummaryData, ownAmountProgressSummaryData)

if __name__ == "__main__":
    send_amountProgressSummary()


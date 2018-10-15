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
from src.python.utils.common.duoyuan_conn import *

con = create_engine('mysql+pymysql://maxiaolei:maxiaolei@123@10.253.5.147:3306/fqmall_ht_prod?charset=utf8')
connFq = con.connect()
connFq=dfq()
# def getmailList():
#     mailListSql='''
#     SELECT recipient_email FROM inv_monitor_mail a WHERE mail_type = 3  ORDER BY id
#     '''
#     mailList=pd.read_sql(mailListSql, connFq)
#     mailList=mailList.iloc[:,0].values.tolist()
#     return mailList


def gen_amountProgressSummary_report():

   #直投数据
    amountProgressSummarySql='''
		select '汇有财' as 'organization',
			   '3' as 'applyPeriod',
			   '嗨秒贷' as 'product',
			   (select IFNULL(sum(ila.sb_amount)/10000,0) from inv_limit_amount ila where ila.period=3 and ila.industry_code='MDCP' and ila.inv_code=10010 and ila.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))) as 'limitAmount',
			   (select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='MDCP' and ibh.bus_type=1000 and ibh.apply_period=3 and ibh.inv_username=10010 and ibh.create_date>if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))) as 'useAmount',
			   (select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='MDCP' and ibh.bus_type=2000 and ibh.apply_period=3 and ibh.inv_username=10010 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))) as 'loanAmount',
			   (select IFNULL(sum(t.apply_amount)/10000,0) from inv_application_pay t where t.apply_period=3 and t.industry_code='MDCP'and t.inv_username=10010 and t.status=0) as 'waitApplyAmount',
			   (SELECT IFNULL(sum(iap.apply_amount)/10000,0) FROM inv_bus_history t1,inv_application_pay iap WHERE t1.hy_application_no=iap.hy_application_no and t1.bus_type = 1000 AND NOT EXISTS (SELECT t2.hy_application_no FROM inv_bus_history t2  WHERE t1.hy_application_no = t2.hy_application_no AND (t2.bus_type = 2000 or t2.bus_type = 2001) ) AND iap.apply_period=3 and iap.industry_code='MDCP' and t1.inv_username = 10010) as 'applyNotLoanAmount',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='djs_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")) as 'waittingCheckAmount',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='djs_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")) as 'beingCheckAmount',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='djs_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS`="STATUS36") as 'waittingSignAmount',
			   (select IFNULL(sum(dap.APPLY_AMOUNT)/10000,0) from inv_application_pay iap,d_application_pay dap,loan_product lp where iap.hy_application_no=dap.app_application_no and dap.app_creditproduct_id = lp.id and dap.`STATUS`="STATUS43" and lp.INVESTOR_NAME='djs_finance' and dap.app_install_ment=3 and dap.HY_INDUSTRY_CODE='MDCP' and iap.`status` in(11,12)) as 'applyFailure'
		UNION
		select '汇有财' as 'organization',
			   '3' as 'applyPeriod',
			   'VIP' as 'product',
			   (select IFNULL(sum(ila.sb_amount)/10000,0) from inv_limit_amount ila where ila.period=3 and ila.industry_code='VIPD' and ila.inv_code=10010 and ila.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))) as 'limitAmount',
			   (select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='VIPD' and ibh.bus_type=1000 and ibh.apply_period=3 and ibh.inv_username=10010 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))) as 'useAmount',
			   (select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='VIPD' and ibh.bus_type=2000 and ibh.apply_period=3 and ibh.inv_username=10010 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))) as 'loanAmount',
			   (select IFNULL(sum(t.apply_amount)/10000,0) from inv_application_pay t where t.apply_period=3 and t.industry_code='VIPD' and t.inv_username=10010 and t.status=0) as 'waitApplyAmount',
			   (SELECT IFNULL(sum(iap.apply_amount)/10000,0) FROM inv_bus_history t1,inv_application_pay iap WHERE t1.hy_application_no=iap.hy_application_no and t1.bus_type = 1000 AND NOT EXISTS (SELECT t2.hy_application_no FROM inv_bus_history t2  WHERE t1.hy_application_no = t2.hy_application_no AND (t2.bus_type = 2000 or t2.bus_type = 2001) ) AND iap.apply_period=3 and iap.industry_code='VIPD' and t1.inv_username = 10010) as 'applyNotLoanAmount',
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='djs_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='djs_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='djs_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS`="STATUS36"),
		   (select IFNULL(sum(dap.APPLY_AMOUNT)/10000,0) from inv_application_pay iap,d_application_pay dap,loan_product lp where iap.hy_application_no=dap.app_application_no and dap.app_creditproduct_id = lp.id and dap.`STATUS`="STATUS43" and lp.INVESTOR_NAME='djs_finance' and dap.app_install_ment=3 and dap.HY_INDUSTRY_CODE='VIPD' and iap.`status` in(11,12)) as 'applyFailure'
		UNION
		select '汇有财',
			'6',
			'嗨秒贷' as 'product',
			(select IFNULL(sum(ila.sb_amount)/10000,0) from inv_limit_amount ila where ila.period=6 and ila.industry_code='MDCP' and ila.inv_code=10010 and ila.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='MDCP' and ibh.bus_type=1000 and ibh.apply_period=6 and ibh.inv_username=10010 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='MDCP' and ibh.bus_type=2000 and ibh.apply_period=6 and ibh.inv_username=10010 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(t.apply_amount)/10000,0) from inv_application_pay t where t.apply_period=6 and t.industry_code='MDCP' and t.inv_username=10010 and t.status=0) as 'waitApplyAmount',
			(SELECT IFNULL(sum(iap.apply_amount)/10000,0) FROM inv_bus_history t1,inv_application_pay iap WHERE t1.hy_application_no=iap.hy_application_no and t1.bus_type = 1000 AND NOT EXISTS (SELECT t2.hy_application_no FROM inv_bus_history t2  WHERE t1.hy_application_no = t2.hy_application_no AND (t2.bus_type = 2000 or t2.bus_type = 2001) ) AND iap.apply_period=6 and iap.industry_code='MDCP' and t1.inv_username = 10010),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='djs_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='djs_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='djs_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS`="STATUS36"),
			(select IFNULL(sum(dap.APPLY_AMOUNT)/10000,0) from inv_application_pay iap,d_application_pay dap,loan_product lp where iap.hy_application_no=dap.app_application_no and dap.app_creditproduct_id = lp.id and dap.`STATUS`="STATUS43" and lp.INVESTOR_NAME='djs_finance' and dap.app_install_ment=6 and dap.HY_INDUSTRY_CODE='MDCP' and iap.`status` in(11,12)) as 'applyFailure'
		UNION
		select '汇有财',
			'6',
			'VIPD' as 'product',
			(select IFNULL(sum(ila.sb_amount)/10000,0) from inv_limit_amount ila where ila.period=6 and ila.industry_code='VIPD' and ila.inv_code=10010 and ila.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='VIPD' and ibh.bus_type=1000 and ibh.apply_period=6 and ibh.inv_username=10010 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='VIPD' and ibh.bus_type=2000 and ibh.apply_period=6 and ibh.inv_username=10010 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(t.apply_amount)/10000,0) from inv_application_pay t where t.apply_period=6 and t.industry_code='VIPD' and t.inv_username=10010 and t.status=0) as 'waitApplyAmount',
			(SELECT IFNULL(sum(iap.apply_amount)/10000,0) FROM inv_bus_history t1,inv_application_pay iap WHERE t1.hy_application_no=iap.hy_application_no and t1.bus_type = 1000 AND NOT EXISTS (SELECT t2.hy_application_no FROM inv_bus_history t2  WHERE t1.hy_application_no = t2.hy_application_no AND (t2.bus_type = 2000 or t2.bus_type = 2001) ) AND iap.apply_period=6 and iap.industry_code='VIPD' and t1.inv_username = 10010),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='djs_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='djs_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='djs_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS`="STATUS36"),
			(select IFNULL(sum(dap.APPLY_AMOUNT)/10000,0) from inv_application_pay iap,d_application_pay dap,loan_product lp where iap.hy_application_no=dap.app_application_no and dap.app_creditproduct_id = lp.id and dap.`STATUS`="STATUS43" and lp.INVESTOR_NAME='djs_finance' and dap.app_install_ment=6 and dap.HY_INDUSTRY_CODE='VIPD' and iap.`status` in(11,12)) as 'applyFailure'
		UNION
		select '盈衍',
			'3',
			'嗨秒贷' as 'product',
			(select IFNULL(sum(ila.sb_amount)/10000,0) from inv_limit_amount ila where ila.period=3 and ila.industry_code='MDCP' and ila.inv_code=10003 and ila.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='MDCP' and ibh.bus_type=1000 and ibh.apply_period=3 and ibh.inv_username=10003 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='MDCP' and ibh.bus_type=3000 and ibh.apply_period=3 and ibh.inv_username=10003 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(t.apply_amount)/10000,0) from inv_application_pay t where t.apply_period=3 and t.industry_code='MDCP' and t.inv_username=10003 and t.status=0) as 'waitApplyAmount',
			(SELECT IFNULL(sum(iap.apply_amount)/10000,0) FROM inv_bus_history t1,inv_application_pay iap WHERE t1.hy_application_no=iap.hy_application_no and t1.bus_type = 1000 AND NOT EXISTS (SELECT t2.hy_application_no FROM inv_bus_history t2  WHERE t1.hy_application_no = t2.hy_application_no AND (t2.bus_type = 2000 or t2.bus_type = 2001)) AND iap.apply_period=3 and iap.industry_code='MDCP' and t1.inv_username = 10003),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='yingyan_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='yingyan_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='yingyan_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS`="STATUS36"),
			(select IFNULL(sum(dap.APPLY_AMOUNT)/10000,0) from inv_application_pay iap,d_application_pay dap,loan_product lp where iap.hy_application_no=dap.app_application_no and dap.app_creditproduct_id = lp.id and dap.`STATUS`="STATUS43" and lp.INVESTOR_NAME='yingyan_finance' and dap.app_install_ment=3 and dap.HY_INDUSTRY_CODE='MDCP' and iap.`status` in(11,12)) as 'applyFailure'
		UNION
		select '盈衍',
			'3',
			'VIPD' as 'product',
			(select IFNULL(sum(ila.sb_amount)/10000,0) from inv_limit_amount ila where ila.period=3 and ila.industry_code='VIPD' and ila.inv_code=10003 and ila.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='VIPD' and ibh.bus_type=1000 and ibh.apply_period=3 and ibh.inv_username=10003 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='VIPD' and ibh.bus_type=3000 and ibh.apply_period=3 and ibh.inv_username=10003 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(t.apply_amount)/10000,0) from inv_application_pay t where t.apply_period=3 and t.industry_code='VIPD' and t.inv_username=10003 and t.status=0) as 'waitApplyAmount',
			(SELECT IFNULL(sum(iap.apply_amount)/10000,0) FROM inv_bus_history t1,inv_application_pay iap WHERE t1.hy_application_no=iap.hy_application_no and t1.bus_type = 1000 AND NOT EXISTS (SELECT t2.hy_application_no FROM inv_bus_history t2  WHERE t1.hy_application_no = t2.hy_application_no AND (t2.bus_type = 2000 or t2.bus_type = 2001)) AND iap.apply_period=3 and iap.industry_code='VIPD' and t1.inv_username = 10003),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='yingyan_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='yingyan_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='yingyan_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS`="STATUS36"),
			(select IFNULL(sum(dap.APPLY_AMOUNT)/10000,0) from inv_application_pay iap,d_application_pay dap,loan_product lp where iap.hy_application_no=dap.app_application_no and dap.app_creditproduct_id = lp.id and dap.`STATUS`="STATUS43" and lp.INVESTOR_NAME='yingyan_finance' and dap.app_install_ment=3 and dap.HY_INDUSTRY_CODE='VIPD' and iap.`status` in(11,12)) as 'applyFailure'
		UNION
		select '盈衍',
			'6',
			'嗨秒贷' as 'product',
			(select IFNULL(sum(ila.sb_amount)/10000,0) from inv_limit_amount ila where ila.period=6 and ila.industry_code='MDCP' and ila.inv_code=10003 and ila.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='MDCP' and ibh.bus_type=1000 and ibh.apply_period=6 and ibh.inv_username=10003 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='MDCP' and ibh.bus_type=3000 and ibh.apply_period=6 and ibh.inv_username=10003 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(t.apply_amount)/10000,0) from inv_application_pay t where t.apply_period=6 and t.industry_code='MDCP' and t.inv_username=10003 and t.status=0) as 'waitApplyAmount',
			(SELECT IFNULL(sum(iap.apply_amount)/10000,0) FROM inv_bus_history t1,inv_application_pay iap WHERE t1.hy_application_no=iap.hy_application_no and t1.bus_type = 1000 AND NOT EXISTS (SELECT t2.hy_application_no FROM inv_bus_history t2  WHERE t1.hy_application_no = t2.hy_application_no AND (t2.bus_type = 2000 or t2.bus_type = 2001)) AND iap.apply_period=6 and iap.industry_code='MDCP' and t1.inv_username = 10003),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='yingyan_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='yingyan_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='yingyan_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` ="STATUS36"),
			(select IFNULL(sum(dap.APPLY_AMOUNT)/10000,0) from inv_application_pay iap,d_application_pay dap,loan_product lp where iap.hy_application_no=dap.app_application_no and dap.app_creditproduct_id = lp.id and dap.`STATUS`="STATUS43" and lp.INVESTOR_NAME='yingyan_finance' and dap.app_install_ment=6 and dap.HY_INDUSTRY_CODE='MDCP' and iap.`status` in(11,12)) as 'applyFailure'
		UNION
		select '盈衍',
			'6',
			'VIPD' as 'product',
			(select IFNULL(sum(ila.sb_amount)/10000,0) from inv_limit_amount ila where ila.period=6 and ila.industry_code='VIPD' and ila.inv_code=10003 and ila.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='VIPD' and ibh.bus_type=1000 and ibh.apply_period=6 and ibh.inv_username=10003 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='VIPD' and ibh.bus_type=3000 and ibh.apply_period=6 and ibh.inv_username=10003 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(t.apply_amount)/10000,0) from inv_application_pay t where t.apply_period=6 and t.industry_code='VIPD' and t.inv_username=10003 and t.status=0) as 'waitApplyAmount',
			(SELECT IFNULL(sum(iap.apply_amount)/10000,0) FROM inv_bus_history t1,inv_application_pay iap WHERE t1.hy_application_no=iap.hy_application_no and t1.bus_type = 1000 AND NOT EXISTS (SELECT t2.hy_application_no FROM inv_bus_history t2  WHERE t1.hy_application_no = t2.hy_application_no AND (t2.bus_type = 2000 or t2.bus_type = 2001)) AND iap.apply_period=6 and iap.industry_code='VIPD' and t1.inv_username = 10003),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='yingyan_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='yingyan_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='yingyan_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` ="STATUS36"),
			(select IFNULL(sum(dap.APPLY_AMOUNT)/10000,0) from inv_application_pay iap,d_application_pay dap,loan_product lp where iap.hy_application_no=dap.app_application_no and dap.app_creditproduct_id = lp.id and dap.`STATUS`="STATUS43" and lp.INVESTOR_NAME='yingyan_finance' and dap.app_install_ment=6 and dap.HY_INDUSTRY_CODE='VIPD' and iap.`status` in(11,12)) as 'applyFailure'
		UNION
		select '笑脸',
			'3',
			'嗨秒贷' as 'product',
			(select IFNULL(sum(ila.sb_amount)/10000,0) from inv_limit_amount ila where ila.period=3 and ila.industry_code='MDCP' and ila.inv_code=10002 and ila.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='MDCP' and ibh.bus_type=1000 and ibh.apply_period=3 and ibh.inv_username=10002 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='MDCP' and ibh.bus_type=3000 and ibh.apply_period=3 and ibh.inv_username=10002 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(t.apply_amount)/10000,0) from inv_application_pay t where t.apply_period=3 and t.industry_code='MDCP' and t.inv_username=10002 and t.status=0) as 'waitApplyAmount',
			(SELECT IFNULL(sum(iap.apply_amount)/10000,0) FROM inv_bus_history t1,inv_application_pay iap WHERE t1.hy_application_no=iap.hy_application_no and t1.bus_type = 1000 AND NOT EXISTS (SELECT t2.hy_application_no FROM inv_bus_history t2  WHERE t1.hy_application_no = t2.hy_application_no AND (t2.bus_type = 2000 or t2.bus_type = 2001)) AND iap.apply_period=3 and iap.industry_code='MDCP' and t1.inv_username = 10002),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='face_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='face_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='face_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS`="STATUS36"),
			(select IFNULL(sum(dap.APPLY_AMOUNT)/10000,0) from inv_application_pay iap,d_application_pay dap,loan_product lp where iap.hy_application_no=dap.app_application_no and dap.app_creditproduct_id = lp.id and dap.`STATUS`="STATUS43" and lp.INVESTOR_NAME='face_finance' and dap.app_install_ment=3 and dap.HY_INDUSTRY_CODE='MDCP' and iap.`status` in(11,12)) as 'applyFailure'
		UNION
		select '笑脸',
			'3',
			'VIPD' as 'product',
			(select IFNULL(sum(ila.sb_amount)/10000,0) from inv_limit_amount ila where ila.period=3 and ila.industry_code='VIPD' and ila.inv_code=10002 and ila.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='VIPD' and ibh.bus_type=1000 and ibh.apply_period=3 and ibh.inv_username=10002 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='VIPD' and ibh.bus_type=3000 and ibh.apply_period=3 and ibh.inv_username=10002 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(t.apply_amount)/10000,0) from inv_application_pay t where t.apply_period=3  and t.industry_code='VIPD' and t.inv_username=10002 and t.status=0) as 'waitApplyAmount',
			(SELECT IFNULL(sum(iap.apply_amount)/10000,0) FROM inv_bus_history t1,inv_application_pay iap WHERE t1.hy_application_no=iap.hy_application_no and t1.bus_type = 1000 AND NOT EXISTS (SELECT t2.hy_application_no FROM inv_bus_history t2  WHERE t1.hy_application_no = t2.hy_application_no AND (t2.bus_type = 2000 or t2.bus_type = 2001)) AND iap.apply_period=3 and iap.industry_code='VIPD' and t1.inv_username = 10002),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='face_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='face_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='face_finance' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` ="STATUS36"),
			(select IFNULL(sum(dap.APPLY_AMOUNT)/10000,0) from inv_application_pay iap,d_application_pay dap,loan_product lp where iap.hy_application_no=dap.app_application_no and dap.app_creditproduct_id = lp.id and dap.`STATUS`="STATUS43" and lp.INVESTOR_NAME='face_finance' and dap.app_install_ment=3 and dap.HY_INDUSTRY_CODE='VIPD' and iap.`status` in(11,12)) as 'applyFailure'
		UNION
		select '笑脸',
			'6',
			'嗨秒贷' as 'product',
			(select IFNULL(sum(ila.sb_amount)/10000,0) from inv_limit_amount ila where ila.period=6 and ila.industry_code='MDCP' and ila.inv_code=10002 and ila.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='MDCP' and ibh.bus_type=1000 and ibh.apply_period=6 and ibh.inv_username=10002 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='MDCP' and ibh.bus_type=3000 and ibh.apply_period=6 and ibh.inv_username=10002 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(t.apply_amount)/10000,0) from inv_application_pay t where t.apply_period=6 and t.industry_code='MDCP' AND t.inv_username=10002 and t.status=0) as 'waitApplyAmount',
			(SELECT IFNULL(sum(iap.apply_amount)/10000,0) FROM inv_bus_history t1,inv_application_pay iap WHERE t1.hy_application_no=iap.hy_application_no and t1.bus_type = 1000 AND NOT EXISTS (SELECT t2.hy_application_no FROM inv_bus_history t2  WHERE t1.hy_application_no = t2.hy_application_no AND (t2.bus_type = 2000 or t2.bus_type = 2001) ) AND iap.apply_period=6 and iap.industry_code='MDCP'  and t1.inv_username = 10002),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='face_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='face_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='face_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS`="STATUS36"),
			(select IFNULL(sum(dap.APPLY_AMOUNT)/10000,0) from inv_application_pay iap,d_application_pay dap,loan_product lp where iap.hy_application_no=dap.app_application_no and dap.app_creditproduct_id = lp.id and dap.`STATUS`="STATUS43" and lp.INVESTOR_NAME='face_finance' and dap.app_install_ment=6 and dap.HY_INDUSTRY_CODE='MDCP' and iap.`status` in(11,12)) as 'applyFailure'
		UNION
		select '笑脸',
			'6',
			'VIPD' as 'product',
			(select IFNULL(sum(ila.sb_amount)/10000,0) from inv_limit_amount ila where ila.period=6 and ila.industry_code='VIPD' and ila.inv_code=10002 and ila.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='VIPD' and ibh.bus_type=1000 and ibh.apply_period=6 and ibh.inv_username=10002 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(iap.apply_amount)/10000,0) from inv_bus_history ibh,inv_application_pay iap where ibh.hy_application_no=iap.hy_application_no and iap.industry_code='VIPD' and ibh.bus_type=3000 and ibh.apply_period=6 and ibh.inv_username=10002 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))),
			(select IFNULL(sum(t.apply_amount)/10000,0) from inv_application_pay t where t.apply_period=6 AND t.industry_code='VIPD'  and t.inv_username=10002 and t.status=0) as 'waitApplyAmount',
			(SELECT IFNULL(sum(iap.apply_amount)/10000,0) FROM inv_bus_history t1,inv_application_pay iap WHERE t1.hy_application_no=iap.hy_application_no and t1.bus_type = 1000 AND NOT EXISTS (SELECT t2.hy_application_no FROM inv_bus_history t2  WHERE t1.hy_application_no = t2.hy_application_no AND (t2.bus_type = 2000 or t2.bus_type = 2001) ) AND iap.apply_period=6 and iap.industry_code='VIPD' and t1.inv_username = 10002),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='face_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='face_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			(select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='face_finance' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS`="STATUS36"),
			(select IFNULL(sum(dap.APPLY_AMOUNT)/10000,0) from inv_application_pay iap,d_application_pay dap,loan_product lp where iap.hy_application_no=dap.app_application_no and dap.app_creditproduct_id = lp.id and dap.`STATUS`="STATUS43" and lp.INVESTOR_NAME='face_finance' and dap.app_install_ment=6 and dap.HY_INDUSTRY_CODE='VIPD' and iap.`status` in(11,12)) as 'applyFailure'
'''
    #债转数据
    ownAmountProgressSummarySql='''
		select '点金石' as 'organization',
			   '3' as 'applyPeriod',
			   '嗨秒贷' as 'product',
			   (select IFNULL(sum(lid.limit_amount)/10000,0) from limit_info_day lid where lid.hy_industry_code='MDCP' and lid.period=3 and lid.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))) as 'limitAmount',
			   (SELECT IFNULL(sum(c.TC_AMOUNT) / 10000, 0) FROM cust_tocash c JOIN d_application_pay dap ON c.app_No = dap.app_application_no WHERE dap.app_install_ment = 3 AND dap.HY_INDUSTRY_CODE = 'MDCP' AND c.tc_status = 'APRO' AND c.pay_mark = 'Y' AND c.TC_BANK_CODE in(SELECT DISTINCT t.bank_code from inv_withhold_bank t where t.available = 1 AND ((t.begin_issue_date IS NULL AND end_issue_date IS NULL)OR NOW() <t.begin_issue_date OR NOW() > t.end_issue_date))) as 'waitLoanAmount',
			   (select IFNULL(sum(iha.apply_amount)/10000,0) from inv_bus_history ibh,inv_hy_application iha,d_application_pay dap where iha.hy_application_no = dap.app_application_no and dap.app_install_ment=3 and ibh.hy_application_no=iha.hy_application_no and dap.HY_INDUSTRY_CODE='MDCP' and ibh.bus_type=3000 and ibh.inv_username=10000 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d')) and ibh.create_date <DATE_FORMAT(date_add(date(now()),INTERVAL 1 day),'%%Y-%%m-%%d')) as 'loanAmount',
			   (select IFNULL(sum(kdf.AMOUNT)/10000,0) from kq_daifu kdf,d_application_pay dap where dap.app_application_no=kdf.app_no and dap.app_install_ment=3 and kdf.COLLECTION_STATUS='WHFA' and kdf.hy_industry_code='MDCP' and kdf.COLLECTION_DATE>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d')) and kdf.COLLECTION_DATE <DATE_FORMAT(date_add(date(now()),INTERVAL 1 day),'%%Y-%%m-%%d')) as 'paymentFailure',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME ='david_fu' and t.app_install_ment =3 and t.HY_INDUSTRY_CODE ='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")) as 'waittingCheckAmount',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME ='david_fu' and t.app_install_ment =3 and t.HY_INDUSTRY_CODE ='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")) as 'beingCheckAmount',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME ='david_fu' and t.app_install_ment =3 and t.HY_INDUSTRY_CODE ='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` ="STATUS36") as 'waittingSignAmount',
			   (SELECT IFNULL(sum(c.TC_AMOUNT)/10000,0) FROM cust_tocash c JOIN d_application_pay dap ON c.app_No = dap.app_application_no WHERE dap.app_install_ment=3 and dap.HY_INDUSTRY_CODE='MDCP' and c.tc_status in('APRD', 'ADFN') AND c.pay_mark = 'Y' AND c.tocash_type = 'HTYSBZD' AND c.hy_industry_code != 'THFQ') as 'waitServiceCheckAmount'
		UNION
		select '点金石' as 'organization',
			   '6' as 'applyPeriod',
			   '嗨秒贷' as 'product',
			   (select IFNULL(sum(lid.limit_amount)/10000,0) from limit_info_day lid where lid.hy_industry_code='MDCP' and lid.period=6 and lid.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))) as 'limitAmount',
			   (SELECT IFNULL(sum(c.TC_AMOUNT) / 10000, 0) FROM cust_tocash c JOIN d_application_pay dap ON c.app_No = dap.app_application_no WHERE dap.app_install_ment = 6 AND dap.HY_INDUSTRY_CODE = 'MDCP' AND c.tc_status = 'APRO' AND c.pay_mark = 'Y' AND c.TC_BANK_CODE in(SELECT DISTINCT t.bank_code from inv_withhold_bank t where t.available = 1 AND ((t.begin_issue_date IS NULL AND end_issue_date IS NULL)OR NOW() <t.begin_issue_date OR NOW() > t.end_issue_date))) as 'waitLoanAmount',
			   (select IFNULL(sum(iha.apply_amount)/10000,0) from inv_bus_history ibh,inv_hy_application iha,d_application_pay dap where iha.hy_application_no = dap.app_application_no and dap.app_install_ment=6  and ibh.hy_application_no=iha.hy_application_no and dap.HY_INDUSTRY_CODE='MDCP' and ibh.bus_type=3000 and ibh.inv_username=10000 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d')) and ibh.create_date <DATE_FORMAT(date_add(date(now()),INTERVAL 1 day),'%%Y-%%m-%%d')) as 'loanAmount',
			   (select IFNULL(sum(kdf.AMOUNT)/10000,0) from kq_daifu kdf,d_application_pay dap where dap.app_application_no=kdf.app_no and dap.app_install_ment=6 and kdf.COLLECTION_STATUS='WHFA' and kdf.hy_industry_code='MDCP' and kdf.COLLECTION_DATE>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d')) and kdf.COLLECTION_DATE <DATE_FORMAT(date_add(date(now()),INTERVAL 1 day),'%%Y-%%m-%%d')) as 'paymentFailure',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME ='david_fu' and t.app_install_ment =6 and t.HY_INDUSTRY_CODE ='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")) as 'waittingCheckAmount',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME ='david_fu' and t.app_install_ment =6 and t.HY_INDUSTRY_CODE ='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME ='david_fu' and t.app_install_ment =6 and t.HY_INDUSTRY_CODE ='MDCP' and t.app_creditproduct_id = lp.id and t.`STATUS`="STATUS36"),
			   (SELECT IFNULL(sum(c.TC_AMOUNT)/10000,0) FROM cust_tocash c JOIN d_application_pay dap ON c.app_No = dap.app_application_no WHERE dap.app_install_ment=6 and dap.HY_INDUSTRY_CODE='MDCP' and c.tc_status in('APRD', 'ADFN') AND c.pay_mark = 'Y' AND c.tocash_type = 'HTYSBZD' AND c.hy_industry_code != 'THFQ') as 'waitServiceCheckAmount'
		UNION
		select '点金石' as 'organization',
			   '3' as 'applyPeriod',
			   'VIP' as 'product',
			   (select IFNULL(sum(lid.limit_amount)/10000,0) from limit_info_day lid where lid.hy_industry_code='VIPD' and lid.period=3 and lid.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))) as 'limitAmount',
			   (SELECT IFNULL(sum(c.TC_AMOUNT) / 10000, 0) FROM cust_tocash c JOIN d_application_pay dap ON c.app_No = dap.app_application_no WHERE dap.app_install_ment = 3 AND dap.HY_INDUSTRY_CODE = 'VIPD' AND c.tc_status = 'APRO' AND c.pay_mark = 'Y' AND c.TC_BANK_CODE in(SELECT DISTINCT t.bank_code from inv_withhold_bank t where t.available = 1 AND ((t.begin_issue_date IS NULL AND end_issue_date IS NULL)OR NOW() <t.begin_issue_date OR NOW() > t.end_issue_date))) as 'waitLoanAmount',
			   (select IFNULL(sum(iha.apply_amount)/10000,0) from inv_bus_history ibh,inv_hy_application iha,d_application_pay dap where iha.hy_application_no = dap.app_application_no and dap.app_install_ment=3 and ibh.hy_application_no=iha.hy_application_no and dap.HY_INDUSTRY_CODE='VIPD' and ibh.bus_type=3000 and ibh.inv_username=10000 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d')) and ibh.create_date <DATE_FORMAT(date_add(date(now()),INTERVAL 1 day),'%%Y-%%m-%%d')) as 'loanAmount',
			   (select IFNULL(sum(kdf.AMOUNT)/10000,0) from kq_daifu kdf,d_application_pay dap where dap.app_application_no=kdf.app_no and dap.app_install_ment=3 and  kdf.COLLECTION_STATUS='WHFA' and kdf.hy_industry_code='VIPD' and kdf.COLLECTION_DATE>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d')) and kdf.COLLECTION_DATE <DATE_FORMAT(date_add(date(now()),INTERVAL 1 day),'%%Y-%%m-%%d')) as 'paymentFailure',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='david_fu' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE ='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")) as 'waittingCheckAmount',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='david_fu' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE ='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME='david_fu' and t.app_install_ment=3 and t.HY_INDUSTRY_CODE ='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS`="STATUS36"),
			   (SELECT IFNULL(sum(c.TC_AMOUNT)/10000,0) FROM cust_tocash c JOIN d_application_pay dap ON c.app_No = dap.app_application_no WHERE dap.app_install_ment=3 and dap.HY_INDUSTRY_CODE='VIPD' and c.tc_status in('APRD', 'ADFN') AND c.pay_mark = 'Y' AND c.tocash_type = 'HTYSBZD' AND c.hy_industry_code != 'THFQ') as 'waitServiceCheckAmount'
		UNION
		select '点金石' as 'organization',
			   '6' as 'applyPeriod',
			   'VIP' as 'product',
			   (select IFNULL(sum(lid.limit_amount)/10000,0) from limit_info_day lid where lid.hy_industry_code='VIPD' and lid.period=6 and lid.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))) as 'limitAmount',
			   (SELECT IFNULL(sum(c.TC_AMOUNT) / 10000, 0) FROM cust_tocash c JOIN d_application_pay dap ON c.app_No = dap.app_application_no WHERE dap.app_install_ment = 6 AND dap.HY_INDUSTRY_CODE = 'VIPD' AND c.tc_status = 'APRO' AND c.pay_mark = 'Y'  AND c.TC_BANK_CODE in(SELECT DISTINCT t.bank_code from inv_withhold_bank t where t.available = 1 AND ((t.begin_issue_date IS NULL AND end_issue_date IS NULL)OR NOW() <t.begin_issue_date OR NOW() > t.end_issue_date))) as 'waitLoanAmount',
			   (select IFNULL(sum(iha.apply_amount)/10000,0) from inv_bus_history ibh,inv_hy_application iha,d_application_pay dap where iha.hy_application_no = dap.app_application_no and dap.app_install_ment=6 and ibh.hy_application_no=iha.hy_application_no and dap.HY_INDUSTRY_CODE='VIPD' and ibh.bus_type=3000 and ibh.inv_username=10000 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d')) and ibh.create_date <DATE_FORMAT(date_add(date(now()),INTERVAL 1 day),'%%Y-%%m-%%d')) as 'loanAmount',
			   (select IFNULL(sum(kdf.AMOUNT)/10000,0) from kq_daifu kdf,d_application_pay dap where dap.app_application_no=kdf.app_no and dap.app_install_ment=6 and kdf.COLLECTION_STATUS='WHFA' and kdf.hy_industry_code='VIPD' and kdf.COLLECTION_DATE>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d')) and kdf.COLLECTION_DATE <DATE_FORMAT(date_add(date(now()),INTERVAL 1 day),'%%Y-%%m-%%d')) as 'paymentFailure',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME ='david_fu' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE ='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68")) as 'waittingCheckAmount',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME ='david_fu' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE ='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69")),
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME ='david_fu' and t.app_install_ment=6 and t.HY_INDUSTRY_CODE ='VIPD' and t.app_creditproduct_id = lp.id and t.`STATUS`="STATUS36"),
			   (SELECT IFNULL(sum(c.TC_AMOUNT)/10000,0) FROM cust_tocash c JOIN d_application_pay dap ON c.app_No = dap.app_application_no WHERE dap.app_install_ment=6 and dap.HY_INDUSTRY_CODE='VIPD' and c.tc_status in('APRD', 'ADFN') AND c.pay_mark = 'Y' AND c.tocash_type = 'HTYSBZD' AND c.hy_industry_code != 'THFQ') as 'waitServiceCheckAmount'
  
        union
        		select '点金石' as 'organization',
			   '1' as 'applyPeriod',
			   '滴答贷' as 'product',
			   (select IFNULL(sum(lid.limit_amount)/10000,0) from limit_info_day lid where lid.hy_industry_code='LDDD' and lid.period=1 and lid.limit_date=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d'))) as 'limitAmount',
			   (SELECT IFNULL(sum(c.TC_AMOUNT) / 10000, 0) FROM cust_tocash c JOIN d_application_pay dap ON c.app_No = dap.app_application_no WHERE dap.app_install_ment = 1 AND dap.HY_INDUSTRY_CODE = 'LDDD' AND c.tc_status = 'APRO' AND c.pay_mark = 'Y'  AND c.TC_BANK_CODE in(SELECT DISTINCT t.bank_code from inv_withhold_bank t where t.available = 1 AND ((t.begin_issue_date IS NULL AND end_issue_date IS NULL)OR NOW() <t.begin_issue_date OR NOW() > t.end_issue_date))) as 'waitLoanAmount',
			   (select IFNULL(sum(iha.apply_amount)/10000,0) from inv_bus_history ibh,inv_hy_application iha,d_application_pay dap where iha.hy_application_no = dap.app_application_no and dap.app_install_ment=1 and ibh.hy_application_no=iha.hy_application_no and dap.HY_INDUSTRY_CODE='LDDD' and ibh.bus_type=3000 and ibh.inv_username=10000 and ibh.create_date>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d')) and ibh.create_date <DATE_FORMAT(date_add(date(now()),INTERVAL 1 day),'%%Y-%%m-%%d')) as 'loanAmount',
			   (select IFNULL(sum(kdf.AMOUNT)/10000,0) from kq_daifu kdf,d_application_pay dap where dap.app_application_no=kdf.app_no and dap.app_install_ment=1 and kdf.COLLECTION_STATUS='WHFA' and kdf.hy_industry_code='LDDD' and kdf.COLLECTION_DATE>=if(hour(now())=0, DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d'),DATE_FORMAT(NOW(),'%%Y-%%m-%%d')) and kdf.COLLECTION_DATE <DATE_FORMAT(date_add(date(now()),INTERVAL 1 day),'%%Y-%%m-%%d')) as 'paymentFailure',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME ='david_fu' and t.app_install_ment=1 and t.HY_INDUSTRY_CODE ='LDDD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS07","STATUS68") and t.create_date>'2018-09-27') as 'waittingCheckAmount',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME ='david_fu' and t.app_install_ment=1 and t.HY_INDUSTRY_CODE ='LDDD' and t.app_creditproduct_id = lp.id and t.`STATUS` in("STATUS08","STATUS69") and t.create_date>'2018-09-27') 'beingCheckAmount',
			   (select IFNULL(sum(t.APPLY_AMOUNT)/10000,0) from d_application_pay t,loan_product lp where lp.INVESTOR_NAME ='david_fu' and t.app_install_ment=1 and t.HY_INDUSTRY_CODE ='LDDD' and t.app_creditproduct_id = lp.id and t.`STATUS`="STATUS36" and t.create_date>'2018-09-27') 'waittingSignAmount',
			   (SELECT IFNULL(sum(c.TC_AMOUNT)/10000,0) FROM cust_tocash c JOIN d_application_pay dap ON c.app_No = dap.app_application_no WHERE dap.app_install_ment=1 and dap.HY_INDUSTRY_CODE='LDDD' and c.tc_status in('APRD', 'ADFN') AND c.pay_mark = 'Y' AND c.tocash_type = 'HTYSBZD' AND c.hy_industry_code != 'THFQ' and dap.create_date>'2018-09-27') as 'waitServiceCheckAmount'
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


    amountProgressSummaryDataSum['limitAmount']=round(amountProgressSummaryDataSum['limitAmount'],2)
    amountProgressSummaryDataSum['useAmount'] = round(amountProgressSummaryDataSum['useAmount'], 2)
    amountProgressSummaryDataSum['loanAmount']=round(amountProgressSummaryDataSum['loanAmount'],2)
    amountProgressSummaryDataSum['waitApplyAmount']=round(amountProgressSummaryDataSum['waitApplyAmount'],2)
    amountProgressSummaryDataSum['applyNotLoanAmount']=round(amountProgressSummaryDataSum['applyNotLoanAmount'],2)
    amountProgressSummaryDataSum['waittingCheckAmount']=round(amountProgressSummaryDataSum['waittingCheckAmount'],2)
    amountProgressSummaryDataSum['beingCheckAmount'] = round(amountProgressSummaryDataSum['beingCheckAmount'], 2)
    amountProgressSummaryDataSum['waittingSignAmount'] = round(amountProgressSummaryDataSum['waittingSignAmount'], 2)
    amountProgressSummaryDataSum['applyFailure'] = round(amountProgressSummaryDataSum['applyFailure'], 2)

    amountProgressSummaryData=amountProgressSummaryData.append(amountProgressSummaryDataSum,ignore_index=True)

    ownAmountProgressSummaryData = pd.read_sql(ownAmountProgressSummarySql, connFq)
    ownAmountProgressSummaryDataSum=ownAmountProgressSummaryData.sum()
    ownAmountProgressSummaryDataSum['organization'] = '合计'
    ownAmountProgressSummaryDataSum['applyPeriod'] = '-'
    ownAmountProgressSummaryDataSum['product'] = '-'


    ownAmountProgressSummaryDataSum['limitAmount'] = round(ownAmountProgressSummaryDataSum['limitAmount'], 2)
    ownAmountProgressSummaryDataSum['waittingCheckAmount'] = round(ownAmountProgressSummaryDataSum['waittingCheckAmount'], 2)
    ownAmountProgressSummaryDataSum['beingCheckAmount'] = round(ownAmountProgressSummaryDataSum['beingCheckAmount'], 2)
    ownAmountProgressSummaryDataSum['waittingSignAmount'] = round(ownAmountProgressSummaryDataSum['waittingSignAmount'], 2)
    ownAmountProgressSummaryDataSum['waitLoanAmount'] = round(ownAmountProgressSummaryDataSum['waitLoanAmount'], 2)
    ownAmountProgressSummaryDataSum['loanAmount'] = round(ownAmountProgressSummaryDataSum['loanAmount'], 2)
    ownAmountProgressSummaryDataSum['paymentFailure'] = round(ownAmountProgressSummaryDataSum['paymentFailure'], 2)
    ownAmountProgressSummaryDataSum['waitServiceCheckAmount'] = round(ownAmountProgressSummaryDataSum['waitServiceCheckAmount'],
                                                                   2)
    ownAmountProgressSummaryData=ownAmountProgressSummaryData.append(ownAmountProgressSummaryDataSum,ignore_index=True)
    return year, month, day, hour, amountProgressSummaryData, ownAmountProgressSummaryData


def send_email(year, month, day, hour, amountProgressSummaryData, ownAmountProgressSummaryData):
    import yagmail
    subject = '资金方进件进度日报'
    receiverTemp=  ['maxiaolei@dpandora.cn'] #getmailList()
    receiver=['haolikun@dpandora.cn','richard.liu@hengyuan-finance.com', 'leo.zhao@hengyuan-finance.com',
              'virginia.xia@hengyuan-finance.com','charles.lin@hengyuan-finance.com', 'emily.zhang@hengyuan-finance.com',
              'vicky.wan@hengyuan-finance.com', 'wangxiaoqing@dpandora.cn','lynn.zhang@hengyuan-finance.com',
              'yongneng.yan@hengyuan-finance.com','wangjun@dpandora.cn','likui@dpandora.cn','randy.sun@hengyuan-finance.com',
              'irwin.chen@idjshi.com', 'hao.wang@hengyuan-finance.com', 'caicai.cai@idjshi.com',
              'gary.zhang@hengyuan-finance.com','yanan.gao@hengyuan-finance.com','kevin.lu@hengyuan-finance.com','yanan.gao@idjshi.com','jacke.pan@hengyuan-finance.com','fanpeijie@dpandora.cn','bill.jiang@hengyuan-finance.com']
    cc = ['maxiaolei@dpandora.cn']
    ccTemp= ['maxiaolei@dpandora.cn']
    yag = yagmail.SMTP(user="security@dpandora.cn", password="Jingyong1234", host="smtp.exmail.qq.com", port="465")

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
    #with open('templates/amountProgressSummaryResult', 'w') as fileobject:  # 使用'w'来提醒python用写入的方式打开
    #    fileobject.write(html)
    yag.send(receiver, subject=subject, contents=html, attachments=None, cc=cc)
    yag.close()


def send_amountProgressSummary():
    year, month, day, hour, amountProgressSummaryData, ownAmountProgressSummaryData= gen_amountProgressSummary_report()
    send_email(year, month, day, hour, amountProgressSummaryData, ownAmountProgressSummaryData)

if __name__ == "__main__":
   send_amountProgressSummary()



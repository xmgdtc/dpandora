#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/11 10:40
# @Author  : mazhi
# @Site    :
import datetime

import pandas as pd
from sqlalchemy import create_engine
from src.python.utils.common.duoyuan_conn import *
# connFq = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(*get_config('fqmall_ht_prod')))
from html_template_helper import render_template



import platform

con = create_engine('mysql+pymysql://maxiaolei:maxiaolei@123@10.253.5.147:3306/fqmall_ht_prod?charset=utf8')
connFq = con.connect()
connFq=dfq()

def getmailList():
    mailListSql = '''
    SELECT recipient_email FROM inv_monitor_mail a WHERE mail_type = 2  ORDER BY id
    '''
    mailList = pd.read_sql(mailListSql, connFq)
    mailList = mailList.iloc[:, 0].values.tolist()
    return mailList


def getKPI(sql):
    data = pd.read_sql(sql, connFq)
    if len(data) > 0:
        result = data.iloc[0, 0]
    else:
        result = ""
    return result

def getDJSLimitDataLDDD(start, period):
    #滴答贷
    LDDD_xe_sql = """
    select limit_amount 当日限额,use_amount 已用限额 
    from limit_info_day where limit_date='%s' and period =%s and hy_industry_code='LDDD'
    """ % (start, period)
    limitAmountLDDD = 0
    useAmountLDDD = 0
    LDDD_xe_data=pd.read_sql(LDDD_xe_sql, connFq)
    if len(LDDD_xe_data) > 0:
        limitAmountLDDD = LDDD_xe_data.at[0, '当日限额']
        useAmountLDDD = LDDD_xe_data.at[0, '已用限额']

    return round(limitAmountLDDD / 10000, 2), round(useAmountLDDD / 10000, 2)

# 获取限额 已用情况
def getDJSLimitData(start, period):
    vipd_three_xe_sql = """
    select limit_amount 当日限额,use_amount 已用限额 
    from limit_info_day where limit_date='%s' and period =%s and hy_industry_code='VIPD'
    """ % (start, period)
    MDCP_three_xe_sql = """
    select limit_amount 当日限额,use_amount 已用限额 
    from limit_info_day where limit_date='%s' and period =%s and hy_industry_code='MDCP'
    """ % (start, period)

    vipd_three_xe_data = pd.read_sql(vipd_three_xe_sql, connFq)
    MDCP_three_xe_data = pd.read_sql(MDCP_three_xe_sql, connFq)

    limitAmountVIPD = 0
    useAmountVIPD = 0
    limitAmountMDCP = 0
    useAmountMDCP = 0

    flag = 1
    if len(vipd_three_xe_data) > 0:
        limitAmountVIPD = vipd_three_xe_data.at[0, '当日限额']
        useAmountVIPD = vipd_three_xe_data.at[0, '已用限额']
    if len(MDCP_three_xe_data) > 0:
        limitAmountMDCP = MDCP_three_xe_data.at[0, '当日限额']
        useAmountMDCP = MDCP_three_xe_data.at[0, '已用限额']


    return round(limitAmountVIPD / 10000, 2), round(useAmountVIPD / 10000, 2), round(limitAmountMDCP / 10000, 2), round(
        useAmountMDCP / 10000, 2)


# 获取限额 已用情况
def getLimitData(start, inv_code, period):
    vipd_three_xe_sql = """
    SELECT ila.hy_limit_amount 当日限额,ila.hy_use_amount 已用限额 FROM inv_limit_amount
    ila WHERE DATE(ila.limit_date)='%s' AND ila.period=%s AND ila.industry_code='VIPD'
    AND ila.inv_code='%s'
    """ % (start, period, inv_code)
    MDCP_three_xe_sql = """
    SELECT ila.hy_limit_amount 当日限额,ila.hy_use_amount 已用限额 FROM inv_limit_amount
    ila WHERE DATE(ila.limit_date)='%s' AND ila.period=%s AND ila.industry_code='MDCP'
    AND ila.inv_code='%s'
    """ % (start, period, inv_code)
    vipd_three_xe_data = pd.read_sql(vipd_three_xe_sql, connFq)
    MDCP_three_xe_data = pd.read_sql(MDCP_three_xe_sql, connFq)
    limitAmountVIPD = 0
    useAmountVIPD = 0
    limitAmountMDCP = 0
    useAmountMDCP = 0
    flag = 1
    if len(vipd_three_xe_data) > 0:
        limitAmountVIPD = vipd_three_xe_data.at[0, '当日限额']
        useAmountVIPD = vipd_three_xe_data.at[0, '已用限额']
    if len(MDCP_three_xe_data) > 0:
        limitAmountMDCP = MDCP_three_xe_data.at[0, '当日限额']
        useAmountMDCP = MDCP_three_xe_data.at[0, '已用限额']

    return round(limitAmountVIPD / 10000, 2), round(useAmountVIPD / 10000, 2), round(limitAmountMDCP / 10000, 2), round(
        useAmountMDCP / 10000, 2)


def getApplyFaildReason(start, end, inv_code):
    sql = '''select CONCAT(t.inv_approval_desc,'(',count(t.inv_approval_desc),')') resultName from inv_application_pay t
        where t.`status` in (11,12)
        and DATE(t.update_date)>='%s' and DATE(t.update_date)<'%s'
        and t.inv_username=%s
        GROUP BY t.inv_approval_desc
        ''' % (start, end, inv_code)
    data = pd.read_sql(sql, connFq)
    result = ''
    for index, row in data.iterrows():
        result = result + row['resultName'] + '<br />'
    return result


# 应代扣
def getWithHoldTotal(start, end, inv_code):
    sql = '''select count(*)
    from inv_bus_history  t
    where inv_username=%s and
    t.create_date >= '%s' and t.create_date < '%s' and t.bus_type = '3000'
    ''' % (inv_code, start, end)
    return getKPI(sql)


# 代扣成功
def getWithHoldSuccess(start, end, inv_code):
    sql = '''select count(*)
    from inv_bus_history  t
    where inv_username=%s and
    t.create_date >= '%s' and t.create_date < '%s' and t.bus_type = '3001'
    ''' % (inv_code, start, end)
    return getKPI(sql)


def getWithHoldFaild(start, end, inv_code):
    sql = '''select count(*)
    from inv_bus_history  t
    where inv_username=%s and
    t.create_date >= '%s' and t.create_date < '%s' and t.bus_type = '3002'
    ''' % (inv_code, start, end)
    return getKPI(sql)


# 等待代扣
def getWithHoldWait(start, end, inv_code):
    sql = '''select count(*) from inv_bus_history  t
where t.inv_username=%s and t.create_date>='%s' and t.create_date<'%s'
and t.bus_type = '3000'
and not EXISTS 
(SELECT 1 from inv_bus_history  t
where t.inv_username=%s and t.create_date>='%s' and t.create_date<'%s'
and t.bus_type in ('3001','3002'))
    ''' % (inv_code, start, end, inv_code, start, end)
    return getKPI(sql)


def append_reasan(data):
    result = ""
    data['reason'] = data['reason'].astype(str)
    data['count'] = data['count'].astype(str)
    if len(data) > 0:
        for index, row in data.iterrows():
            if not row['reason']:
                result = result + row['reason'] + row['count'] + "<br />"
    return result


def gen_hyc(start, end):
    """
    嗨钱对接汇有财每日数据总结
    :param start:
    :param end:
    :return:
    """
    searchApplyFaileReason = """
        select t.inv_approval_desc as 'reason', count(t.inv_approval_desc) as 'count' from inv_application_pay t
    where t.`status` in (11,12)
    and DATE(t.update_date)>='%s' and DATE(t.update_date)<'%s'
    and t.inv_username=10010
    GROUP BY t.inv_approval_desc;
    """ % (start, end)

    searchApplyFaileReason_data = pd.read_sql(searchApplyFaileReason, connFq)
    if len(searchApplyFaileReason_data) > 0:
        applyFailureRootCause = searchApplyFaileReason_data.iloc[0, 0]
    else:
        applyFailureRootCause = ""

    total_sql = """
        SELECT count(hy_application_no) AS '汇有财进件申请数'
    FROM `inv_application_pay` t
    where t.inv_username=10010 and DATE(t.create_date)>='%s' and DATE(t.create_date)<'%s'
    """ % (start, end)
    total_data = pd.read_sql(total_sql, connFq)
    if len(total_data) > 0:
        applyTotalCount = total_data.iloc[0, 0]
    else:
        applyTotalCount = 0

    success_sql = """
        SELECT count(hy_application_no) AS '汇有财进件成功数'
    from inv_dianjinshi_application t
    where DATE(t.create_date)>='%s' and DATE(t.create_date)<'%s'
    """ % (start, end)
    success_data = pd.read_sql(success_sql, connFq)
    if len(success_data) > 0:
        applySuccessCount = success_data.iloc[0, 0]
    else:
        applySuccessCount = 0

    # fail_sql = """
    #     SELECT count(hy_application_no) AS '汇有财进件失败数'
    # FROM `inv_application_pay` t
    # where  t.`status` in (12) and t.inv_username=10010
    # and t.bus_type='1001'
    # and DATE(t.update_date)>='%s' and DATE(t.update_date)<'%s'
    # """ % (start, end)
    # fail_data = pd.read_sql(fail_sql, connFq)
    # if len(fail_data) > 0:
    #     applyFailureCount = fail_data.iloc[0, 0]
    # else:
    #     applyFailureCount = 0

    # 进件失败数
    searchFBapplyFailureCountSQL = '''
    select count(*) from inv_bus_history  t
    where t.inv_username=10010 
    and t.create_date>='%s' and t.create_date<'%s'
    and t.bus_type='1001'
    ''' % (start, end)
    applyFailureCount = getKPI(searchFBapplyFailureCountSQL)

    wait_sql = """
        SELECT count(hy_application_no) AS '汇有财进件等待数'
    FROM `inv_application_pay` t
    where t.`status`=0 and t.inv_username=10010 
    and DATE(t.create_date)>='%s' and DATE(t.create_date)<'%s'
    """ % (start, end)
    wait_data = pd.read_sql(wait_sql, connFq)
    if len(wait_data) > 0:
        applyWaitCount = wait_data.iloc[0, 0]
    else:
        applyWaitCount = 0

    if applySuccessCount + applyFailureCount != 0:
        applySuccessRate = round((applySuccessCount / (applySuccessCount + applyFailureCount)) * 100, 2)
    else:
        applySuccessRate = 0
    # dk_total_sql = """
    #     select count(iap.hy_application_no) as '放款成功数'
    # from inv_application_pay iap
    # LEFT JOIN inv_dianjinshi_application idp ON iap.hy_application_no = idp.hy_application_no
    # where iap.status in(2,3) and iap.inv_username=10010
    # and DATE(iap.update_date)>='%s' and DATE(iap.update_date)<'%s'
    # """ % (start, end)
    # dk_total_data = pd.read_sql(dk_total_sql, connFq)
    # if len(dk_total_data) > 0:
    #     withholdingTotalCount = dk_total_data.iloc[0, 0]
    # else:
    #     withholdingTotalCount = 0

    # searchDJSWithHoldingSuccessCount_sql = """
    #     select count(iap.hy_application_no) as '代扣成功数'
    # from inv_application_pay iap
    # LEFT JOIN inv_dianjinshi_application idp ON iap.hy_application_no = idp.hy_application_no
    # where
    # iap.status in(2,3) and idp.order_status = 4 and idp.withholding_status=1 and iap.inv_username=10010
    # and DATE(iap.update_date)>='%s' and DATE(iap.update_date)<'%s'
    # """ % (start, end)
    # searchDJSWithHoldingSuccessCount_data = pd.read_sql(searchDJSWithHoldingSuccessCount_sql, connFq)
    # if len(searchDJSWithHoldingSuccessCount_data) > 0:
    #     withholdingSuccessCount = searchDJSWithHoldingSuccessCount_data.iloc[0, 0]
    # else:
    #     withholdingSuccessCount = 0

    # searchDJSWithHoldingFailedCount_sql = """
    #     select count(iap.hy_application_no) as '代扣失败数'
    # from inv_application_pay iap
    # LEFT JOIN inv_dianjinshi_application idp ON iap.hy_application_no = idp.hy_application_no
    # where iap.status in(2,3) and idp.order_status = 4 and idp.withholding_status=3
    # and idp.withholding_count =5 and iap.inv_username=10010
    # and DATE(iap.update_date)>='%s' and DATE(iap.update_date)<'%s'
    # """ % (start, end)
    # searchDJSWithHoldingFailedCount_data = pd.read_sql(searchDJSWithHoldingFailedCount_sql, connFq)
    # if len(searchDJSWithHoldingFailedCount_data) > 0:
    #     withholdingFailureCount = searchDJSWithHoldingFailedCount_data.iloc[0, 0]
    # else:
    #     withholdingFailureCount = 0

    # searchDJSWithHoldingWaitingCount_sql = """
    #     select count(iap.hy_application_no) as '代扣等待数'
    # from inv_application_pay iap
    # LEFT JOIN inv_dianjinshi_application idp ON iap.hy_application_no = idp.hy_application_no
    # where iap.status in(2,3) and idp.order_status = 4
    # and (idp.withholding_status!=1 and idp.withholding_count != 5 )  and iap.inv_username=10010
    # and DATE(iap.update_date)>='%s' and DATE(iap.update_date)<'%s'
    # """ % (start, end)
    # searchDJSWithHoldingWaitingCount_data = pd.read_sql(searchDJSWithHoldingWaitingCount_sql, connFq)
    # if len(searchDJSWithHoldingWaitingCount_data) > 0:
    #     withholdingWaitCount =  searchDJSWithHoldingWaitingCount_data.iloc[0, 0]

    searchDJSWithHoldingFailedReason_sql = """
        SELECT
    idp.withhold_reason as 'reason', count(withhold_reason) as 'count'
    FROM
    inv_dianjinshi_application idp where idp.hy_application_no in(
      select t.hy_application_no from inv_bus_history t
      where t.bus_type=3002
      and t.create_date>='%s'
      and t.create_date <'%s'
      and t.inv_username= 10010)
    GROUP BY idp.withhold_reason
    order by count(idp.withhold_reason) desc;
    """ % (start, end)
    searchDJSWithHoldingFailedReason_data = pd.read_sql(searchDJSWithHoldingFailedReason_sql, connFq)
    withholdingFailureRootCause = append_reasan(searchDJSWithHoldingFailedReason_data)
    # if len(searchDJSWithHoldingFailedReason_data) > 0:
    #     withholdingFailureRootCause = searchDJSWithHoldingFailedReason_data.iloc[0, 0]
    # else:
    #     withholdingFailureRootCause = ""

    # if withholdingSuccessCount+withholdingFailureCount !=0:
    #     withholdingSuccessRate = round((withholdingSuccessCount / (withholdingSuccessCount+withholdingFailureCount)) * 100,2)
    limitAmount3VIPD, useAmount3VIPD, limitAmount3MDCP, useAmount3MDCP = getLimitData(start, '10010', '3')
    limitAmount6VIPD, useAmount6VIPD, limitAmount6MDCP, useAmount6MDCP = getLimitData(start, '10010', '6')
    # if not withholdingFailureRootCause:
    #     withholdingFailureRootCause = ""
    withholdingSuccessRate = 0
    withholdingTotalCount = getWithHoldTotal(start, end, '10010')
    withholdingSuccessCount = getWithHoldSuccess(start, end, '10010')
    withholdingFailureCount = getWithHoldFaild(start, end, '10010')
    try:
        withholdingSuccessRate = round(
            (withholdingSuccessCount / (withholdingFailureCount + withholdingSuccessCount)) * 100, 2)
    except Exception:
        pass
    return {"userName": "汇有财",
            "applyTotalCount": applyTotalCount,
            "applySuccessCount": applySuccessCount,
            "applyFailureCount": applyFailureCount,
            "applyWaitCount": applyWaitCount,
            "applySuccessRate": "%.2f%%" % applySuccessRate,
            "applyFailureRootCause": getApplyFaildReason(start, end, '10010'),
            "withholdingTotalCount": withholdingTotalCount,
            "withholdingSuccessCount": withholdingSuccessCount,
            "withholdingFailureCount": withholdingFailureCount,
            "withholdingWaitCount": getWithHoldWait(start, end, '10010'),
            "withholdingSuccessRate": "%.2f%%" % withholdingSuccessRate,
            "withholdingFailureRootCause": withholdingFailureRootCause,
            "flag": 0,
            "limitAmount3MDCP": limitAmount3MDCP,
            "useAmount3MDCP": useAmount3MDCP ,
            "limitAmount3VIPD": limitAmount3VIPD ,
            "useAmount3VIPD": useAmount3VIPD ,
            "limitAmount6MDCP": limitAmount6MDCP ,
            "useAmount6MDCP": useAmount6MDCP ,
            "limitAmount6VIPD": limitAmount6VIPD ,
            "useAmount6VIPD": useAmount6VIPD,
            "limitAmount1LDDD": 0.0,
            "useAmount1LDDD": 0.0
            }


def gen_YingYan(start, end):
    """
    嗨钱对接汇有财每日数据总结
    :param start:
    :param end:
    :return:
    """
    searchApplyFaileReason = """
        select t.inv_approval_desc as 'reason', count(t.inv_approval_desc) as 'count' from inv_application_pay t
    where t.`status` in (11,12)
    and DATE(t.update_date)>='%s' and DATE(t.update_date)<'%s'
    and t.inv_username=10003
    GROUP BY t.inv_approval_desc;
    """ % (start, end)

    searchApplyFaileReason_data = pd.read_sql(searchApplyFaileReason, connFq)
    if len(searchApplyFaileReason_data) > 0:
        applyFailureRootCause = searchApplyFaileReason_data.iloc[0, 0]
    else:
        applyFailureRootCause = ""

    total_sql = """
        SELECT count(hy_application_no) AS '汇有财进件申请数'
    FROM `inv_application_pay` t
    where t.inv_username=10003 and DATE(t.create_date)>='%s' and DATE(t.create_date)<'%s'
    """ % (start, end)
    total_data = pd.read_sql(total_sql, connFq)
    if len(total_data) > 0:
        applyTotalCount = total_data.iloc[0, 0]
    else:
        applyTotalCount = 0

    success_sql = """
    select count(iap.hy_application_no) as '进件成功数'
    from inv_yingyan_application iyp
    LEFT JOIN inv_application_pay iap ON iap.hy_application_no = iyp.hy_application_no
    where DATE(iyp.create_date)>='%s' and DATE(iyp.create_date)<'%s'
    """ % (start, end)
    success_data = pd.read_sql(success_sql, connFq)
    if len(success_data) > 0:
        applySuccessCount = success_data.iloc[0, 0]
    else:
        applySuccessCount = 0

    # fail_sql = """
    #     select count(hy_application_no) as '进件失败数' from inv_application_pay
    # where inv_username = '10003'
    # and status in (11,12)
    # and t.bus_type='1001'
    # and DATE(update_date)>='%s' and DATE(update_date)<'%s'
    # """ % (start, end)
    # fail_data = pd.read_sql(fail_sql, connFq)
    # if len(fail_data) > 0:
    #     applyFailureCount = fail_data.iloc[0, 0]
    # else:
    #     applyFailureCount = 0

    # 进件失败数
    searchFBapplyFailureCountSQL = '''
    select count(*) from inv_bus_history  t
    where t.inv_username=10003 
    and t.create_date>='%s' and t.create_date<'%s'
    and t.bus_type='1001'
    ''' % (start, end)
    applyFailureCount = getKPI(searchFBapplyFailureCountSQL)

    wait_sql = """
         select count(hy_application_no) as '进件等待数' from inv_application_pay
    where inv_username = '10003'
    and status in (0)
    and DATE(create_date)>='%s' and DATE(create_date)<'%s'
    """ % (start, end)
    wait_data = pd.read_sql(wait_sql, connFq)
    if len(wait_data) > 0:
        applyWaitCount = wait_data.iloc[0, 0]
    else:
        applyWaitCount = 0
    applySuccessRate = 0
    if (applySuccessCount + applyFailureCount) != 0:
        applySuccessRate = round((applySuccessCount / (applySuccessCount + applyFailureCount)) * 100, 2)
    dk_total_sql = """
    select count(iap.hy_application_no) as '放款成功数'
    from inv_application_pay iap
    LEFT JOIN inv_yingyan_application iyp ON iap.hy_application_no = iyp.hy_application_no
    LEFT JOIN inv_yingyan_repay_plan iyr on iyp.hy_application_no = iyr.TARGET_CODE and iyr.REPAY_PlAN_NO = 1
    where iap.status in(2,3,4) and iyp.apply_status in(4,5)
    and DATE(iyr.CREATE_DATE)>='%s' and DATE(iyr.CREATE_DATE)<'%s'
    """ % (start, end)
    dk_total_data = pd.read_sql(dk_total_sql, connFq)
    if len(dk_total_data) > 0:
        withholdingTotalCount = dk_total_data.iloc[0, 0]
    else:
        withholdingTotalCount = 0

    searchDJSWithHoldingSuccessCount_sql = """
    select count(iap.hy_application_no) as '代扣成功数'
    from inv_application_pay iap LEFT JOIN inv_yingyan_application iyp ON iap.hy_application_no = iyp.hy_application_no
    LEFT JOIN inv_yingyan_repay_plan iyr on iyp.hy_application_no = iyr.TARGET_CODE and iyr.REPAY_PlAN_NO = 1
    where iap.status in(2,3,4) and iyp.apply_status in(4,5)  and iyp.withholding_status=1
    and DATE(iyr.CREATE_DATE)>='%s' and DATE(iyr.CREATE_DATE)<'%s'
    """ % (start, end)
    searchDJSWithHoldingSuccessCount_data = pd.read_sql(searchDJSWithHoldingSuccessCount_sql, connFq)
    if len(searchDJSWithHoldingSuccessCount_data) > 0:
        withholdingSuccessCount = searchDJSWithHoldingSuccessCount_data.iloc[0, 0]
    else:
        withholdingSuccessCount = 0

    searchDJSWithHoldingFailedCount_sql = """
    select count(iap.hy_application_no) as '代扣失败数'
    from inv_application_pay iap
    LEFT JOIN inv_dianjinshi_application idp ON iap.hy_application_no = idp.hy_application_no
    where iap.status in(2,3) and idp.order_status = 4 and idp.withholding_status=3
    and idp.withholding_count =5 and iap.inv_username=10010
    and DATE(iap.CREATE_DATE)>='%s' and DATE(iap.CREATE_DATE)<'%s'
    """ % (start, end)
    searchDJSWithHoldingFailedCount_data = pd.read_sql(searchDJSWithHoldingFailedCount_sql, connFq)
    if len(searchDJSWithHoldingFailedCount_data) > 0:
        withholdingFailureCount = searchDJSWithHoldingFailedCount_data.iloc[0, 0]
    else:
        withholdingFailureCount = 0

    # searchDJSWithHoldingFailedCountForAlarm_sql = """
    #     select count(iap.hy_application_no) as '代扣失败数'
    # from inv_application_pay iap
    # LEFT JOIN inv_dianjinshi_application idp ON iap.hy_application_no = idp.hy_application_no
    # where iap.status in(2,3) and idp.order_status = 4 and idp.withholding_status=3
    # and iap.inv_username=10010 and DATE(iyr.CREATE_DATE)>='%s' and DATE(iyr.CREATE_DATE)<'%s'
    # """ % (start, end)
    # searchDJSWithHoldingFailedCountForAlarm_data = pd.read_sql(searchDJSWithHoldingFailedCountForAlarm_sql, connFq)
    # if len(searchDJSWithHoldingFailedCountForAlarm_data) > 0:
    #     withholdingFailureCount = withholdingFailureCount + searchDJSWithHoldingFailedCountForAlarm_data.iloc[0, 0]

    # searchDJSWithHoldingWaitingCountForAlarm_sql = """
    # select count(iap.hy_application_no) as '代扣等待数' from inv_application_pay iap LEFT JOIN inv_yingyan_application iyp ON iap.hy_application_no = iyp.hy_application_no
    # LEFT JOIN inv_yingyan_repay_plan iyr on iyp.hy_application_no = iyr.TARGET_CODE and iyr.REPAY_PlAN_NO = 1
    # where iap.status in(2,3,4) and iyp.apply_status in(4,5)
    # and DATE(iyr.CREATE_DATE)>='%s' and DATE(iyr.CREATE_DATE)<'%s'
    # and (iyp.withholding_status !=1 and iyp.time_to_live != 0);
    # """ % (start, end)
    # searchDJSWithHoldingWaitingCountForAlarm_data = pd.read_sql(searchDJSWithHoldingWaitingCountForAlarm_sql, connFq)
    # if len(searchDJSWithHoldingWaitingCountForAlarm_data) > 0:
    #     withholdingWaitCount = searchDJSWithHoldingWaitingCountForAlarm_data.iloc[
    #         0, 0]
    # else:
    #     withholdingWaitCount = 0

    searchDJSWithHoldingWaitingCount_sql = """
    select count(iap.hy_application_no) as '代扣等待数' from inv_application_pay iap LEFT JOIN inv_yingyan_application iyp ON iap.hy_application_no = iyp.hy_application_no
    LEFT JOIN inv_yingyan_repay_plan iyr on iyp.hy_application_no = iyr.TARGET_CODE and iyr.REPAY_PlAN_NO = 1
    where iap.status in(2,3,4) and iyp.apply_status in(4,5)
    and DATE(iyr.CREATE_DATE)>='%s' and DATE(iyr.CREATE_DATE)<'%s'
    and iyp.withholding_status =2;
    """ % (start, end)
    searchDJSWithHoldingWaitingCount_data = pd.read_sql(searchDJSWithHoldingWaitingCount_sql, connFq)
    if len(searchDJSWithHoldingWaitingCount_data) > 0:
        withholdingWaitCount = searchDJSWithHoldingWaitingCount_data.iloc[0, 0]

    searchDJSWithHoldingFailedReason_sql = """

        select
    iyp.withhold_reason as 'reason', count(iap.hy_application_no) as 'count'
    from inv_application_pay iap
    LEFT JOIN inv_yingyan_application iyp ON iap.hy_application_no = iyp.hy_application_no
    LEFT JOIN inv_yingyan_repay_plan iyr on iyp.hy_application_no = iyr.TARGET_CODE and iyr.REPAY_PlAN_NO = 1
    where iap.status in(2,3,4)
          and iyr.CREATE_DATE>='%s'
      and iyr.CREATE_DATE <'%s'
          and iyp.apply_status in(4,5) and iyp.withholding_status=3 and iyp.time_to_live = 0
    GROUP BY iyp.withhold_reason
    order by count(iyp.withhold_reason) desc
    """ % (start, end)
    searchDJSWithHoldingFailedReason_data = pd.read_sql(searchDJSWithHoldingFailedReason_sql, connFq)
    withholdingFailureRootCause = append_reasan(searchDJSWithHoldingFailedReason_data)
    # if len(searchDJSWithHoldingFailedReason_data) > 0:
    #     withholdingFailureRootCause = searchDJSWithHoldingFailedReason_data.iloc[0, 0]
    # else:
    #     withholdingFailureRootCause = ""
    withholdingSuccessRate = 0
    if withholdingSuccessCount + withholdingFailureCount != 0:
        withholdingSuccessRate = round(
            (withholdingSuccessCount / (withholdingSuccessCount + withholdingFailureCount)) * 100, 2)

    vipd_three_xe_sql = """
    SELECT ila.hy_limit_amount 当日限额,ila.hy_use_amount 已用限额 FROM inv_limit_amount
    ila WHERE DATE(ila.limit_date)='%s' AND ila.period=3 AND ila.industry_code='VIPD'
    and ila.inv_code='10003'
    """ % start
    MDCP_three_xe_sql = """
    SELECT ila.hy_limit_amount 当日限额,ila.hy_use_amount 已用限额 FROM inv_limit_amount
    ila WHERE DATE(ila.limit_date)='%s' AND ila.period=3 AND ila.industry_code='MDCP'
    and ila.inv_code='10003'
    """ % start
    vipd_three_xe_data = pd.read_sql(vipd_three_xe_sql, connFq)
    MDCP_three_xe_data = pd.read_sql(MDCP_three_xe_sql, connFq)
    limitAmount3VIPD = 0
    useAmount3VIPD = 0
    limitAmount3MDCP = 0
    useAmount3MDCP = 0
    flag = 1
    if len(vipd_three_xe_data) > 0:
        limitAmount3VIPD = vipd_three_xe_data.at[0, '当日限额']
        useAmount3VIPD = vipd_three_xe_data.at[0, '已用限额']
    if len(MDCP_three_xe_data) > 0:
        limitAmount3MDCP = MDCP_three_xe_data.at[0, '当日限额']
        useAmount3MDCP = MDCP_three_xe_data.at[0, '已用限额']

    vipd_6_xe_sql = """
    SELECT ila.hy_limit_amount 当日限额,ila.hy_use_amount 已用限额 FROM inv_limit_amount
    ila WHERE DATE(ila.limit_date)='%s' AND ila.period=6 AND ila.industry_code='VIPD'
    and ila.inv_code='10003'
    """ % start
    MDCP_6_xe_sql = """
    SELECT ila.hy_limit_amount 当日限额,ila.hy_use_amount 已用限额 FROM inv_limit_amount
    ila WHERE DATE(ila.limit_date)='%s' AND ila.period=6 AND ila.industry_code='MDCP'
    and ila.inv_code='10003'
    """ % start
    vipd_6_xe_data = pd.read_sql(vipd_6_xe_sql, connFq)
    MDCP_6_xe_data = pd.read_sql(MDCP_6_xe_sql, connFq)
    limitAmount6MDCP = 0
    useAmount6MDCP = 0
    limitAmount6VIPD = 0
    useAmount6VIPD = 0
    if len(vipd_6_xe_data) > 0 and len(MDCP_6_xe_data) > 0:
        flag = 0
        limitAmount6MDCP = MDCP_6_xe_data.at[0, '当日限额']
        useAmount6MDCP = MDCP_6_xe_data.at[0, '已用限额']
        limitAmount6VIPD = vipd_6_xe_data.at[0, '当日限额']
        useAmount6VIPD = vipd_6_xe_data.at[0, '当日限额']

    # if not withholdingFailureRootCause:
    #     withholdingFailureRootCause = ""
    return {"userName": "盈衍",
            "applyTotalCount": applyTotalCount,
            "applySuccessCount": applySuccessCount,
            "applyFailureCount": applyFailureCount,
            "applyWaitCount": applyWaitCount,
            "applySuccessRate": "%.2f%%" % (applySuccessRate),
            "applyFailureRootCause": getApplyFaildReason(start, end, '10003'),
            "withholdingTotalCount": getWithHoldTotal(start, end, '10003'),
            "withholdingSuccessCount": getWithHoldSuccess(start, end, '10003'),
            "withholdingFailureCount": getWithHoldFaild(start, end, '10003'),
            "withholdingWaitCount": getWithHoldWait(start, end, '10003'),
            "withholdingSuccessRate": "%.2f%%" % (withholdingSuccessRate),
            "withholdingFailureRootCause": withholdingFailureRootCause,
            "flag": 0,
            "limitAmount3MDCP": limitAmount3MDCP / 10000,
            "useAmount3MDCP": useAmount3MDCP / 10000,
            "limitAmount3VIPD": limitAmount3VIPD / 10000,
            "useAmount3VIPD": useAmount3VIPD / 10000,
            "limitAmount6MDCP": limitAmount6MDCP / 10000,
            "useAmount6MDCP": useAmount6MDCP / 10000,
            "limitAmount6VIPD": limitAmount6VIPD / 10000,
            "useAmount6VIPD": useAmount6VIPD / 10000,
            "limitAmount1LDDD": 0.0,
            "useAmount1LDDD": 0.0
            }


def gen_xiaolian(start, end):
    # 笑脸进件申请数
    searchFBApplyCountSQL = '''
    SELECT count(hy_application_no) AS '笑脸进件申请数'
    FROM `inv_application_pay` t
    where t.inv_username=10002
	and t.create_date>='%s' and create_date<'%s'
    ''' % (start, end)

    applyTotalCount = getKPI(searchFBApplyCountSQL)

    # 笑脸进件成功
    searchFBapplySuccessCountSQL = '''
    SELECT count(hy_application_no) AS '笑脸进件等待数'
    FROM `inv_facebank_application` t
    where t.create_date>='%s' and t.create_date<'%s'
    ''' % (start, end)
    applySuccessCount = getKPI(searchFBapplySuccessCountSQL)

    # 笑脸进件失败数
    searchFBapplyFailureCountSQL = '''
    select count(*) from inv_bus_history  t
    where t.inv_username=10002 
    and t.create_date>='%s' and t.create_date<'%s'
    and t.bus_type='1001'
    ''' % (start, end)
    applyFailureCount = getKPI(searchFBapplyFailureCountSQL)

    # 笑脸进件等待数
    searchFBApplyWaitingCountSQL = '''
    SELECT count(hy_application_no)
    FROM `inv_application_pay` t
    where t.`status`=0 and t.inv_username=10002
    and t.create_date>='%s' and t.create_date<'%s'
    ''' % (start, end)
    applyWaitCount = getKPI(searchFBApplyWaitingCountSQL)

    # 笑脸进件失败原因
    # applyFailureRootCauseSQL = '''select t.inv_approval_desc as 'reason', count(t.inv_approval_desc) as 'count' from inv_application_pay t
    # where t.`status` in (11,12)
    # and DATE(t.update_date)>='%s' and DATE(t.update_date)<'%s'
    # and t.inv_username=10002
    # GROUP BY t.inv_approval_desc;''' % (start, end)
    applyFailureRootCause = getApplyFaildReason(start, end, '10002')

    # 成功率
    applySuccessRate=0
    if (applySuccessCount + applyFailureCount) != 0:
        applySuccessRate = round((applySuccessCount / (applySuccessCount + applyFailureCount)) * 100, 2)

    # 成功代扣数
    withholdingSuccessCount = getWithHoldSuccess(start, end, '10002')

    # 失败代扣数
    withholdingFailureCount = getWithHoldFaild(start, end, '10002')

    # 等待代扣数
    withholdingWaitCount = getWithHoldWait(start, end, '10002')

    # 代扣总数
    withholdingTotalCount = getWithHoldTotal(start, end, '10002')
    # withholdingTotalCount=withholdingSuccessCount+withholdingFailureCount

    # 代扣成功率
    withholdingSuccessRate = 0
    if (withholdingTotalCount != 0):
        withholdingSuccessRate = round((withholdingSuccessCount / withholdingTotalCount) * 100, 2)

    searchFBWithHoldingFailedReasonSQL = '''
        SELECT
        ifa.withhold_reason as 'reason', count(ifa.withhold_reason) as 'count'
        FROM
        inv_facebank_application ifa where ifa.hy_application_no in(
        select t.hy_application_no from inv_bus_history t
        where t.bus_type=3002
        and t.create_date>='%s'
        and t.create_date<'%s'
        and t.inv_username= 10002)
        GROUP BY ifa.withhold_reason
        order by count(ifa.withhold_reason) desc
        ''' % (start, end)
    # 代扣失败原因
    withholdingFailureRootCause = getKPI(searchFBWithHoldingFailedReasonSQL)

    limitAmount3VIPD, useAmount3VIPD, limitAmount3MDCP, useAmount3MDCP = getLimitData(start, '10002', '3')
    limitAmount6VIPD, useAmount6VIPD, limitAmount6MDCP, useAmount6MDCP = getLimitData(start, '10002', '6')

    return {"userName": "笑脸",
            "applyTotalCount": applyTotalCount,
            "applySuccessCount": applySuccessCount,
            "applyFailureCount": applyFailureCount,
            "applyWaitCount": applyWaitCount,
            "applySuccessRate": "%.2f%%" % applySuccessRate,
            "applyFailureRootCause": applyFailureRootCause,
            "withholdingTotalCount": withholdingTotalCount,
            "withholdingSuccessCount": withholdingSuccessCount,
            "withholdingFailureCount": withholdingFailureCount,
            "withholdingWaitCount": withholdingWaitCount,
            "withholdingSuccessRate": "%.2f%%" % withholdingSuccessRate,
            "withholdingFailureRootCause": withholdingFailureRootCause,
            "flag": 0,
            "limitAmount3MDCP": limitAmount3MDCP,
            "useAmount3MDCP": useAmount3MDCP,
            "limitAmount3VIPD": limitAmount3VIPD,
            "useAmount3VIPD": useAmount3VIPD,
            "limitAmount6MDCP": limitAmount6MDCP,
            "useAmount6MDCP": useAmount6MDCP,
            "limitAmount6VIPD": limitAmount6VIPD,
            "useAmount6VIPD": useAmount6VIPD,
            "limitAmount1LDDD": 0.0,
            "useAmount1LDDD": 0.0
            }


def gen_dianjinshi(start, end):
    # 进件申请数
    searchFBApplyCountSQL = '''
    select count(t.hy_application_no) from inv_hy_application t
    where t.create_date>='%s'  and t.create_date<'%s' 
    ''' % (start, end)

    applyTotalCount = getKPI(searchFBApplyCountSQL)

    # 进件成功
    searchFBapplySuccessCountSQL = '''
    select count(t.hy_application_no) from inv_hy_application t
    where t.create_date>='%s'  and t.create_date<'%s' 
    ''' % (start, end)
    applySuccessCount = getKPI(searchFBapplySuccessCountSQL)

    # 进件失败数
    searchFBapplyFailureCountSQL = '''
    select count(*) from inv_bus_history  t
    where t.inv_username=10000 and t.create_date>='%s' and t.create_date<'%s'
    and t.bus_type = '1001'
    ''' % (start, end)
    applyFailureCount = 0

    # 进件等待数
    searchFBApplyWaitingCountSQL = '''
    SELECT count(hy_application_no)
    FROM `inv_application_pay` t
    where t.`status`=0 and t.inv_username=10000
    and t.create_date>='%s' and t.create_date<'%s'
    ''' % (start, end)
    applyWaitCount = 0

    # 失败原因
    applyFailureRootCauseSQL = '''select t.inv_approval_desc as 'reason', count(t.inv_approval_desc) as 'count' from inv_application_pay t
    where t.`status` in (11,12)
    and DATE(t.update_date)>='%s' and DATE(t.update_date)<'%s'
    and t.inv_username=10000
    GROUP BY t.inv_approval_desc;''' % (start, end)
    applyFailureRootCause = getKPI(applyFailureRootCauseSQL)

    # 成功率
    applySuccessRate = 0
    if applySuccessCount + applyFailureCount != 0:
        applySuccessRate = round((applySuccessCount / (applySuccessCount + applyFailureCount)) * 100, 2)

    # 成功代扣数
    withholdingSuccessCount = getWithHoldSuccess(start, end, '10000')

    # 失败代扣数
    withholdingFailureCount = getWithHoldFaild(start, end, '10000')

    # 等待代扣数
    withholdingWaitCount = getWithHoldWait(start, end, '10000')

    # 代扣总数
    # withholdingTotalCount=withholdingSuccessCount+withholdingFailureCount
    withholdingTotalCount = withholdingTotalCount = getWithHoldTotal(start, end, '10000')

    # 成功率
    withholdingSuccessRate = 0
    if (withholdingTotalCount != 0):
        withholdingSuccessRate = round((withholdingSuccessCount / withholdingTotalCount) * 100, 2)

    # 代扣失败原因
    withholdingFailureRootCause = getApplyFaildReason(start, end, '10000')

    limitAmount3VIPD, useAmount3VIPD, limitAmount3MDCP, useAmount3MDCP = getDJSLimitData(start, '3')
    limitAmount6VIPD, useAmount6VIPD, limitAmount6MDCP, useAmount6MDCP = getDJSLimitData(start, '6')
    limitAmount1LDDD, useAmount1LDDD = getDJSLimitDataLDDD(start,1)

    return {"userName": "点金石",
            "applyTotalCount": applyTotalCount,
            "applySuccessCount": applySuccessCount,
            "applyFailureCount": applyFailureCount,
            "applyWaitCount": applyWaitCount,
            "applySuccessRate": "%.2f%%" % applySuccessRate,
            "applyFailureRootCause": applyFailureRootCause,
            "withholdingTotalCount": withholdingTotalCount,
            "withholdingSuccessCount": withholdingSuccessCount,
            "withholdingFailureCount": withholdingFailureCount,
            "withholdingWaitCount": withholdingWaitCount,
            "withholdingSuccessRate": "%.2f%%" % withholdingSuccessRate,
            "withholdingFailureRootCause": withholdingFailureRootCause,
            "flag": 0,
            "limitAmount3MDCP": limitAmount3MDCP,
            "useAmount3MDCP": useAmount3MDCP,
            "limitAmount3VIPD": limitAmount3VIPD,
            "useAmount3VIPD": useAmount3VIPD,
            "limitAmount6MDCP": limitAmount6MDCP,
            "useAmount6MDCP": useAmount6MDCP,
            "limitAmount6VIPD": limitAmount6VIPD,
            "useAmount6VIPD": useAmount6VIPD,
            "limitAmount1LDDD": limitAmount1LDDD,
            "useAmount1LDDD": useAmount1LDDD,
            }


def gen(start, end):
    items = []
    items.append(gen_hyc(start, end))
    items.append(gen_YingYan(start, end))
    items.append(gen_xiaolian(start, end))
    items.append(gen_dianjinshi(start, end))
    html = render_template('hicashSummary.html', {"yesterday": start, "items": items})
    html = html.replace('\n', '')

    import yagmail
    subject = '嗨钱对接资金方数据汇总'
    receiverTemp = ['maxiaolei@dpandora.cn']
    receiver = ['richard.liu@hengyuan-finance.com', 'haolikun@dpandora.cn', 'pison.you@hengyuan-finance.com',
                'wangjun@dpandora.cn',
                'jiangmaolin@dpandora.cn', 'gengxiaolin@dpandora.cn',
                'vicky.wan@hengyuan-finance.com', 'lynn.zhang@hengyuan-finance.com',
                'yongneng.yan@hengyuan-finance.com',
                'likui@dpandora.cn', 'wangxiaoqing@dpandora.cn', 'randy.sun@hengyuan-finance.com',
                'charles.lin@hengyuan-finance.com','charles.lin@hengyuan-finance.com']
    cc = ['maxiaolei@dpandora.cn', 'dongdong.zhai@hengyuan-finance.com', 'mazhi@dpandora.cn']
    ccTemp=['maxiaolei@dpandora.cn']
    yag = yagmail.SMTP(user="security@dpandora.cn", password="Jingyong1234", host="smtp.exmail.qq.com", port="465")
    yag.send(to=receiver, subject=subject, contents=html, attachments=None, cc=cc)
    yag.close()


import time

end = time.strftime('%Y-%m-%d', time.localtime(time.time()))
start = time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400))
gen(start, end)

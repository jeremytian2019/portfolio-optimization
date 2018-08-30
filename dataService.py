# encoding: UTF-8

import sys
import json
import copy
from datetime import datetime,date
from datetime import time as dtTime
from time import time, sleep

import pandas as pd
from pandas.core.frame import DataFrame,Series

from pymongo import MongoClient, ASCENDING, DESCENDING

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.app.ctaStrategy.ctaBase import MINUTE_DB_NAME, DAILY_DB_NAME


w = None

try:
    from WindPy import w
except ImportError:
    print u'请先安装WindPy接口'

# 加载配置
config = open('config.json')
setting = json.load(config)

MONGO_HOST = setting['MONGO_HOST']
MONGO_PORT = setting['MONGO_PORT']
SYMBOLS = setting['SYMBOLS']
BGNDATE = setting['BGNDATE']
ENDDATE = setting['ENDDATE']

TRADEDATE_DB_NAME = "VnTrader_TradeDate_Db"


mc = MongoClient(MONGO_HOST, MONGO_PORT)        # Mongo连接
minuteDb = mc[MINUTE_DB_NAME]                   # 分钟线数据库
dailyDb = mc[DAILY_DB_NAME]                     # 日线数据库
dailyDb2 = mc['VnTrader_Daily_Db2']             # 日线数据库2
dailyDb3 = mc['VnTrader_Daily_Db3']             # 日线数据库3
forecastDb = mc['Vntrader_Forecast_Db']


tradeDateDb = mc[TRADEDATE_DB_NAME]

result = w.start()
connected = False

if not result.ErrorCode:
    connected = True
    logContent = u'Wind接口连接成功'
else:
    logContent = u'Wind接口连接失败，错误代码%d' %result.ErrorCode
print logContent


#===Rongbo=============================================================================
class VtPreviewData():
    windcode = ''
    sec_name = ''    
    profitnotice_abstract = ''
    profitnotice_style = ''
    profitnotice_date = None
    profitnotice_change = 0
    profitnotice_lasteps = 0
    profitnotice_netprofitmax = 0
    profitnotice_netprofitmin = 0
    profitnotice_changemax = 0
    profitnotice_changemin = 0
    profitnotice_netsalesmax = 0
    profitnotice_netsalesmin = 0
    profitnotice_netsalesyoymax = 0
    profitnotice_netsalesyoymin = 0
    profitnotice_salesmax = 0
    profitnotice_salesmin = 0
    profitnotice_salesyoymax = 0
    profitnotice_salesyoymin = 0
    qprofitnotice_abstract = 0
    qprofitnotice_style = 0
    qprofitnotice_date = 0
    qprofitnotice_netprofitmax = 0
    qprofitnotice_netprofitmin = 0
    qprofitnotice_changemax = 0
    qprofitnotice_changemin = 0
    qprofitnotice_netsalesmax = 0
    qprofitnotice_netsalesmin = 0
    qprofitnotice_netsalesyoymax = 0
    qprofitnotice_netsalesyoymin = 0
    qprofitnotice_salesmax = 0
    qprofitnotice_salesmin = 0
    qprofitnotice_salesyoymax = 0
    qprofitnotice_salesyoymin = 0
    

class VtForecastData():
    '''业绩快报数据类型'''
    windcode = ''
    sec_name = ''
    performanceexpress_date = None
    performanceexpress_perfexincome = 0
    performanceexpress_perfexprofit = 0
    performanceexpress_perfextotalprofit = 0
    performanceexpress_perfexnetprofittoshareholder = 0
    performanceexpress_perfexepsdiluted = 0
    performanceexpress_perfexroediluted = 0
    performanceexpress_perfextotalassets = 0
    performanceexpress_perfexnetassets = 0
    performanceexpress_or_yoy = 0
    performanceexpress_op_yoy = 0
    performanceexpress_ebt_yoy = 0
    performanceexpress_np_yoy = 0
    performanceexpress_eps_yoy = 0
    performanceexpress_roe_yoy = 0
    performanceexpress_income_ya = 0
    performanceexpress_profit_ya = 0
    performanceexpress_totprofit_ya = 0
    performanceexpress_netprofit_ya = 0
    performanceexpress_eps_ya = 0
    performanceexpress_bps = 0
    performanceexpress_netassets_b = 0
    performanceexpress_bps_b = 0
    performanceexpress_eqy_growth = 0
    performanceexpress_bps_growth = 0
    performanceexpress_totassets_growth = 0

#----------------------------------------------------------------------
def dataMigration():
    """数据迁移，重构数据结构"""
    # dailyDb数据迁移至dailyDb2
    # 通过复权因子序列间接获取日线数据库dailyDb的集合名列表
    print u'dailyDb数据开始迁移至dailyDb2...'
    
    start = time()
    
    mycl = dailyDb['FQYZ']
    myFQYZ = pd.DataFrame(list(mycl.find())) 
    del myFQYZ['_id']
    myFQYZ.set_index('datetime',inplace=True)
    # 迁移全部数据
    collectList = myFQYZ.columns.values
    
    UTC_FORMAT = '%Y-%m-%dT%H:%M:%S' 
    
    # 手动续断点继续迁移
    #collectPosition = collectList.tolist().index('601330SH')
    #collectListTest1 = collectList[collectPosition:]
    
    # 手动迁移指定标的
    #collectListTest2 = ['601330SH','601990SH']    
    
    
    for i in collectList:   # 此处可启用不同的数据迁移方式
        print i
        # 获取数据库某一标的数据   
        clTemp = dailyDb[i[:-2]]
        
        if clTemp:
            mystkDF = pd.DataFrame(list(clTemp.find()))    
            del mystkDF['_id']
            dayList = mystkDF['datetime'].values     
            mystkDF.set_index('datetime',inplace=True)
            mystklength = mystkDF.iloc[:,0].size  
            if mystklength > 0:    
                while (mystklength > 0):
                    try:
                        mystklength = mystklength - 1    
                        
                        dayTemp = str(dayList[mystklength])[0:19]
                      
                        dayfinal= datetime.strptime(dayTemp, UTC_FORMAT)
                         
                        dayStr = pd.to_datetime(str(dayList[mystklength])).strftime('%Y-%m-%d')
                        
                        cl_to_insert = dailyDb2[dayStr]
                        cl_to_insert.ensure_index([('symbol', ASCENDING)], unique=True)         # 添加索引
                        
                        bar = VtBarData()
                        
                        bar.symbol = mystkDF[dayStr]['symbol'].values[0]
                        bar.exchange = mystkDF[dayStr]['exchange'].values[0]
                        bar.vtSymbol = mystkDF[dayStr]['vtSymbol'].values[0]
                        bar.open = mystkDF[dayStr]['open'].values[0]
                        bar.high = mystkDF[dayStr]['high'].values[0]
                        bar.low = mystkDF[dayStr]['low'].values[0]
                        bar.close = mystkDF[dayStr]['close'].values[0]
                        bar.volume = mystkDF[dayStr]['volume'].values[0]
                        bar.datetime = datetime.combine(dayfinal, dtTime.min)
                        bar.date = mystkDF[dayStr]['date'].values[0]
                        bar.time = mystkDF[dayStr]['time'].values[0]
                        
                        if bar.open == None or bar.high == None or bar.low == None or bar.close == None or bar.volume == None:
                            continue
    
                        d = bar.__dict__
                        flt = {'symbol': bar.symbol}
                        cl_to_insert.replace_one(flt, d, True)  
                    except Exception as e:
                        eString = 'Exception:%s'%(e)
                        print eString 
                        continue                       
                    
    end = time()
    cost = (end - start) * 1000
    
    print u'dailyDb数据迁移至dailyDb2完成！Time consuming: %s msec.' %(cost)    
                    
                    
#----------------------------------------------------------------------
def dataMigration2(dayStr):
    """数据迁移，重构数据结构"""
    # dailyDb2数据迁移至dailyDb3
    # 通过交易日序列间接获取日线数据库dailyDb2的集合名列表
    print u'dailyDb2数据开始迁移至dailyDb3...'
    
    start = time()
    
    trdDayList = getAllTrdDate()
    if dayStr in trdDayList:
        trdDayList_sorted = date_sort(trdDayList)
        trdDayPosition = trdDayList_sorted.tolist().index(dayStr)  
        trdDayLastList = trdDayList_sorted[0:(trdDayPosition+1)]

        cl_to_insert = dailyDb3['dailyBar']
        cl_to_insert.ensure_index([('datetime', ASCENDING)], unique=True)          
        
        for i in trdDayLastList:
            print i
            # 获取数据库某一标的数据   
            clTemp = dailyDb2[i]
        
            if clTemp:
                try:
                    mystkDF = pd.DataFrame(list(clTemp.find()))    
                    del mystkDF['_id']
                    stkList = mystkDF['symbol'].values  
                    mystkDF.set_index('symbol',inplace=True)
                    mystklength = mystkDF.iloc[:,0].size  
                    stkDictList = []
                    if mystklength > 0:    
                        while (mystklength > 0):
                            mystklength = mystklength - 1    
                            
                            stkStr = stkList[mystklength]
                        
                            bar = VtBarData()
               
                            bar.symbol = stkStr
                            bar.exchange = mystkDF.loc[stkStr].exchange
                            bar.vtSymbol = mystkDF.loc[stkStr].vtSymbol
                            bar.open = mystkDF.loc[stkStr].open
                            bar.high = mystkDF.loc[stkStr].high
                            bar.low = mystkDF.loc[stkStr].low
                            bar.close = mystkDF.loc[stkStr].close
                            bar.volume = mystkDF.loc[stkStr].volume
                            bar.datetime = mystkDF.loc[stkStr].datetime
                            bar.date = mystkDF.loc[stkStr].date
                            bar.time = mystkDF.loc[stkStr].time
                            
                            if bar.open == None or bar.high == None or bar.low == None or bar.close == None or bar.volume == None:
                                continue
                            
                            d = [stkStr,bar.__dict__]
                            stkDictList.append(d)
                            stkDict = dict(stkDictList)
                    datetimeInt = int(i.split('-')[0] + i.split('-')[1] + i.split('-')[2])
                    d2 = {'datetime':datetimeInt}
                    d3 = {'data':stkDict}
                    d4 = d2.copy()
                    d4.update(d3)
                    flt = {'datetime': datetimeInt}
                    cl_to_insert.replace_one(flt, d4, True)  
                except Exception as e:
                    eString = 'Exception:%s'%(e)
                    print eString 
                    continue                  
                
    end = time()
    cost = (end - start) * 1000               

    print u'dailyDb2数据迁移至dailyDb3成功！Time consuming: %s msec.'%(cost)

#----------------------------------------------------------------------
def dataMigration3(bar):
    """增量更新dailyDb2"""
    dayStr = str(bar.date[0:4] + '-' + bar.date[4:6] + '-' + bar.date[6:8])
    cl_to_insert = dailyDb2[dayStr]
    cl_to_insert.ensure_index([('symbol', ASCENDING)], unique=True)   
    flt = {'symbol':bar.symbol}
    dt = {'$set':{'volume':bar.volume,\
                 'gatewayName':bar.gatewayName,\
                 'exchange':bar.exchange,\
                 'symbol':bar.symbol,\
                 'datetime':bar.datetime,\
                 'high':bar.high,\
                 'rawData':bar.rawData,\
                 'time':bar.time,\
                 'date':bar.date,\
                 'close':bar.close,\
                 'openInterest':bar.openInterest,\
                 'open':bar.open,\
                 'vtSymbol':bar.vtSymbol,\
                 'low':bar.low}}
    cl_to_insert.update(flt,dt,True,True)       

#----------------------------------------------------------------------
def dataMigration4(bar):
    """增量更新dailyDb3"""
    cl_to_insert = dailyDb3['dailyBar']
    cl_to_insert.ensure_index([('datetime', ASCENDING)], unique=True)   
    dayInt = int(bar.date)
    flt = {'datetime':dayInt}
    dt = {'$set':{'data.%s.volume'%(bar.symbol):bar.volume,\
                 'data.%s.gatewayName'%(bar.symbol):bar.gatewayName,\
                 'data.%s.exchange'%(bar.symbol):bar.exchange,\
                 'data.%s.symbol'%(bar.symbol):bar.symbol,\
                 'data.%s.datetime'%(bar.symbol):bar.datetime,\
                 'data.%s.high'%(bar.symbol):bar.high,\
                 'data.%s.rawData'%(bar.symbol):bar.rawData,\
                 'data.%s.time'%(bar.symbol):bar.time,\
                 'data.%s.date'%(bar.symbol):bar.date,\
                 'data.%s.close'%(bar.symbol):bar.close,\
                 'data.%s.openInterest'%(bar.symbol):bar.openInterest,\
                 'data.%s.open'%(bar.symbol):bar.open,\
                 'data.%s.vtSymbol'%(bar.symbol):bar.vtSymbol,\
                 'data.%s.low'%(bar.symbol):bar.low}}
    cl_to_insert.update(flt,dt,True,True)       
                
#----------------------------------------------------------------------
def getDailyBar():  
    """读取数据库dailyDb3里的数据"""
    dailybar = dailyDb3['dailyBar']
    dailybarDF = pd.DataFrame(list(dailybar.find()))
    del dailybarDF['_id']
    dailybarDF.set_index('datetime',inplace=True)
    print dailybarDF
    
#----------------------------------------------------------------------
def delDailyBar(bgnDate,endDate):
    """删除所有标的一段时间的日线数据(慎用)"""
    TraDaysData = w.tdays(bgnDate,endDate)    #获取交易日系列  
    if TraDaysData and TraDaysData.ErrorCode == 0:
        TraDaysList = TraDaysData.Data[0]  
    
    AllStkList = getAllStkCode()   # 获取A股代码
    
    lengthAllStk = len(AllStkList)
    delCount = 0 
    
    print 'Start delete dailybar of AllStock from %s to %s ...'%(bgnDate,endDate)
    if AllStkList:
        for stk in AllStkList:
            stkN = stk.split('.')[0]
            delDailyBarbySymbol2(stkN,TraDaysList)
            delCount += 1
    print 'All stock Num:%i, delete stock Num:%i from %s to %s successfully!'%(lengthAllStk,delCount,bgnDate,endDate)
    
    
#----------------------------------------------------------------------
def delDailyBarbySymbol(symbol,bgnDate,endDate):
    """删除某一标的一段时间的日线数据(慎用)——单独使用"""
    TraDaysData = w.tdays(bgnDate,endDate)    #获取交易日系列  
    if TraDaysData and TraDaysData.ErrorCode == 0:
        TraDaysList = TraDaysData.Data[0] 
        print TraDaysList
        
        try:
            for traday in TraDaysList:
                dailyDb[symbol].delete_one({'datetime':traday}) 
            print 'Delete dailybar data of %s from %s to %s successfully!'%(symbol,bgnDate,endDate)
        except Exception as e:
            print(e)   

#----------------------------------------------------------------------
def delDailyBarbySymbol2(symbol,TraDaysList):
    """删除某一标的一段时间的日线数据(慎用)——删除所有标的使用"""
    try:
        for traday in TraDaysList:
            dailyDb[symbol].delete_one({'datetime':traday}) 
    except Exception as e:
        print(e)   

#----------------------------------------------------------------------
def updateDailyBar(bgnDate,endDate,updateType,upbgnDate,upendDate,synchro):   # updateType:1-覆盖更新，2-手动复权更新; synchro: 1-数据库同步更新，else-只更新数据库VnTrader_Daily_Db
    """检测A股在时间段内是否发生除权除息，进行数据更新（前复权）"""
    
    start = time()
    
    # 读取复权因子序列
    mycl = dailyDb['FQYZ']
    myFQYZ = pd.DataFrame(list(mycl.find())) 
    del myFQYZ['_id']
    myFQYZ.set_index('datetime',inplace=True)
    print myFQYZ
    
    # 对比endDate的复权因子与bgnDate的复权因子是否相同
    length = len(myFQYZ.loc[[endDate]].values[0])
    updateStkList = []
    if length > 0:
        while(length > 0):
            try:
                length = length - 1
                dtFramebgn = myFQYZ.loc[[bgnDate]].values[0][length]
                dtFrameend = myFQYZ.loc[[endDate]].values[0][length] 
                
                if dtFramebgn != dtFrameend:
                    updateStk = [myFQYZ.columns[length],dtFramebgn,dtFrameend]
                    updateStkList.append(updateStk) 
                    print myFQYZ.columns[length]
                    print dtFramebgn
                    print dtFrameend
                    print '-' * 20
            except Exception as e:
                eString = 'Exception:%s'%(e)
                print eString 
                continue            
        print updateStkList
        updateStkLen = len(updateStkList)
        print '%i stock data need to be updated!'%(updateStkLen)
        
    # 如果不相同，则对这个标的进行覆盖更新  
    if updateType == 1:
        print u'开始对标的进行覆盖更新...'
        if updateStkLen > 0:
            while(updateStkLen > 0):
                updateStkLen = updateStkLen - 1
                updateStk = updateStkList[updateStkLen][0]
                print 'update:' + updateStk
                symbol = updateStk[:-2]
                downDailyBarBySymbol(symbol, upbgnDate, upendDate, synchro)
        end = time()
        cost = (end - start) * 1000        
        print u'覆盖更新完成！Time consuming: %s msec.'%(cost)
        
    # 如果不相同，则对这个标的进行手动复权更新
    elif updateType == 2:
        print u'开始对标的进行手动前复权更新...'
        if updateStkLen > 0:
            bgnDatetime = datetime.strptime(bgnDate, '%Y-%m-%d')
            endDatetime = datetime.strptime(endDate, '%Y-%m-%d')
            UTC_FORMAT = '%Y-%m-%dT%H:%M:%S'                
            while(updateStkLen > 0):
                updateStkLen = updateStkLen - 1
                updateStk = updateStkList[updateStkLen][0]
                fqyz_old = updateStkList[updateStkLen][1]
                fqyz_new = updateStkList[updateStkLen][2]
                qianFQ = fqyz_old/fqyz_new
                
                print 'update:' + updateStk  
                symbol = updateStk[:-2]
                
                # 获取数据库某一标的数据,对high low open close进行前复权     
                mystk = dailyDb[symbol]
                if mystk:
                    mystkDF = pd.DataFrame(list(mystk.find()))    
                    del mystkDF['_id']
                    dayList = mystkDF['datetime'].values     
                    mystkDF.set_index('datetime',inplace=True)
                    mystklength = mystkDF.iloc[:,0].size  
                    if mystklength > 0:    
                        while (mystklength > 0):
                            mystklength = mystklength - 1    
                            
                            dayTemp = str(dayList[mystklength])[0:19]
                          
                            dayfinal= datetime.strptime(dayTemp, UTC_FORMAT)
                            
                            if dayfinal < endDatetime:
                                
                                dayStr = pd.to_datetime(str(dayList[mystklength])).strftime('%Y-%m-%d')
                                
                                bar = VtBarData()
                                
                                bar.symbol = mystkDF[dayStr]['symbol'].values[0]
                                bar.exchange = mystkDF[dayStr]['exchange'].values[0]
                                bar.vtSymbol = mystkDF[dayStr]['vtSymbol'].values[0]
                                bar.open = mystkDF[dayStr]['open'].values[0]*qianFQ
                                bar.high = mystkDF[dayStr]['high'].values[0]*qianFQ
                                bar.low = mystkDF[dayStr]['low'].values[0]*qianFQ
                                bar.close = mystkDF[dayStr]['close'].values[0]*qianFQ
                                bar.volume = mystkDF[dayStr]['volume'].values[0]
                                bar.datetime = datetime.combine(dayfinal, dtTime.min)
                                bar.date = mystkDF[dayStr]['date'].values[0]
                                bar.time = mystkDF[dayStr]['time'].values[0]
                                
                                if bar.open == None or bar.high == None or bar.low == None or bar.close == None or bar.volume == None:
                                    continue

                                d = bar.__dict__
                                flt = {'datetime': bar.datetime}
                                mystk.replace_one(flt, d, True)  
                                
                                if synchro == 1:
                                    """增量更新dailyDb2"""
                                    dataMigration3(bar)
                                    """增量更新dailyDb3"""
                                    dataMigration4(bar)
                                else:
                                    continue
                
        end = time()
        cost = (end - start) * 1000        

        print u'手动前复权更新完成！Time consuming: %s msec.'%(cost)
    else: 
        print 'Please enter the right type of updated!'
        
        
#----------------------------------------------------------------------
def getUpdateDay(dayStr):
    '''判断是否是交易日并返回这个交易日及上一个交易日''' 
    trdDayList = getAllTrdDate()
    if dayStr in trdDayList:
        trdDayPosition = trdDayList.tolist().index(dayStr)  
        lastTrdDay = trdDayList[trdDayPosition-1]
        updateDay = [1,dayStr,lastTrdDay]
    else:
        updateDay = [0]
    return updateDay


#----------------------------------------------------------------------
def date_sort(dateList):
    """用冒泡排序进行日期排序"""
    # 耗时长
    try:
        for j in range(len(dateList)-1):
            for i in range(len(dateList)-j-1):
                lower = datetime.strptime(dateList[i], '%Y-%m-%d')
                upper = datetime.strptime(dateList[i+1], '%Y-%m-%d')
                if lower > upper:
                    dateList[i],dateList[i+1] = dateList[i+1],dateList[i]  
        return dateList
    except Exception as e:
        print(e)   


#----------------------------------------------------------------------
def getFQYZ(bgnDate, endDate):
    """下载所有A股的复权因子"""
    
    start = time()
    
    mycl = dailyDb['FQYZ']   # 复权因子
    mycl.ensure_index([('datetime', ASCENDING)], unique=True)         # 添加索引
    
    AllStkList = getAllStkCode()   # 获取A股代码
    
    if AllStkList:
        # 2018-04-01
        data = w.wsd(AllStkList,"adjfactor",bgnDate,endDate)
        
        if data and data.ErrorCode == 0:
            stkCodeList = []
            dateList = []
            lengthCode = len(data.Codes)
            lengthTime = len(data.Times)
            if lengthCode > 0:
                while(lengthCode > 0):
                    lengthCode = lengthCode - 1
                    stkCode = data.Codes[lengthCode]
                    stkCodeList.append(stkCode.split('.')[0] + stkCode.split('.')[1])
            if lengthTime > 0:
                while(lengthTime > 0):
                    lengthTime = lengthTime - 1
                    dateList.append(data.Times[lengthTime].strftime("%Y-%m-%d"))
            
            dataframe = DataFrame(data.Data,index=stkCodeList[::-1])
            dataframeT = dataframe.T
            datatimes = {'datetime':dateList[::-1]}
            datatimesframe = DataFrame(datatimes)
            dataframeF = pd.concat([dataframeT,datatimesframe],axis=1)
            print dataframeF
   
            length = len(dataframeF.index)
            if length > 0:
                while(length > 0):
                    try:
                        length = length - 1
                        dataTemp = dataframeF[length:length+1]
                        d = json.loads(dataTemp.T.to_json()).values()
                        flt = {'datetime':dataframeF.index[length]}
                        mycl.replace_one(flt, d[0], True)
                    except:
                        continue
            end = time()
            cost = (end - start) * 1000            
            print 'Get FQYZ successfully! Time consuming: %s msec.' %(cost)
    
    mycl = dailyDb['FQYZ']
    myFQYZ = pd.DataFrame(list(mycl.find())) 
    del myFQYZ['_id']
    myFQYZ.set_index('datetime',inplace=True)
    print myFQYZ  
    
    
#----------------------------------------------------------------------
def getForecast(bgnDate, endDate):
    """获取所有A股业绩快报数据"""
    
    start = time()
    
    mycl = forecastDb['PreEstimate']   # 业绩快报
    mycl.ensure_index([('datetime', ASCENDING)], unique=True)         # 添加索引
    
    AllStkList = getAllStkList(2)   # 从数据库获取A股代码
    
    if AllStkList:
        # 2018-04-01
        data = w.wsd(AllStkList,"adjfactor",bgnDate,endDate)
        
        if data and data.ErrorCode == 0:
            stkCodeList = []
            dateList = []
            lengthCode = len(data.Codes)
            lengthTime = len(data.Times)
            if lengthCode > 0:
                while(lengthCode > 0):
                    lengthCode = lengthCode - 1
                    stkCode = data.Codes[lengthCode]
                    stkCodeList.append(stkCode.split('.')[0] + stkCode.split('.')[1])
            if lengthTime > 0:
                while(lengthTime > 0):
                    lengthTime = lengthTime - 1
                    dateList.append(data.Times[lengthTime].strftime("%Y-%m-%d"))
            
            dataframe = DataFrame(data.Data,index=stkCodeList[::-1])
            dataframeT = dataframe.T
            datatimes = {'datetime':dateList[::-1]}
            datatimesframe = DataFrame(datatimes)
            dataframeF = pd.concat([dataframeT,datatimesframe],axis=1)
            print dataframeF
   
            length = len(dataframeF.index)
            if length > 0:
                while(length > 0):
                    try:
                        length = length - 1
                        dataTemp = dataframeF[length:length+1]
                        d = json.loads(dataTemp.T.to_json()).values()
                        flt = {'datetime':dataframeF.index[length]}
                        mycl.replace_one(flt, d[0], True)
                    except:
                        continue
            end = time()
            cost = (end - start) * 1000            
            print 'Get FQYZ successfully! Time consuming: %s msec.' %(cost)
    
    mycl = dailyDb['FQYZ']
    myFQYZ = pd.DataFrame(list(mycl.find())) 
    del myFQYZ['_id']
    myFQYZ.set_index('datetime',inplace=True)
    print myFQYZ  
    
    
    """增量更新forecastDb"""
    cl_to_insert = forecastDb['PreEstimate'] 
    cl_to_insert.ensure_index([('datetime', ASCENDING)], unique=True)   
    dayInt = int(bar.date)
    flt = {'datetime':dayInt}
    dt = {'$set':{'data.%s.volume'%(bar.symbol):bar.volume,\
                 'data.%s.gatewayName'%(bar.symbol):bar.gatewayName,\
                 'data.%s.exchange'%(bar.symbol):bar.exchange,\
                 'data.%s.symbol'%(bar.symbol):bar.symbol,\
                 'data.%s.datetime'%(bar.symbol):bar.datetime,\
                 'data.%s.high'%(bar.symbol):bar.high,\
                 'data.%s.rawData'%(bar.symbol):bar.rawData,\
                 'data.%s.time'%(bar.symbol):bar.time,\
                 'data.%s.date'%(bar.symbol):bar.date,\
                 'data.%s.close'%(bar.symbol):bar.close,\
                 'data.%s.openInterest'%(bar.symbol):bar.openInterest,\
                 'data.%s.open'%(bar.symbol):bar.open,\
                 'data.%s.vtSymbol'%(bar.symbol):bar.vtSymbol,\
                 'data.%s.low'%(bar.symbol):bar.low}}
    cl_to_insert.update(flt,dt,True,True)


#----------------------------------------------------------------------
def downForecastBySymbol(symbol, bgnDate, endDate, synchro):   # synchro: 1-数据库同步更新，else-只更新数据库VnTrader_Daily_Db
    """下载某一标的的业绩快报数据"""
    start = time()

    cl = dailyDb[symbol]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)         # 添加索引
    
    exchange = generateExchange(symbol)
    vtSymbol = '.'.join([symbol, exchange])    
    
    fileds = 'open,high,low,close,volume'
    reqStr = 'PriceAdj=F'   #前复权
    baseData = reqWsd(vtSymbol, fileds, bgnDate, endDate, reqStr)      
    
    if baseData and baseData.ErrorCode == 0:
        length = len(baseData.Times)    
        if length > 0:    
            while (length > 0):
                length = length - 1    
        
                bar = VtBarData()
        
                bar.symbol = symbol
                bar.exchange = generateExchange(bar.symbol)
                bar.vtSymbol = '.'.join([bar.symbol, bar.exchange])
                bar.open = baseData.Data[0][length]
                bar.high = baseData.Data[1][length]
                bar.low = baseData.Data[2][length]
                bar.close = baseData.Data[3][length]
                bar.volume = baseData.Data[4][length]
                bar.datetime = datetime.combine(baseData.Times[length], dtTime.min)
                bar.date = bar.datetime.strftime("%Y%m%d")
                bar.time = bar.datetime.strftime("%H:%M:%S")    
                
                if bar.open == None or bar.high == None or bar.low == None or bar.close == None or bar.volume == None:
                    continue
        
                d = bar.__dict__
                flt = {'datetime': bar.datetime}
                cl.replace_one(flt, d, True)   
                
                if synchro == 1:
                    """增量更新dailyDb2"""
                    dataMigration3(bar)
                    """增量更新dailyDb3"""
                    dataMigration4(bar)
                else:
                    continue

    end = time()
    cost = (end - start) * 1000

    print u'合约%s数据下载完成%s - %s，耗时%s毫秒' %(symbol, baseData.Times[0], baseData.Times[-1], cost)
    

#----------------------------------------------------------------------
def getAllStkCode():
    """这个函数真的只是获取最新的所有A股代码(从万得)"""
    dateStr = datetime.now().strftime('%Y-%m-%d')
    reqSymbol = 'sectorconstituent'
    reqStr = 'date=%s;windcode=881001.WI'%(dateStr)
    
    data = reqWset(reqSymbol, reqStr)
    
    if data and data.ErrorCode == 0:
        AllStkList = data.Data[1]
        print 'get all stockcode successfully!'
        return AllStkList
    

#----------------------------------------------------------------------
def getAllStkList(returnType):
    """获取最新的所有A股代码(从数据库)"""
    try:
        cl = tradeDateDb['stkinfo']
        stkInfoDf = pd.DataFrame(list(cl.find().sort('vtSymbol',ASCENDING)))
        del stkInfoDf['_id']
        stkSymbol = stkInfoDf['symbol'].values.tolist()
        stkVtSymbol = stkInfoDf['vtSymbol'].values.tolist()
        stkName = stkInfoDf['stkName'].values.tolist()
        sktSymbolName = [(k,v) for k,v in zip(stkSymbol,stkName)]
        sktVtSymbolName = [(k,v) for k,v in zip(stkVtSymbol,stkName)]
        if returnType == 1:
            return stkSymbol
        elif returnType == 2:
            return stkVtSymbol
        elif returnType == 3:
            return sktSymbolName
        elif returnType == 4:
            return sktVtSymbolName
        else:
            print 'please enter the right returnType!'
            return None
    except Exception as e:
        print(e)
        return None
        
        
#----------------------------------------------------------------------
def getAllTrdDate():
    """从数据库读取所有的交易日信息"""
    try:
        cl = tradeDateDb['tradedate']
        # 为了方便后续使用，这里的交易日序列进行了排序
        tradeDateDf = pd.DataFrame(list(cl.find().sort('tradedate',ASCENDING))) 
        del tradeDateDf['_id']
        dayList = tradeDateDf['datestr'].values 
        return dayList
    except Exception as e:
        print(e)    

#======================================================================================






#----------------------------------------------------------------------
def generateExchange(symbol):
    """生成VT合约代码"""
    if symbol[0:2] in ['60', '51']:
        exchange = 'SH'
    elif symbol[0:2] in ['00', '15', '30']:
        exchange = 'SZ'
    elif symbol[0:2] in ['IF', 'IC', 'IH']:
        exchange = 'CFE'    
    return exchange


#----------------------------------------------------------------------
def downDailyBarBySymbol(symbol, bgnDate, endDate, synchro):   # synchro: 1-数据库同步更新，else-只更新数据库VnTrader_Daily_Db
    """下载某一标的的日线数据"""
    start = time()

    cl = dailyDb[symbol]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)         # 添加索引
    
    exchange = generateExchange(symbol)
    vtSymbol = '.'.join([symbol, exchange])    
    
    fileds = 'open,high,low,close,volume'
    reqStr = 'PriceAdj=F'   #前复权
    baseData = reqWsd(vtSymbol, fileds, bgnDate, endDate, reqStr)      
    
    if baseData and baseData.ErrorCode == 0:
        length = len(baseData.Times)    
        if length > 0:    
            while (length > 0):
                length = length - 1    
        
                bar = VtBarData()
        
                bar.symbol = symbol
                bar.exchange = generateExchange(bar.symbol)
                bar.vtSymbol = '.'.join([bar.symbol, bar.exchange])
                bar.open = baseData.Data[0][length]
                bar.high = baseData.Data[1][length]
                bar.low = baseData.Data[2][length]
                bar.close = baseData.Data[3][length]
                bar.volume = baseData.Data[4][length]
                bar.datetime = datetime.combine(baseData.Times[length], dtTime.min)
                bar.date = bar.datetime.strftime("%Y%m%d")
                bar.time = bar.datetime.strftime("%H:%M:%S")    
                
                if bar.open == None or bar.high == None or bar.low == None or bar.close == None or bar.volume == None:
                    continue
        
                d = bar.__dict__
                flt = {'datetime': bar.datetime}
                cl.replace_one(flt, d, True)   
                
                if synchro == 1:
                    """增量更新dailyDb2"""
                    dataMigration3(bar)
                    """增量更新dailyDb3"""
                    dataMigration4(bar)
                else:
                    continue

    end = time()
    cost = (end - start) * 1000

    print u'合约%s数据下载完成%s - %s，耗时%s毫秒' %(symbol, baseData.Times[0], baseData.Times[-1], cost)
    
    
#----------------------------------------------------------------------
def downMinuteBarBySymbol(symbol):
    """下载某一标的的分钟线数据"""
    start = time()

    cl = minuteDb[symbol]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)         # 添加索引
    
    exchange = generateExchange(symbol)
    vtSymbol = '.'.join([symbol, exchange])    
    
    fileds = 'open,high,low,close,volume'
    reqStr = 'PriceAdj=F'
    baseData = reqWsi(vtSymbol, fileds, BGNDATE, ENDDATE, reqStr)      
    
    if baseData and baseData.ErrorCode == 0:
        length = len(baseData.Times)    
        if length > 0:    
            while (length > 0):
                length = length - 1    
        
                bar = VtBarData()
        
                bar.symbol = symbol
                bar.exchange = generateExchange(bar.symbol)
                bar.vtSymbol = '.'.join([bar.symbol, bar.exchange])
                bar.open = baseData.Data[0][length]
                bar.high = baseData.Data[1][length]
                bar.low = baseData.Data[2][length]
                bar.close = baseData.Data[3][length]
                bar.volume = baseData.Data[4][length]
                bar.datetime = baseData.Times[length]
                bar.date = bar.datetime.strftime("%Y%m%d")
                bar.time = bar.datetime.strftime("%H:%M:%S")              
        
                d = bar.__dict__
                flt = {'datetime': bar.datetime}
                cl.replace_one(flt, d, True)            

    end = time()
    cost = (end - start) * 1000

    print u'合约%s数据下载完成%s - %s，耗时%s毫秒' %(symbol, baseData.Times[0], baseData.Times[-1], cost)

    
#----------------------------------------------------------------------
def downloadAllDailyBar(bgnDate, endDate):
    """下载所有配置中的合约的分钟线数据"""
    print '-' * 50
    print u'开始下载标的分钟线数据'
    print '-' * 50
    
    # 添加下载任务
    for symbol in SYMBOLS:
        downDailyBarBySymbol(str(symbol), )
    
    print '-' * 50
    print u'标的分钟线数据下载完成'
    print '-' * 50
    
#----------------------------------------------------------------------
def downloadAllMinuteBar():
    """下载所有配置中的合约的分钟线数据"""
    print '-' * 50
    print u'开始下载标的分钟线数据'
    print '-' * 50
    
    # 添加下载任务
    for symbol in SYMBOLS:
        downMinuteBarBySymbol(str(symbol))
    
    print '-' * 50
    print u'标的分钟线数据下载完成'
    print '-' * 50
    
 
#----------------------------------------------------------------------
def downloadAllStk():
    """下载所有的股票代码"""
    cl = tradeDateDb['stkinfo']
    cl.ensure_index([('vtSymbol', ASCENDING)], unique=True)         # 添加索引
    
    dateStr = datetime.now().strftime('%Y-%m-%d')
    reqSymbol = 'sectorconstituent'
    reqStr = 'date=%s;windcode=881001.WI'%(dateStr)
    
    data = reqWset(reqSymbol, reqStr)
    
    if data and data.ErrorCode == 0:
        length = len(data.Data[0])    
        if length > 0:    
            while (length > 0):
                length = length - 1
                
                vtSymbol = data.Data[1][length]
                symbol = vtSymbol.split('.')[0]    
                uptDate = data.Data[0][length]
                stkName = data.Data[2][length]
                
                stkInfo = {}
                stkInfo['uptDate'] = uptDate
                stkInfo['symbol'] = symbol
                stkInfo['vtSymbol'] = vtSymbol
                stkInfo['stkName'] = stkName
                
                flt = {'vtSymbol' : vtSymbol}     
                
                cl.replace_one(flt, stkInfo, True)    
    
        print 'Download all stock info successfully!'
#----------------------------------------------------------------------
def downloadAllStkDailyBar(bgnDate, endDate, synchro):   # synchro: 1-数据库同步更新，else-只更新数据库VnTrader_Daily_Db
    """获取所有股票日线行情"""
    dateStr = datetime.now().strftime('%Y-%m-%d')
    reqSymbol = 'sectorconstituent'
    reqStr = 'date=%s;windcode=881001.WI'%(dateStr)
    
    data = reqWset(reqSymbol, reqStr)
    
    if data and data.ErrorCode == 0:
        length = len(data.Data[0])    
        if length > 0:    
            while (length > 0):
                length = length - 1
                
                vtSymbol = data.Data[1][length]
                symbol = vtSymbol.split('.')[0]
                downDailyBarBySymbol(symbol, bgnDate, endDate, synchro)
        print 'Download all stock daily bar from %s to %s successfuly!'%(bgnDate,endDate)
                
#----------------------------------------------------------------------
def downloadTrdDays(bgnDate, endDate):
    """下载交易日"""
    cl = tradeDateDb['tradedate']
    cl.ensure_index([('tradedate', ASCENDING)], unique=True)         # 添加索引
    
    baseData = w.tdays(bgnDate, endDate, "")
    
    if baseData and baseData.ErrorCode == 0:
        length = len(baseData.Times)    
        if length > 0:    
            while (length > 0):
                length = length - 1
                
                d = {'tradedate' : baseData.Data[0][length], 'datestr' : baseData.Data[0][length].strftime('%Y-%m-%d')}
                flt = {'tradedate' : copy.copy(baseData.Data[0][length])}     
                
                cl.replace_one(flt, d, True)    
    
    
#----------------------------------------------------------------------
def reqWsd(symbol, reqFields, bgnTime, endTime, reqStr):
    """查询历史行情数据"""
    if connected:
        data = w.wsd(symbol, reqFields, bgnTime, endTime, reqStr)
    
        return data
    else:
        return None

#----------------------------------------------------------------------
def reqWsdRelative(symbol, reqFields, relativeDate, baseDate, reqStr):
    """查询历史行情数据"""
    if connected:
        data = w.wsd(symbol, reqFields, relativeDate, baseDate, reqStr)
    
        return data   
    else:
        return None
    
    
#----------------------------------------------------------------------
def reqWsi(symbol, reqFields, bgnTime, endTime, reqStr):
    """查询历史行情数据"""
    if connected:
        data = w.wsi(symbol, reqFields, bgnTime, endTime, reqStr)
    
        return data
    else:
        return None
    
#----------------------------------------------------------------------
def reqWset(symbol, reqStr):
    """查询历史行情数据"""
    if connected:
        data = w.wset(symbol, reqStr)
    
        return data
    else:
        return None
    
#----------------------------------------------------------------------
def getLastTrdDate(offset, baseDate):
    """查询上一个交易日"""
    data = w.tdaysoffset(offset, baseDate, '')
            
    targetDate = ''
    length = len(data.Codes)
    if length > 0 and data.ErrorCode == 0:
        targetDate = data.Data[0][0]    
    
    return targetDate

    
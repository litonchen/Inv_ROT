#bilibili github
if __name__ == '__main__':
    from pandasql import sqldf
    import pandas as pd
    import tushare as ts
    import requests
    import time,datetime
    pro = ts.pro_api()
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    data=sqldf('select *,(lower(substr(ts_code,8,2)) || substr(ts_code,1,6))  api_tick from data')
    data=data.append(pd.DataFrame({'api_tick':['sz399001','sz399006','sh000001']}))
    sina_api='http://hq.sinajs.cn/?format=json&list={0}'
    update=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("start:",datetime.datetime.now())
    for tick in data['api_tick'].unique():
        current={   'name':[],'tick':[],'trade_date':[],'Open':[],'High':[],'Low':[],'pre_close':[],'Volume':[],'Amt':[], 'date':[] ,'upd':[]   }    
    #            content=requests.get('http://hq.sinajs.cn/?format=json&list=sh688005').text
        content=requests.get(sina_api.format(tick)).text

        list = content.split(',')
        current['name']=list[0][list[0].find("=")+1:30]
        current['tick']=tick
        current['Open']=list[1]
        current['pre_close']=list[2] #上一日收盘价
        current['Close'] =list[3]
        current['High']=list[4]
        current['Low']=list[5]
        current['trade_date']=list[30].replace("-","")
        current['Volume']=float(list[8])/100
        current['Amt']=float(list[9])/10000
        current['date']=list[30]+" "+list[31]
        current['pct_chg']=round(((float(list[3])-float(list[2]))/float(list[2]))*100,2)
        current['upd']=update
        if tick == data['api_tick'].unique()[0]:  
            p_update=pd.DataFrame(current,index=[0])
        else:
            p_update=p_update.append(pd.DataFrame(current,index=[0]))
    print("finish:",datetime.datetime.now(),"   len:",len(p_update))

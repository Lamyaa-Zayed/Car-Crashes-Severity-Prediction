import pandas as pd 
import datetime


#Remove the duplicate from the weather
def StartCleanupWeather(df_weather):
    df_weather.drop_duplicates(subset=['Year', 'Day','Month','Hour'], keep='last',inplace=True)
    df_weather['Full Date']=df_weather['Year'].apply(lambda x:str(x)+'-')+df_weather['Month'].apply(lambda x:str(x)+'-')+df_weather['Day'].apply(lambda x:str(x)+' ')+df_weather['Hour'].apply(lambda x:str(x)+':0:0')
    #df_weather["Full Date"]=df_weather['Year']+'-'+df_weather['Day']+'-'+df_weather['Month']+' '+df_weather['Hour'])+':0:0'
    lmb=lambda x:datetime.datetime.timestamp(datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df_weather["Full Date With H"]=df_weather["Full Date"].apply(lmb)
   
def ParseTime(time):
    #remove dots from the date
    date=datetime.datetime.strptime(time.split('.')[0], '%Y-%m-%d %H:%M:%S')
    date = date.replace(second=0, minute=0)
    return datetime.datetime.timestamp(date)

def ParseTimeWithouthours(time):
    #remove dots from the date
    date=datetime.datetime.strptime(time.split('.')[0], '%Y-%m-%d %H:%M:%S')
    date = date.replace(second=0, minute=0,hour=0)
    return datetime.datetime.timestamp(date)

def ParseHolidayTimeWithouthours(time):
    date=datetime.datetime.strptime(time, '%Y-%m-%d')
    date = date.replace(second=0, minute=0,hour=0)
    return datetime.datetime.timestamp(date)

def StartCleanupTrain(df_train):
    df_train["Full Date With H"]=df_train["timestamp"].apply(ParseTime)
    df_train["Full Date Without H"]=df_train["timestamp"].apply(ParseTimeWithouthours)
    #remove duplicates based on the position and time
    df_train.drop_duplicates(subset=['Lat', 'Lng','Full Date With H'], keep='last',inplace=True)
    
def StartCleanupHoliday(df_holidays):
    df_holidays["Full Date Without H"]=df_holidays["date"].apply(ParseHolidayTimeWithouthours)
    df_holidays.drop(["date","description"],axis=1,inplace=True)
    f= lambda x:True
    df_holidays['Is Holiday']=True
def FillOFFDays(df):
    for i,row in df.iterrows():

        date=datetime.datetime.strptime(row['timestamp'].split('.')[0], '%Y-%m-%d %H:%M:%S')
        if (date.strftime("%A")=='Saturday' or date.strftime("%A")=='Sunday') :
            df.at[i,'Is Holiday']=True
        elif row['Is Holiday']!=True:
            df.at[i,'Is Holiday']=False

     
def StartProcess(weatherfilePath,trainfilePath,holidayfilePath):
    df_weather = pd.read_csv(weatherfilePath)
    StartCleanupWeather(df_weather)
    df_weather.to_csv('weatherUpdated.csv')
    df_train = pd.read_csv(trainfilePath)
    StartCleanupTrain(df_train)
    df_train.to_csv('trainUpdated2.csv')
    #Merge based on the time stamp
    df_mergedata=pd.merge(df_train,df_weather,how='left',on=['Full Date With H'])
    df_mergedata.to_csv('merged.csv')
    df_holiday = pd.read_csv(holidayfilePath)
    StartCleanupHoliday(df_holiday)
    df_holiday.to_csv('holidayUpdated.csv')
    df_mergedata2=pd.merge(df_mergedata,df_holiday,how='left',on=['Full Date Without H'])
    df_mergedata2.to_csv('merged2.csv')
    FillOFFDays(df_mergedata2)
    df_mergedata2.to_csv('merged2WithOFFdays.csv')
    return df_mergedata2
def FillData(df):
    pass

df_merged=StartProcess('weather-sfcsv.csv','train.csv','holidays.csv')




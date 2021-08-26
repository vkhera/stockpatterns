import yfinance as yf
import pandas as pd
from matplotlib import pyplot
import matplotlib as mpl
import numpy as np

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
          'November', 'December']
def addPct(dict):
    X = dict['Date']
    Y = dict['LowValue']
    Z = []
    Z.append(0)
    numOfRows = len(X)
    row=1
    while row < numOfRows:
        Z.append(round(((Y[row] - Y[row - 1])/Y[row - 1])*100,2))
        row +=1
    return  {'Date': X, 'LowValue': Y, 'MoM-Pct' : Z}


def cleanUp(length,timeArray,lowValArray):
    Y1=[]
    X1=[]
    lowVal = 0
    while lowVal < length:
        if lowValArray[lowVal] > 1:
            X1.append(timeArray[lowVal])
            Y1.append(round(lowValArray[lowVal],2))
        lowVal += 1


    return {'Date': X1, 'LowValue': Y1}


def monthlyPattern(dict):
    dateList = dict['Date']
    momList = dict['MoM-Pct']
    numOfRows = len(dateList)
    month = []
    monthlyAverage = []
    numOfTwelveList = []
    rowCal = 0
    while rowCal < 12:
        print(f'Month count is {rowCal}')
        monthTotal = 0
        rowArr = 0
        numOfTwelves = 0
        while rowArr < numOfRows:
            if (rowCal + rowArr) < numOfRows:
                monthTotal += momList[rowCal + rowArr]
                numOfTwelves +=1
            rowArr +=12
        monthlyAverage.append(monthTotal/numOfTwelves)
        print(f'Number of tweleves are {numOfTwelves} and total is {monthTotal}')
        month.append(pd.to_datetime(dateList[rowCal]).month_name())
        numOfTwelveList.append(numOfTwelves)
        rowCal +=1
    return {'Month': month, 'Average': monthlyAverage,'NumberOfItems':numOfTwelveList}

def getMonthlySplit(dict):
    dateList = dict['Date']
    momList = dict['MoM-Pct']
    numOfRows = len(dateList)
    month = []
    monthlyAverage = []
    rowCal = 0
    final = []
    while rowCal < 12:
        print(f'Month count is {rowCal}')
        monthlyData = []
        year = []
        monthTotal = 0
        rowArr = 0
        numOfTwelves = 0
        while rowArr < numOfRows:
            if (rowCal + rowArr) < numOfRows:
                monthlyData.append(momList[rowCal + rowArr])
                year.append(pd.to_datetime(dateList[rowCal + rowArr]).year)
                numOfTwelves +=1
            rowArr +=12
        monthlyAverage.append(monthTotal/numOfTwelves)
#        print(f'Number of tweleves are {numOfTwelves} and total is {monthTotal}')
        month.append(pd.to_datetime(dateList[rowCal]).month_name())
        final.append(pd.to_datetime(dateList[rowCal]).month_name())
        final.append(year)
        final.append(monthlyData)
        rowCal +=1
    print(final)
    return final

# create a differenced series
def getMonthlyData(stockCode):
    try:
        google = yf.Ticker(stockCode)
    #    df = google.history(period='max', interval="1mo")[['Low']]
        df = google.history(start='2000-01-01', end='2021-01-01', interval="1mo")[['Low']]
    #    df['date'] = pd.to_datetime(df.index).time
        print(df)
        print("---------------------------------")
        X = df.index.values
        Y = df['Low'].values
        dict = cleanUp(len(df),X,Y)
        dict = addPct(dict)
        dictMom = monthlyPattern(dict)
        getMonthlySplit(dict)
    except:
        dictMom = {'Month': months, 'Average': np.zeros(12),'NumberOfItems':np.zeros(12)}
    df = pd.DataFrame(dictMom)
#    df.to_csv(stockCode + '-dataAverage.csv', index=False)
    return {'code':stockCode , 'frame':df}

def showMonthlySingle(stockList,monthlyData):
    np.random.seed(100)
    mycolors = np.random.choice(list(mpl.colors.XKCD_COLORS.keys()), len(codes), replace=False)
    iterator = 0
    months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
              'November', 'December']
    pyplot.figure(figsize=(16, 5), dpi=100)
    for data in monthlyData:
        pyplot.plot(months, data['frame']['Average'], color=mycolors[iterator], label=code)
        iterator += 1
    pyplot.xlabel("Month")
    pyplot.ylabel("Average")
    pyplot.title("Month over month return")
    pyplot.legend()
    #pyplot.gca().set(title='Over Time', xlabel='Month', ylabel='Percentage')
    pyplot.show()

def showMonthlyMultiple(stockList,monthlyData):
    np.random.seed(100)
    mycolors = np.random.choice(list(mpl.colors.XKCD_COLORS.keys()), len(stockList), replace=False)
    iterator = 0
    pyplot.figure(figsize=(18, 5), dpi=80)
    if len(stockList)%2 > 0 :
        columns = (len(stockList)+1)/2
    for data in monthlyData:
        pyplot.subplot(2, columns, 1+iterator)
        lbl = data['code']+'-'+str(data['frame']['NumberOfItems'][0])
        pyplot.plot(months, data['frame']['Average'], color=mycolors[iterator], label=lbl)
        pyplot.title(lbl)
        iterator += 1
    pyplot.tight_layout()
#    pyplot.legend()
    #pyplot.gca().set(title='Over Time', xlabel='Month', ylabel='Percentage')
    pyplot.show()

#codes = ['XLI','XLE','XLV','XLU','XLF','XLK','XLC','XLB','XLP','XLRE']
codes = ['EWU','EWQ','INDA']
monthlyData = []
for code in codes:
    monthlyData.append(getMonthlyData(code))
showMonthlyMultiple(codes,monthlyData)

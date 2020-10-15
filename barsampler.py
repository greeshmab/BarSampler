import pandas as pd

def preProcessDataintoCSV(filepath):
    """
        Given a TAQ Text file path covert it into 
        pipe seperated CSV file with the same name.
        
	Parameters
	----------
	filepath: str
	    Path for the input flatfile extracted from Gz.
        
        Returns
        -------
        None.

    
    """
    with open(filepath, 'r') as filename:
        filedata = filename.readlines()
        filedata.pop()
        with open(filepath+'.csv', 'w') as filenamecsv:
            for i in range(len(filedata)):
            #filenamecsv.write(filedata[i])
                #if(i%10000==0):
                filenamecsv.write(filedata[i])
    return

def getTicksDataForaSymbol(data, symbol):
    """
        Given a dataFrame and a symbol, filter out the trades for 
        the symbol.
        
        Parameters
        ----------
        data : str
            DataFrame of the input trade data.
        symbol : str
            Symbol for the ticks to be filtered from the input data.
        
        Returns
        -------
        pandas.core.frame.DataFrame
            Dataframe with trades for the symbol.
            
        
    """
    return data[data['Symbol']==symbol]

def deleteTradeType(data, tradeType):
    """
        Given a dataFrame and a symbol, filter out the trades for 
        the tradeType.
        
        Parameters
        ----------
        data : str
            DataFrame of the input trade data.
        tradeType : str
            TradeType for the ticks to be filtered from the input data.
        
        Returns
        -------
        pandas.core.frame.DataFrame
            Dataframe with trades for the given tradeType (Sale condition).


    """
    return data[~data['Sale Condition'].str.contains(tradeType)]

def reIndextoTime(data):
    """
        Given a dataFrame, convert time col from 
        string to Pandas date time and set it as Index.
        
        Parameters
        ----------
        data : str
            DataFrame of the input trade data.
        
        Returns
        -------
        pandas.core.frame.DataFrame
            Dataframe with trades for the given tradeType (Sale condition).
            
      
    """
    data["Time"] = pd.to_datetime(data["Time"], format='%H%M%S%f')
    data = data.set_index("Time")
    data.index.names = ["Date_Time"]
    return data
    
def minuteBarsResampler(data, units = 'T', size = 20, volume = False):
    """
        Given a dataFrame, and time units, resample into time bars.
        
        Parameters
        ----------
        data : str
            DataFrame of the input trade data.
        units : str
            String 'T'(default) for minutes, 'S' for seconds.
        size : int
            Size of the time 20 mins(default).
        volume : bool
            If true Outputs the cumulative Vol along with OHLV.
        
        Returns
        -------
        pandas.core.frame.DataFrame
            OHLCV data for given time period bars.
            
            
            
    """
    # add this to remove outside trading hours
    #data = data[(data.index >= '1900-01-01 09:30:00.0') & (data.index <= '1900-01-01 16:30:00.0')]
    time = str(size)+units
    if(volume==False):
        return data['Trade Price'].resample(time).ohlc()
    else:
        return data['Trade Price'].resample(time).ohlc().merge(
            data['Trade Volume'].resample(time).sum(), left_on='Date_Time', right_on='Date_Time')
    
def tickResampler(data, size = 15, volume = False):
    """
        Given a dataFrame, and tick size, resample into tick bars.
        
        Parameters
        ----------
        data : str
            DataFrame of the input trade data.
        size : int
            Size of the ticks (15 default).
        volume : bool
            If true Outputs the cumulative Vol along with OHLV.
        
        Returns
        -------
        pandas.core.frame.DataFrame
            OHLCV data for given tick bar size.
            

    """
    # add this to remove outside trading hours
    #data = data[(data.index >= '1900-01-01 09:30:00.0') & (data.index <= '1900-01-01 16:30:00.0')]
    start = 0 
    ohlc = []
    while(start<data.shape[0]):
        dftemp = data['Trade Price'][start:start+size].to_list()
        ohlc.append([dftemp[0], max(dftemp), min(dftemp), dftemp[-1], sum(data['Trade Volume'][start:start+size])])
        start = start+size
    OHLCV = pd.DataFrame(ohlc, columns=['Open', 'High', 'Low', 'Close', 'Vol'])

    if(volume==True):
        return OHLCV
    else: 
        return OHLCV[['Open', 'High', 'Low', 'Close']]
    
def volResampler(data, size = 100):
    """
        Given a dataFrame, and Vol units, resample into Volume bars.
        
        Parameters
        ----------
        data : str
            DataFrame of the input trade data.
        size : int
            Size of the Volume 100 (default).

        Returns
        -------
        pandas.core.frame.DataFrame
            OHLCV data for given volume period bars.
            

    """
    # add this to remove outside trading hours
    #data = data[(data.index >= '1900-01-01 09:30:00.0') & (data.index <= '1900-01-01 16:30:00.0')]
    start = 0
    end = 0
    ohlc = []
    tempsum = 0 
    for i in range(data.shape[0]):
        if(tempsum+data['Trade Volume'][i]<size):
            tempsum += data['Trade Volume'][i]
        else:
            end = i+1
            dftemp = data['Trade Price'][start:end].to_list()
            ohlc.append([dftemp[0], max(dftemp), min(dftemp), dftemp[-1], sum(data['Trade Volume'][start:end])])
            start = i+1
            tempsum = 0 
    OHLCV = pd.DataFrame(ohlc, columns=['Open', 'High', 'Low', 'Close', 'Vol'])     
    return OHLCV

def dollarResampler(data, size = 10000):
    """
        Given a dataFrame, and Dollar units, resample into Dollar bars.
        
        Parameters
        ----------
        data : str
            DataFrame of the input trade data.
        size : int
            Size of the Dollar for bars, 100 (default).
        
        Returns
        -------
        pandas.core.frame.DataFrame
            OHLCV data for given Dollar period bars.
            
            
    """
    # add this to remove outside trading hours
    #data = data[(data.index >= '1900-01-01 09:30:00.0') & (data.index <= '1900-01-01 16:30:00.0')]
    start = 0
    ohlcvd = []
    tempDollarSum = 0 
    for i in range(data.shape[0]):
        tempDollar = data['Trade Volume'][i]*data['Trade Price'][i]
        if(tempDollarSum + tempDollar<size):
            tempDollarSum += tempDollar
        else:
            dftemp = data['Trade Price'][start:i+1].to_list()
            ohlcvd.append([dftemp[0], max(dftemp), min(dftemp), dftemp[-1], 
                           sum(data['Trade Volume'][start:i+1]), sum(data['Trade Volume'][start:i+1]*data['Trade Price'][start:i+1])])
            start = i+1
            tempDollarSum = 0 
    OHLCVD = pd.DataFrame(ohlcvd, columns=['Open', 'High', 'Low', 'Close', 'Vol', 'dollar'])     
    return OHLCVD

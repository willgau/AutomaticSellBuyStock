import os
import asyncio
import aiohttp
import redis.asyncio as redis
from alpaca_trade_api import REST, TimeFrame  # Assuming Alpaca has an async stream API
import threading
import yfinance as yf
import pandas as pd
import time
import datetime

'''
    ALPACA API SECTION
'''
ALPACA_API_KEY = os.getenv('ALPACA_API_KEY')
ALPACA_SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')
ALPACA_BASE_URL = os.getenv('ALPACA_BASE_URL', 'https://paper-api.alpaca.markets')


'''
    GLOBAL INSTANCE
'''
# Alpaca API 
alpaca = REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, 'https://paper-api.alpaca.markets')
# Thread Even Handler
stop_event = threading.Event()
# Shared data with stocks name and price
shared_data = {"NVDA": float('-inf')}
#Logging
LOG = False

'''
    PORFOLIO CLASS
'''
# This class hold various information for this program 
# such as the current cash, logging info, stocks info, etc.
class Porfolio:
    def __init__(self, name="Will", cash=0.0):
        self.mName = name
        self.mCash = cash
        self.mQuantity = {}
        self.mOrder = []
        self.logPorfolio() 
        self.mMainExecutionTime = 0.0
        self.mOtherExecutionTime = 0.0
        
    def logPorfolio(self):
        if True:
            print(f"Porfolio Owner: ", self.mName)
            print(f"Available capital: ${self.mCash:,.2f}")
            print(f"Quantity of stock to buy: ", self.mQuantity)
        
    def cash(self):
        return self.mCash
        
    def setCash(self, iAmount):
        self.mCash = iAmount
        
    def updateQuantity(self):
        for key, value in shared_data.items():
            self.mQuantity[key] = int(self.mCash // value)
    
    def getSpecificStockQuantity(self, key):
        if key in self.mQuantity:
            return self.mQuantity[key]
        return 0
    
    def setOrderBook(self,order):
        # (order, price)
        self.mOrder.append((order,shared_data[order.symbol]))
        
     # First order that is executed
     # in this case, the first one (NVDA)
    def getMainOrder(self):
        if not len(self.mOrder) == 0:
            return self.mOrder[0]
        return None
        
    def setMainExecutionTime(self, iTime):
        self.mMainExecutionTime = iTime
        
    def getMainExecutionTime(self):
        return self.mMainExecutionTime
        
    def setOthernExecutionTime(self, iTime):
        self.mOtherExecutionTime = iTime
        
    def getOtherExecutionTime(self):
        return self.mOtherExecutionTime 
        
    def printExecutionTime(self):
        print(f"Main Ticker: Execution of order took {self.mMainExecutionTime:.8f} seconds.")
        print(f"Other Tickers: Execution of order took {self.mOtherExecutionTime:.8f} seconds.")
    
    def setCorrMat(self, iMat):
        self.mCorrMat = iMat.sort_values()
        
    def getCorrMat(self):
        return self.mCorrMat
        
    def logAnalysis(self):
        self.printExecutionTime()
        cash_spent = 0
        for order,price in self.mOrder:
            print("\n--- Post Trade Analysis ---")
            print(f"Order ID: {order.id}")
            print(f"Symbol: {order.symbol}")
            print(f"Side: {order.side}")
            print(f"Quantity: {order.qty}")
            print(f"Price: {price}")
            cash_spent = cash_spent + int(order.qty) * price
        print(f"Total porfolio spending: {cash_spent}")
         
         
# Main Instance of the porfolio for the execution, 
# we init it with the default 100k$ but we could 
# init it with the cash from the alpaca account
myPorfolio = Porfolio(cash=100000)


"""
Process the received signal message asynchronously.

The message (msg) is shall contains the following:
    - "ticker": the symbol (default "NVDA")
    - "action": the action to perform ("buy", "sell")
""" 
def process_signal(msg):
    """
    Execute a market order on NVDA based on the signal.
    The order side is determined by the signal direction:
    - 'b' for buy
    - 's' for sell
    """
    ticker = msg.get('Ticker', 'NVDA')
    order_side = 'buy' if msg.get('Direction', 'b').lower() == 'b' else 'sell'
    
    # Calculate how many shares we can buy/sell with our entire cash allocation
    quantity = myPorfolio.getSpecificStockQuantity(ticker)

    if quantity <= 0:
        print("Insufficient capital to execute trade.")
        return None

    # Submit the order.
    try:
        order = alpaca.submit_order(
            symbol=ticker,
            qty=quantity,
            side=order_side,
            type='market',
            time_in_force='day'
        )
        
        if LOG:
            print(f"Submitted {order_side} order for {quantity} shares of {ticker} at market price.")
            
        myPorfolio.setOrderBook(order)
        
        return order
        
    except Exception as e:
        print(f"Error placing order: {e}")
        return None
        
def process_strategy(msg):
    
    order_side = 'buy' if msg.get('Direction', 'b').lower() == 'b' else 'sell'

    #loop on all ticker
    main_order, price = myPorfolio.getMainOrder() or (None, None)
    
    corr_mat = myPorfolio.getCorrMat()
    
    allocated_budget = 0

    if main_order and price:
        allocated_budget = myPorfolio.cash() - (price * int(main_order.qty))
    else:
        allocated_budget = myPorfolio.cash()
        
    # These could be launch in their own thread
    for name, _ in corr_mat.items():
        if name not in shared_data:
            continue
        price = shared_data[name]
        quantity = allocated_budget // price
        if quantity > 0:
            try:
                order = alpaca.submit_order(
                    symbol=name,
                    qty=quantity,
                    side=order_side,
                    type='market',  # in production, you might use more advanced order types
                    time_in_force='day'
                )        
            except Exception as e:
                print(f"Error placing order: {e}")
                continue
            myPorfolio.setOrderBook(order)
            allocated_budget = allocated_budget - price*quantity

    
async def listen_redis_signal():
    # Create an async Redis connection
    r = redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        db=int(os.getenv('REDIS_DB', 0)),
        decode_responses=True
    )
    stream_name = 'nvda'
    while True:
        # xread will wait for a message with a timeout of 5 seconds
        messages = await r.xread({stream_name: '0-0'}, count=1, block=5000)
        if messages:
            start = time.perf_counter()
            stop_event.set() # one-shot system
            for stream, msgs in messages:
                for msg_id, msg in msgs:
                    if LOG:
                        print(f"Received signal: {msg}")
                    process_signal(msg)
                    end = time.perf_counter()
                    myPorfolio.setMainExecutionTime(end-start)
                    process_strategy(msg)
                    end2 = time.perf_counter()
                    myPorfolio.setOthernExecutionTime(end2-end)
                    return  # one-shot system
        else:
            print("No signal received; continuing to wait...")

def updatePriceWorker(ticker, shared_data, lock):
    while True:
        time.sleep(3)
        #today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        if not stop_event.is_set():            
            try:
                # subscription does not permet querying recent SIP data
                #bars = alpaca.get_bars(ticker, TimeFrame.Hour, today, today)
                #bars = alpaca.get_bars(ticker, TimeFrame.Hour, "2021-06-08", "2021-06-08")
                trade = alpaca.get_latest_trade(ticker)
                if not trade:
                    print("No market data available.")
                else:
                    #current_price = bars[-1].c 
                    current_price = trade.price
                    if LOG:
                        print(f"Current market price for {ticker.upper()}: ${current_price:.2f}")
                    with lock:
                        shared_data[ticker.upper()] = current_price
                        if LOG:
                            print("Shared_Data",shared_data)
                
    
            except Exception as e:
                print(f"Error retrieving market data: {e}")
                return None  
        else:
            break
                
async def main():
    await listen_redis_signal()
    

"""Retrieve available cash from the Alpaca account."""
def get_capital_allocation():
    while True:
        time.sleep(3)
        if not stop_event.is_set():
            account = alpaca.get_account()
            myPorfolio.setCash(float(account.cash))
            myPorfolio.updateQuantity()
            if LOG:
                myPorfolio.logPorfolio()  
        else:
            break
        
'''
Compute the correlation Matrix between the givens stocks
'''       
def computeCorrelationMatrix(tickers):
    # Create an empty dictionary to hold each ticker's adjusted close data
    data_dict = {}
    
    # Loop through each ticker and fetch its historical data individually
    for ticker in tickers:
        try:
            ticker_obj = yf.Ticker(ticker)
            hist = ticker_obj.history(start="2020-01-01", end="2025-01-01")
            # Check if data is returned; if not, print a warning
            if hist.empty:
                print(f"Warning: No data returned for {ticker}.")
            else:
                data_dict[ticker] = hist['Close']
        except:
            print(f"Erro with Yahoo fetch {e}")
    # Combine the data into a single DataFrame
    df = pd.DataFrame(data_dict)

    # Calculate daily returns
    returns = df.pct_change().dropna()
    
    # Compute and display the correlation matrix of returns
    corr_matrix = returns.corr()

    # Isolate NVDA's correlations with other stocks (drop self-correlation)
    nvda_corr = corr_matrix.loc['NVDA'].drop('NVDA')
    
    # Get all the correlations based on NVDA
    top_corr = nvda_corr.nlargest(len(tickers))
    
    if LOG:
        print(f"\nThe top {len(tickers)} correlations for NVDA are:")
        print(top_corr)
        
    return top_corr

# This could be calculate outside of market hours
# We could add many more corelation 
def preMarketCalculation():
    
    #Historical corelation for semiconductors company
    #lets take the following stocks:
    '''
    
    Advanced Micro Devices Inc. (AMD)
    Taiwan Semiconductor Manufacturing Co. Ltd (TSM)
    Synopsys Inc. (SNPS)
    Intel Corp. (INTC)
    Qualcomm Inc. (QCOM)
    '''  
    #Historical corelation for big tech company
    #lets take the following stocks:
    '''
    Apple Inc. (AAPL)
    Microsoft Corporation (MSFT)
    Amazon (AMZN)
    Google (GOOGL)
    ''' 
    # This could be refined with other data
    tickers = ['NVDA','AMD','TSM','SNPS','INTC','QCOM','AAPL','MSFT','AMZN','GOOGL']
    
    # Save the matrix
    myPorfolio.setCorrMat(computeCorrelationMatrix(tickers))
    
    if LOG:
        print(myPorfolio.getCorrMat())  
        
    return tickers

if __name__ == '__main__':
    
    # Init mutex for thread data handling
    mutex = threading.Lock()
    
    # Calculate historical data correlation
    tickers = preMarketCalculation()
    
    '''
        THREADS
    '''
    myThreads = []
    
    # Fetch the capital in is thread to know exactly how much we can buy
    CapitalAllocationThread = threading.Thread(target=get_capital_allocation, args=())
    CapitalAllocationThread.start()
    
    # each ticker get is own thread to refresh and fetch the current live price
    for i in range(len(tickers)):
        thread = threading.Thread(target=updatePriceWorker, args=(tickers[i],shared_data,mutex))
        myThreads.append(thread)
        thread.start()
        
    myThreads.append(CapitalAllocationThread)

    # This was for test purposes
    time.sleep(10)
    
    asyncio.run(main())
    
    # Graceful clean-up
    for thread in myThreads:
        thread.join()
        
    print("Final shared_data:", shared_data)
    
    # Log the analysis
    myPorfolio.logAnalysis()
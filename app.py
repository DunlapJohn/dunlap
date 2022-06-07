import pandas as pd
import matplotlib.pyplot as plt
import time
from mpldatacursor import datacursor
import datetime
import mango
import streamlit as st
from sqlalchemy import create_engine
import psycopg2




DB_NAME = "bldpmfro"
DB_USER = "bldpmfro"
DB_HOST = "queenie.db.elephantsql.com"
DB_PASS = "vTgtl5oHo0l5Ct2huCLEYjEvkJkfwUuG"




conn = psycopg2.connect(database = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST )

cur = conn.cursor()
engine = create_engine('postgresql://bldpmfro:vTgtl5oHo0l5Ct2huCLEYjEvkJkfwUuG@queenie.db.elephantsql.com/bldpmfro')
con = engine.connect()

print('Database Connected Successfully')

# cur.execute('Drop Table orderbook')
# cur.execute('Drop Table btc')
# print('Table Dropped Successfully')

cur.execute("""
          CREATE TABLE orderbook
          (index int not null,
          Date TIMESTAMP not null,
          price char(64) not null,
          Size float not null,
          Taker char(64) NOT NULL,
          Maker char(64) NOT NULL
        
          ) 
            """)

conn.commit()    
  
cur.execute("""
          CREATE TABLE btc
          (ID serial primary key,
          Date TIMESTAMP NOT NULL, 
          PRICE decimal NOT NULL,
          OI decimal NOT NULL
          ) 
            """)
conn.commit()  
print('Database tables created Successfully')

stop_requested = False
while not stop_requested:
        try:
            with mango.ContextBuilder.build(cluster_name="mainnet") as context:
                pause_seconds = 15
                market = mango.market(context, "BTC-PERP")
                event_queue = mango.PerpEventQueue.load(context, market.event_queue_address, market.lot_size_converter)
                book = (event_queue.fills)
                data=pd.DataFrame(book)
                if not data.empty:
                    data=data.astype(str)
                    print(data)


                    data[0] = data[0].str.replace('SELL','',regex=True)
                    data[0] = data[0].str.replace('BUY','',regex=True)
                    data[0] = data[0].str.replace(' M','_',regex=True)
                    data[0] = data[0].str.replace('__','-',regex=True)
                    data[0] = data[0].str.replace('at ','_',regex=True)
                    data[0] = data[0].str.replace('r-','_',regex=True)
                    data[0] = data[0].str.replace('r:','_',regex=True)
                    data[0] = data[0].str.replace(', ','_',regex=True)
                    data[0] = data[0].str.replace('[','_',regex=True)
                    data[0] = data[0].str.replace(']','_',regex=True)
                    data[0] = data[0].str.replace(' _','_',regex=True)
                    data[0] = data[0].str.replace('_ ','_',regex=True)
                    data[0] = data[0].str.replace('--','_',regex=True)
                    data[0] = data[0].str.replace('___','_',regex=True)
                    data[0] = data[0].str.replace(',at,','_',regex=True)
                    data[0] = data[0].str.replace(',_','_',regex=True)
                    data[0] = data[0].str.replace('_','_',regex=True)
                    data[0] = data[0].str.replace(',/,','_',regex=True)
                    data[0] = data[0].str.replace(']-','_',regex=True)
                    data[0] = data[0].str.replace('[','',regex=True)
                    
                    data = data[0].str.split('_', expand=True)
        
                    #cd NewMangoProject  streamlit run marketstack.py
                    data=data.astype(str)
                    data.rename(columns = {2:'Size', 3:'Price', 4:'Date',7:'Maker', 9:'Taker'}, inplace = True)
                    data=data[[ 'Date','Size', 'Price', 'Maker','Taker']]
                    
                    data['Date'] = data['Date'].str.replace('[','',regex=True)
                    data['Taker'] = data['Taker'].str.replace(', ',',_',regex=True)
                    
                    data = data.drop_duplicates()

                    
                    data.rename(columns = {'Taker':'taker', 'Date':'date','Price':'price', 'Size':'size','Maker':'maker', 'Taker':'taker'}, inplace = True)
                    
                    data = pd.DataFrame(data)
                    data['date'] = pd.to_datetime(data['date'])
                    data['date'] = data['date'].dt.tz_localize(None)
                    data['price'].apply(pd.to_numeric, errors='coerce')
                    

                    data.to_sql(name='orderbook',con=con, if_exists='append')
                    Query = ("select * from orderbook")
                    cur.execute(Query)
                    data = cur.fetchall()
                    st.write(data)
                    data=pd.DataFrame(data)
                    data[1] = pd.to_datetime(data[1])
                    
                    # data['y'] = np.where(data[3] > 1,1,0)
                    # data = data.loc[data['y']==1]
                
                    data = data.sort_values([2,1], ascending=[True,True])
                    st.write(data)
                    
                    x=data[1]
                    y=data[2]
                    # Ploting Order Book Data
                    fig, ax1 = plt.subplots(1, figsize=(12,6))
                    
                    fig.set_size_inches(16.5, 8.5)
                    lines = ax1.scatter(x,y, label ="Maker")
                    ax1.set_xlabel('date')
                    ax1.set_ylabel('price')
                    datacursor(lines)
                    st.pyplot(fig)
                    
                    data=pd.DataFrame(data)
                    data1=data.groupby([4])[2].sum()
                    data2=data.groupby([5])[2].sum()
                    data2 = data2.astype('string')

                    fig, ax1 = plt.subplots(1, figsize=(12,6))
                    
                    fig.set_size_inches(16.5, 8.5)
                    lines = ax1.scatter(x,y, label ="Maker")
                    ax1.set_xlabel('date')
                    ax1.set_ylabel('price')
                    datacursor(lines)
                    st.pyplot(fig)
                    
                    
                
                
                # data = str(data)
                
                # cur.execute("INSERT INTO orderbook (taker) values (%s)",[data])
                
                print('Orderbook data inserted successfully')
                # print('done')
                with mango.ContextBuilder.build() as context:
                    pause_seconds = 10
                    perp_market = mango.PerpMarket.ensure(mango.market(context, 'BTC-PERP'))   
                    market = perp_market.fetch_funding(context)
                    st.write(market)
                    market = pd.DataFrame([market])
                    st.table(market)
                    market['open_interest_BTC']=market['open_interest'].apply(pd.to_numeric, errors='coerce')
                    market['oracle_price']=market['oracle_price'].apply(pd.to_numeric, errors='coerce')
                    market['open_interest']=market['oracle_price']*market['open_interest_BTC']
                    market = market[['oracle_price','from_','open_interest']]

                    b=market[['oracle_price']].iloc[0]
                    b=b.item()
                    b = float(b)
                    
                    a=market[['open_interest']].iloc[0]
                    a=a.item()
                    a = float(a)
                    # datetime object containing current date and time
                    now = datetime.datetime.now()
                    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                    print(dt_string, b, a)

                    cur.execute("SET datestyle = dmy")
                    conn.commit()

                    cur.execute("INSERT INTO btc (Date, Price, OI) values ( %s, %s, %s)",(dt_string, b, a))

                    conn.commit()

                print('BTC data inserted successfully')
                print(f"Pausing for {pause_seconds} seconds.\n")
                time.sleep(pause_seconds)
            
            
            
        except KeyboardInterrupt:
            stop_requested = True


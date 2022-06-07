import pandas as pd
import time
import datetime
import mango
import streamlit as st
from sqlalchemy import create_engine
import psycopg2



 

DB_NAME = "d9k0a7aiggrtae"
DB_USER = "emyiazqpbkpsib"
DB_HOST = "qec2-52-206-182-219.compute-1.amazonaws.com"
DB_PASS = "ae0b97784400b039cb892b3c09b8f07ffff163e00fb18251de127ef0e8c54907"
DB_PORT = 5342


conn = psycopg2.connect(database = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port =DB_PORT)

cur = conn.cursor()
engine = create_engine('postgres://emyiazqpbkpsib:ae0b97784400b039cb892b3c09b8f07ffff163e00fb18251de127ef0e8c54907@ec2-52-206-182-219.compute-1.amazonaws.com:5432/d9k0a7aiggrtae')
con = engine.connect()

print('Database Connected Successfully')

# cur.execute('Drop Table btc')
# print('Table Dropped Successfully')

# cur.execute("""
#           CREATE TABLE orderbook
#           (index int not null,
#           Date TIMESTAMP not null,
#           price char(64) not null,
#           Size float not null,
#           Taker char(64) NOT NULL,
#           Maker char(64) NOT NULL
        
#           ) 
#             """)

# conn.commit()    
  
# cur.execute("""
#           CREATE TABLE btc
#           (ID serial primary key,
#           Date TIMESTAMP NOT NULL, 
#           PRICE decimal NOT NULL,
#           OI decimal NOT NULL
#           ) 
#             """)
# conn.commit()  
# print('Database tables created Successfully')

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
                    data=pd.DataFrame(data)
                    data[1] = pd.to_datetime(data[1])
                    
                    # data['y'] = np.where(data[3] > 1,1,0)
                    # data = data.loc[data['y']==1]
                
                    data = data.sort_values([2,1], ascending=[True,True])
                    

                    
                    
                
                
                # data = str(data)
                
                # cur.execute("INSERT INTO orderbook (taker) values (%s)",[data])
                
                print('Orderbook data inserted successfully')
                # print('done')
                with mango.ContextBuilder.build() as context:
                    pause_seconds = 10
                    perp_market = mango.PerpMarket.ensure(mango.market(context, 'BTC-PERP'))   
                    market = perp_market.fetch_funding(context)
                    market = pd.DataFrame([market])
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

                def orders():
                    conn = psycopg2.connect(database = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST )
                    cur = conn.cursor()
                    cur.execute("SELECT * FROM orderbook")
                    rows = cur.fetchall()
                    order = rows
                    order = pd.DataFrame(order)
                    order.rename(columns = {0:'Index',1:'Date', 2:'Price', 3:'Size',4:'Maker', 5:'Taker'}, inplace = True)
                    order=order.drop_duplicates( keep=False)
                    order['Taker'] = order['Taker'].str.strip()
                    order['Date'].unique()
                    order['Price'] =order['Price'].str.replace(',','')
                    order['Price']= order['Price'].astype(float)
                    order['Price'].astype(int)
                    order['USD_Size'] = order['Size']*order['Price']
                    return order

                order=orders()
                st.write(order)
                print('Dataframe Successfully Organized')
            
            
        except KeyboardInterrupt:
            stop_requested = True


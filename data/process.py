import requests 
import pandas as pd 
from dotenv import load_dotenv
import os 

URLS ={
    'NASDAQ': 'https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt',
    'OTHER_LISTED': 'https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt',
}

def get_ticker_data(URL):
    resp = requests.get(URL)

    data = resp.text.split("\n")
    tickerls = list()
    namels = list()
    
    for line in data[1:len(data)-2]:
        elements = line.split("|")[0:2] # first and second row contain the ticker and the company name 
        tickerls.append(elements[0].strip())
        namels.append(elements[1].split("-")[0].strip())
    
    return tickerls, namels

def prepare_file(file_path):

    write_header =  os.path.exists(file_path)
    if write_header:
        columns = ["Ticker", "Name"]
        pd.DataFrame(columns = columns).to_csv(file_path, index = False)



def create_dataframe(URL):
    
    tickers, names = get_ticker_data(URL)
    
    df = pd.DataFrame({"Ticker": tickers, "Name": names})

    return df
        

def create_file(file_path = "./tickers.csv"):

    prepare_file(file_path)

    for name, url in URLS.items():
        write_header = not os.path.exists(file_path)
        df = create_dataframe(url)
        df.to_csv(
            file_path,
            mode='a',
            header = write_header,
            index = False
        )
if __name__ == "__main__":

    create_file()
    


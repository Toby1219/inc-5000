import asyncio, logging, inspect, time, functools, sqlite3, os
from requests_cache import CachedSession
from fake_useragent import UserAgent
import json
import pandas as pd
from dataclasses import dataclass, asdict, field

def timer(func):
    @functools.wraps(func)
    async def wrapper(*agrs, **kwargs):
        start = time.perf_counter()
        await func(*agrs, **kwargs)
        end = time.perf_counter()
        total = end - start
        print(f"Execution time: {round(total, 2)}s")
    return wrapper


@dataclass
class Companies:
    Rank:str
    CompanyName:str
    Workers:str
    PreviousWorker:str
    Website:str
    State:str
    City:str
    Growth:str
    Industry:str
    Metro:str
    MetroCode:str
    ZipCode:int
    Founded:int

@dataclass
class StoreData:
    data_list:list[Companies] = field(default_factory=list)
    folder:str = ''
    file:str = ''
    path:str = ''
    
    def mkdirctory(self):
        if not os.path.exists(self.folder):
            os.mkdir(self.folder)
            self.path = f"{self.folder}/{self.file}"
            return self.path 
        
    def dataframe(self):
        return pd.json_normalize(self.data_list, sep='_')
    
    def savetojson(self):
        self.path = f"{self.folder}/{self.file}"
        if not os.path.exists(f'{self.path}.json'):
            self.dataframe().to_json(f'{self.path}.json', orient='records', index=False, indent=3)
        else:
            existing_df = pd.read_json(f"{self.path}.json")
            new_df = self.dataframe()
            update_df = pd.concat([existing_df, new_df])
            update_df.to_json(f"{self.path}.json", orient='records', indent=2)

    def savetocsv(self):
        self.path = f"{self.folder}/{self.file}"
        if os.path.exists(f'{self.path}.csv'):
            self.dataframe().to_csv(f"{self.path}.csv", index=False, mode='a', header=False)
        else:
            self.dataframe().to_csv(f'{self.path}.csv', index=False)
    
    def savetoexcel(self):
        self.path = f"{self.folder}/{self.file}"
        if not os.path.exists(f'{self.path}.xlsx'):
            self.dataframe().to_excel(f'{self.path}.xlsx', index=False)
        else:
            with pd.ExcelWriter(f'{self.path}.xlsx', mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
                self.dataframe().to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=writer.sheets['Sheet1'].max_row)

    def savetosql(self):
        self.path = f"{self.folder}/{self.file}"
        conn = sqlite3.connect(f"{self.path}.db")
        self.dataframe().to_sql(name='scraped', con=conn, if_exists='append', index=False)
        conn.close()             
            
    def saver(self):
        self.mkdirctory()
        self.savetojson()
        self.savetocsv()
        self.savetoexcel()
        self.savetosql()
                
async def get_response(url):
    head = {
        "authority": "api.inc.com",
        "method": "GET",
        "scheme": "https",
        "accept": "application/json",
        "accept-encoding": "gzip",
        "origin": "https://www.inc.com",
        "referer": "https://www.inc.com/",
        "user-agent": UserAgent().random
    }
    session = CachedSession("incCached.db")
    response = session.get(url, headers=head)
    if response.status_code == 200:
        return response.json()
    else:
        return response.status_code

async def extract_data(jsonObj):
    commpany = jsonObj['companies']
    store_data = StoreData(folder="inc5000_2024", file="2004_data")
    for item in commpany:
        rank = item['rank']
        company_name = item['company']
        workers = item['workers']
        previous_worker = item['previous_workers']
        website = item['website']
        state = item['state_l']
        city = item['city']
        growth = f"{item['growth']:,}%"
        industry = item['industry']
        metro = item['metro']
        metrocode = item['metrocode']
        zipcode = item['zipcode']
        founded = item['founded']
        dataObj = Companies(Rank=rank, CompanyName=company_name, Workers=workers, 
                            PreviousWorker=previous_worker, Website=website, State=state,
                            City=city, Growth=growth, Industry=industry, Metro=metro,
                            MetroCode=metrocode, ZipCode=zipcode, Founded=founded)
        
        store_data.data_list.append(asdict(dataObj))
    store_data.saver()

@timer
async def main():
    for pag in range(1, 7):
        response = await get_response(f'https://api.inc.com/rest/i5list/2024?records=1000&page={pag}')
        await extract_data(response)
   
if __name__ == '__main__':
    asyncio.run(main())
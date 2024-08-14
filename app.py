from flask import Flask, render_template, jsonify, Response, stream_with_context
from pymongo import MongoClient
import json
import asyncio
import aiohttp
import time
from datetime import timezone
from contextlib import asynccontextmanager
from datetime import datetime
from flask_cors import CORS
from threading import Thread
from collections import deque

app = Flask(__name__)
CORS(app)

RATE_LIMIT_STATUS = 429
RATE_LIMIT_WAIT_TIME = 40
MAX_RETRIES = 3
ITEMS_PER_SECOND = 1.0
ITEM_DELAY = 1 / ITEMS_PER_SECOND
log_queue = deque(maxlen=100)

client = MongoClient('mongodb+srv://gaabriieel2233:5Skt7kY0rtJoiQS8@albion-data.e5j2tli.mongodb.net/')
db = client['albion-data']

items_collection = db['item_prices']
blacklist_collection = db['blacklist']
collection_status_collection = db['collection_status']
lucrative_items_collection = db['item_lucratives']
logs_collection = db['logs']

with open('items.json', 'r', encoding='utf-8') as f:
    items_data = json.load(f)

cities = ['Bridgewatch', 'Caerleon', 'Fort Sterling', 'Lymhurst', 'Martlock', 'Thetford', 'Black Market']

async def read_blacklist():
    return {item['unique_name'] for item in blacklist_collection.find()}

async def ensure_db_initialized():
    return await read_blacklist()

@app.before_request
def before_request():
    global blacklist
    blacklist = asyncio.run(ensure_db_initialized())

def add_to_blacklist(item):
    blacklist_collection.insert_one({"unique_name": item})

def update_collection_status(index):
    collection_status_collection.update_one(
        {"id": 1},
        {"$set": {"current_index": index}},
        upsert=True
    )

def get_last_saved_index():
    status = collection_status_collection.find_one({"id": 1})
    return int(status['current_index']) if status else 0

blacklist = asyncio.run(read_blacklist())

async def fetch_with_retry(session, url, max_retries=5, delay=1):
    for attempt in range(max_retries):
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientResponseError, aiohttp.ClientConnectorError) as e:
            if attempt == max_retries - 1:
                error_msg = f"Erro após {max_retries} tentativas: {str(e)}"
                print(error_msg)
                return None
            await asyncio.sleep(delay * (2 ** attempt))  # exponential backoff

@asynccontextmanager
async def get_session():
    session = aiohttp.ClientSession()
    try:
        yield session
    finally:
        await session.close()

class RateLimiter:
    def __init__(self, rate, per):
        self.rate = rate
        self.per = per
        self.allowance = rate
        self.last_check = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            current = time.monotonic()
            time_passed = current - self.last_check
            self.last_check = current
            self.allowance += time_passed * (self.rate / self.per)
            if self.allowance > self.rate:
                self.allowance = self.rate

            if self.allowance < 1.0:
                sleep_time = (1.0 - self.allowance) * (self.per / self.rate)
                await asyncio.sleep(sleep_time)
                self.allowance = 0.0
            else:
                self.allowance -= 1.0

rate_limiter = RateLimiter(rate=280, per=300)  # 300 requests per 300 seconds (5 minutes)

async def fetch_item_data(session, items):
    await rate_limiter.acquire()
    cities_str = ','.join(cities)
    item_names = ','.join([item['UniqueName'] for item in items])
    url = f"https://west.albion-online-data.com/api/v2/stats/prices/{item_names}?locations={cities_str}"
    data = await fetch_with_retry(session, url)
    if data and len(data) > 0:
        item_data = []
        for entry in data:
            item = next((item for item in items if item['UniqueName'] == entry['item_id']), None)
            if item:
                item_data.append({
                    'unique_name': item['UniqueName'],
                    'city': entry['city'],
                    'sell_price_min': entry.get('sell_price_min'),
                    'buy_price_max': entry.get('buy_price_max'),
                    'index': item['Index'],
                    'last_updated_date': entry.get('sell_price_min_date'),
                    'last_saved_date': datetime.utcnow().isoformat()
                })
        return item_data
    return None

async def collect_data():
    global blacklist
    blacklist = await read_blacklist()

    last_index = get_last_saved_index()
    print(f"Continuando coleta a partir do índice {last_index}")

    async with get_session() as session:
        items_batch = []
        for item in items_data[last_index:]:
            if item['UniqueName'] in blacklist:
                print(f"Pulando item na lista negra: {item['UniqueName']}")
                continue

            items_batch.append(item)
            if len(items_batch) >= 100:  # Ajuste o tamanho do lote conforme necessário
                item_data = await fetch_item_data(session, items_batch)
                if item_data:
                    for data in item_data:
                        print(f"Dados coletados para {data['unique_name']} em {data['city']}")
                    print(f"Salvando dados para o lote de itens")
                    await save_item_data(item_data)
                else:
                    for item in items_batch:
                        print(f"Nenhum dado disponível para {item['UniqueName']} em todas as cidades. Adicionando à lista negra.")
                        add_to_blacklist(item['UniqueName'])
                items_batch = []

            update_collection_status(item['Index'])

            # Adiciona um atraso de 1.1 segundos entre as coletas de dados
            await asyncio.sleep(1.1)

        # Process remaining items in the batch
        if items_batch:
            item_data = await fetch_item_data(session, items_batch)
            if item_data:
                for data in item_data:
                    print(f"Dados coletados para {data['unique_name']} em {data['city']}")
                print(f"Salvando dados para o lote de itens")
                await save_item_data(item_data)
            else:
                for item in items_batch:
                    print(f"Nenhum dado disponível para {item['UniqueName']} em todas as cidades. Adicionando à lista negra.")
                    add_to_blacklist(item['UniqueName'])

        print("Coleta de dados concluída.")

async def save_item_data(item_data):
    try:
        for data in item_data:
            if data['buy_price_max'] > 0 and data['sell_price_min'] > 0:
                items_collection.update_one(
                    {"unique_name": data['unique_name'], "city": data['city']},
                    {"$set": data},
                    upsert=True
                )
                print(f"Dados inseridos no banco para {data['unique_name']} em {data['city']}")
            else:
                print(f"Dados ignorados para {data['unique_name']} em {data['city']} devido a preços inválidos")
    except Exception as e:
        error_msg = f"Erro ao inserir dados no MongoDB: {str(e)}"
        print(error_msg)

def start_background_task(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(collect_data())

@app.route('/')
def index():
    return render_template('index.html')

async def save_profitable_items(items):
    try:
        await lucrative_items_collection.delete_many({})
        await lucrative_items_collection.insert_many([
            {"unique_name": unique_name, "city": item_data['city']}
            for unique_name, item_data in items.items()
        ])
        print("Itens lucrativos atualizados com sucesso!")
    except Exception as e:
        error_msg = f"Erro ao atualizar itens lucrativos: {str(e)}"
        print(error_msg)

def calculate_profitable_items():
    items = list(items_collection.find())
    
    if not items:
        return {}

    item_profit = {}
    
    for item in items:
        unique_name = item['unique_name']
        city = item['city']
        buy_price_max = item['buy_price_max']
        sell_price_min = item['sell_price_min']
        
        if buy_price_max == 0 or sell_price_min == 0:
            continue
        
        profit = sell_price_min - buy_price_max
        profit_percentage = (profit / buy_price_max) * 100 if buy_price_max > 0 else 0
        
        if unique_name not in item_profit or profit > item_profit[unique_name]['profit']:
            item_profit[unique_name] = {
                'city': city,
                'buy_price_max': buy_price_max,
                'sell_price_min': sell_price_min,
                'profit': profit,
                'profit_percentage': profit_percentage
            }
    
    # Filtrar itens com lucro superior a 15%
    filtered_items = {
        unique_name: data for unique_name, data in item_profit.items()
        if data['profit_percentage'] > 15
    }
    
    sorted_items = dict(sorted(filtered_items.items(), key=lambda x: x[1]['profit'], reverse=True))
    
    limited_items = dict(list(sorted_items.items())[:100])
    
    return limited_items

@app.route('/profit')
def profit():
    profitable_items = calculate_profitable_items()
    save_profitable_items(profitable_items)
    
    limited_items = dict(list(profitable_items.items())[:100])
    
    return render_template('profit.html', items=limited_items)

@app.route('/db')
def show_database():
    tables = db.list_collection_names()

    all_data = {}

    for table in tables:
        rows = list(db[table].find())
        all_data[table] = rows

    return render_template('database.html', tables=tables, data=all_data)

@app.route('/api/profitable_items')
def api_profitable_items():
    try:
        profitable_items = calculate_profitable_items()
        
        serializable_items = {}
        for unique_name, item_data in profitable_items.items():
            serializable_items[unique_name] = {
                'city': item_data['city'],
                'buy_price_max': item_data['buy_price_max'],
                'sell_price_min': item_data['sell_price_min'],
                'profit': item_data['profit'],
                'profit_percentage': item_data['profit_percentage']
            }
        
        return jsonify(serializable_items)
    except Exception as e:
        print(f"Erro ao buscar itens lucrativos: {str(e)}")
        return jsonify({"error": "Erro ao buscar itens lucrativos"}), 500

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    t = Thread(target=start_background_task, args=(loop,))
    t.start()
    app.run(debug=True)

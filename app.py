import sqlite3
import json
import asyncio
import aiohttp
from aiohttp import ClientResponseError
from datetime import datetime
import threading
from flask import Flask, render_template, jsonify
from flask_cors import CORS  # Importe o CORS

app = Flask(__name__)
CORS(app) 

# Configurações
RATE_LIMIT_STATUS = 429
RATE_LIMIT_WAIT_TIME = 40
MAX_RETRIES = 3
ITEMS_PER_SECOND = 1.0  # Ajustado para respeitar o limite de 300 chamadas a cada 5 minutos
ITEM_DELAY = 1 / ITEMS_PER_SECOND

# Configuração do banco de dados SQLite
DB_NAME = '/tmp/albion_market.db'

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Criar tabelas
    c.execute('''CREATE TABLE IF NOT EXISTS item_prices
                 (unique_name TEXT, city TEXT, sell_price_min INTEGER, buy_price_max INTEGER, 
                  item_name TEXT, index_value INTEGER, last_updated_date TEXT, last_saved_date TEXT,
                  PRIMARY KEY (unique_name, city))''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS blacklist
                 (unique_name TEXT PRIMARY KEY)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS collection_status
                 (id INTEGER PRIMARY KEY, current_index INTEGER)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS item_lucratives
                 (unique_name TEXT, city TEXT, PRIMARY KEY (unique_name, city))''')
    
    conn.commit()
    conn.close()

init_db()

# Carregar dados dos itens
with open('items.json', 'r', encoding='utf-8') as f:
    items_data = json.load(f)

cities = ['Bridgewatch', 'Caerleon', 'Fort Sterling', 'Lymhurst', 'Martlock', 'Thetford', 'Black Market']

def read_blacklist():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT unique_name FROM blacklist")
    result = c.fetchall()
    conn.close()
    return set(item[0] for item in result)

def add_to_blacklist(item):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO blacklist (unique_name) VALUES (?)", (item,))
        conn.commit()
    except sqlite3.IntegrityError:
        print(f"Item {item} já está na lista negra.")
    finally:
        conn.close()

def update_collection_status(index):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO collection_status (id, current_index) VALUES (1, ?)", (index,))
    conn.commit()
    conn.close()

blacklist = read_blacklist()

async def fetch_item_data(session, item, city):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            if item is None or 'UniqueName' not in item:
                print(f"Item inválido: {item}")
                return None

            if item['UniqueName'] in blacklist:
                print(f"Item {item['UniqueName']} está na lista negra e será ignorado.")
                return None

            url = f"https://west.albion-online-data.com/api/v2/stats/prices/{item['UniqueName']}?locations={city}"
            async with session.get(url) as response:
                if response.status == RATE_LIMIT_STATUS:
                    print(f"Limite de taxa atingido. Aguardando {RATE_LIMIT_WAIT_TIME} segundos antes de tentar novamente.")
                    await asyncio.sleep(RATE_LIMIT_WAIT_TIME)
                    retries += 1
                    continue

                response.raise_for_status()
                data = await response.json()
                if data and len(data) > 0:
                    return {
                        'unique_name': str(item['UniqueName']),
                        'city': str(city),
                        'sell_price_min': str(data[0].get('sell_price_min', '0')),
                        'buy_price_max': str(data[0].get('buy_price_max', '0')),
                        'item_name': str(item.get('LocalizedNames', {}).get('PT-BR') or item.get('LocalizedNames', {}).get('EN-US', 'Unknown')),
                        'index': str(item.get('Index', '0')),
                        'last_updated_date': str(data[0].get('sell_price_min_date', datetime.utcnow().isoformat())),
                        'last_saved_date': str(datetime.utcnow().isoformat())
                    }
                else:
                    print(f"Nenhum dado disponível para {item['UniqueName']} em {city}")
                    return None
        except ClientResponseError as e:
            if e.status == RATE_LIMIT_STATUS:
                print(f"Limite de taxa atingido. Aguardando {RATE_LIMIT_WAIT_TIME} segundos antes de tentar novamente.")
                await asyncio.sleep(RATE_LIMIT_WAIT_TIME)
                retries += 1
            else:
                print(f"Erro ao buscar dados para {item['UniqueName']} em {city}: {str(e)}")
                return None
        except Exception as e:
            print(f"Erro ao buscar dados para {item.get('UniqueName', 'Unknown')} em {city}: {str(e)}")
            return None

    print(f"Não foi possível obter dados para {item.get('UniqueName', 'Unknown')} em {city} após {MAX_RETRIES} tentativas.")
    return None

async def collect_data(start_index=0):
    global blacklist
    blacklist = read_blacklist()
    processed_items = 0
    items_processed = []

    async with aiohttp.ClientSession() as session:
        for item in items_data[start_index:]:
            if processed_items >= 30:
                break

            if item['UniqueName'] in blacklist:
                print(f"Pulando item na lista negra: {item['UniqueName']}")
                continue

            item_data = []
            for city in cities:
                data = await fetch_item_data(session, item, city)
                if data:
                    item_data.append(data)
                
                await asyncio.sleep(ITEM_DELAY)

            if not item_data:
                print(f"Nenhum dado disponível para {item['UniqueName']} em todas as cidades. Adicionando à lista negra.")
                add_to_blacklist(item['UniqueName'])
                continue

            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            for data in item_data:
                try:
                    c.execute('''INSERT OR REPLACE INTO item_prices 
                        (unique_name, city, sell_price_min, buy_price_max, item_name, index_value, last_updated_date, last_saved_date) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                        (str(data['unique_name']), str(data['city']), 
                        int(data['sell_price_min'] or 0), int(data['buy_price_max'] or 0),
                        str(data['item_name']), int(data['index'] or 0), 
                        str(data['last_updated_date']), str(data['last_saved_date'])))
                    print(f"Item: {data['item_name']}, City: {data['city']}, Buy Price Max: {data['buy_price_max']}, Sell Price Min: {data['sell_price_min']}, Updated At: {data['last_updated_date']}")
                except Exception as e:
                    print(f"Erro ao inserir dados no SQLite para {item['UniqueName']} em {data['city']}: {str(e)}")
            conn.commit()
            conn.close()

            print(f"Processed item: {item['UniqueName']}")
            items_processed.append(item)
            
            update_collection_status(item['Index'])

            processed_items += 1
            await asyncio.sleep(ITEM_DELAY)

        if items_processed:
            update_collection_status(items_processed[-1]['Index'] + 1)

@app.route('/')
def collect():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT current_index FROM collection_status WHERE id = 1")
    result = c.fetchone()
    current_index = result[0] if result else 0
    conn.close()
    
    def background_collect():
        try:
            asyncio.run(collect_data(current_index))
        except Exception as e:
            print(f"Erro durante a coleta de dados: {str(e)}")

    thread = threading.Thread(target=background_collect)
    thread.start()
    
    return render_template('index.html', message="Data collection started in the background!")

def save_profitable_items(items):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("DELETE FROM item_lucratives")
        c.executemany("INSERT INTO item_lucratives (unique_name, city) VALUES (?, ?)",
                      [(unique_name, item_data['city']) for unique_name, item_data in items.items()])
        conn.commit()
        print("Itens lucrativos atualizados com sucesso!")
    except Exception as e:
        print(f"Erro ao atualizar itens lucrativos: {str(e)}")
    finally:
        conn.close()

def calculate_profitable_items():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row  # Adicione esta linha
    c = conn.cursor()
    c.execute("SELECT * FROM item_prices")
    items = c.fetchall()
    conn.close()
    
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
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row['name'] for row in c.fetchall()]

    all_data = {}

    for table in tables:
        c.execute(f"SELECT * FROM {table}")
        rows = c.fetchall()
        all_data[table] = [dict(row) for row in rows]

    conn.close()

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

if __name__ == "__main__":
    app.run(debug=True)

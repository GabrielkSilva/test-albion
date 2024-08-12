from flask import Flask, render_template
import json
import asyncio
import aiohttp
from aiohttp import ClientResponseError
from supabase import create_client, Client
from datetime import datetime
import time

app = Flask(__name__)

RATE_LIMIT_STATUS = 429  # Código HTTP para "Too Many Requests"
RATE_LIMIT_WAIT_TIME = 30  # Tempo de espera em segundos
MAX_RETRIES = 3  # Número máximo de tentativas

# Configuração do Supabase
url: str = "https://krtfgygfynhsveoxogex.supabase.co"
key: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtydGZneWdmeW5oc3Zlb3hvZ2V4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjM0ODMxNjIsImV4cCI6MjAzOTA1OTE2Mn0.eO_zJ97Zuf3QQHLJ48ZwEsZQ4bWr6jxj-N8IwJOBYHk"
supabase: Client = create_client(url, key)

# Carregar dados dos itens
with open('items.json', 'r', encoding='utf-8') as f:
    items_data = json.load(f)

cities = ['Bridgewatch', 'Caerleon', 'Fort Sterling', 'Lymhurst', 'Martlock', 'Thetford', 'Black Market']

def read_blacklist():
    try:
        with open('blacklist.json', 'r') as f:
            return set(json.load(f))
    except FileNotFoundError:
        return set()

def write_blacklist(blacklist):
    with open('blacklist.json', 'w') as f:
        json.dump(list(blacklist), f)

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
                        'unique_name': item['UniqueName'],
                        'city': city,
                        'sell_price_min': data[0].get('sell_price_min', 0),
                        'buy_price_max': data[0].get('buy_price_max', 0),
                        'item_name': item.get('LocalizedNames', {}).get('PT-BR') or item.get('LocalizedNames', {}).get('EN-US', 'Unknown'),
                        'index': item.get('Index', 0),
                        'last_updated_date': data[0].get('sell_price_min_date', datetime.utcnow().isoformat()),
                        'last_saved_date': datetime.utcnow().isoformat()
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
    async with aiohttp.ClientSession() as session:
        for item in items_data[start_index:]:
            if item['UniqueName'] in blacklist:
                print(f"Pulando item na lista negra: {item['UniqueName']}")
                continue

            item_data = []
            for city in cities:
                data = await fetch_item_data(session, item, city)
                if data:
                    item_data.append(data)
                
                # Pausa de 1.5 segundos entre chamadas de API
                await asyncio.sleep(1.5)

            if not item_data:
                print(f"Nenhum dado disponível para {item['UniqueName']} em todas as cidades. Adicionando à lista negra.")
                blacklist.add(item['UniqueName'])
                write_blacklist(blacklist)
                continue

            for data in item_data:
                try:
                    supabase.table('item_prices').upsert(data).execute()
                    print(f"Item: {data['item_name']}, City: {data['city']}, Buy Price Max: {data['buy_price_max']}, Sell Price Min: {data['sell_price_min']}, Updated At: {data['last_updated_date']}")
                except Exception as e:
                    print(f"Erro ao inserir dados no Supabase para {item['UniqueName']} em {data['city']}: {str(e)}")

            print(f"Processed item: {item['UniqueName']}")
            
            try:
                supabase.table('collection_status').update({'current_index': int(item['Index'])}).eq('id', 1).execute()
            except Exception as e:
                print(f"Erro ao atualizar o índice no Supabase: {str(e)}")

            # Pausa de 1.5 segundos entre cada iteração do item
            await asyncio.sleep(0.3)

        if start_index + len(items_data) >= len(items_data):
            start_index = 0


import threading

@app.route('/')
def collect():
    result = supabase.table('collection_status').select('current_index').execute()
    current_index = result.data[0]['current_index'] if result.data else 0
    
    def background_collect():
        try:
            asyncio.run(collect_data(current_index))
        except Exception as e:
            print(f"Erro durante a coleta de dados: {str(e)}")

    # Inicia a coleta em um thread separado
    thread = threading.Thread(target=background_collect)
    thread.start()
    
    return render_template('index.html', message="Data collection started in the background!")



if __name__ == "__main__":
    app.run(debug=True)
    

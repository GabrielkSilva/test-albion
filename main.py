from flask import Flask, render_template
import json
import asyncio
import aiohttp
from supabase import create_client, Client

app = Flask(__name__)

# Configuração do Supabase
url: str = "https://krtfgygfynhsveoxogex.supabase.co"
key: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtydGZneWdmeW5oc3Zlb3hvZ2V4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjM0ODMxNjIsImV4cCI6MjAzOTA1OTE2Mn0.eO_zJ97Zuf3QQHLJ48ZwEsZQ4bWr6jxj-N8IwJOBYHk"
supabase: Client = create_client(url, key)

# Carregar dados dos itens
with open('items.json', 'r', encoding='utf-8') as f:
    items_data = json.load(f)

cities = ['Bridgewatch', 'Caerleon', 'Fort Sterling', 'Lymhurst', 'Martlock', 'Thetford', 'Black Market']

async def fetch_item_data(session, item, city):
    url = f"https://west.albion-online-data.com/api/v2/stats/prices/{item['UniqueName']}?locations={city}"
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            if data:
                return {
                    'unique_name': item['UniqueName'],
                    'city': city,
                    'sell_price_min': data[0].get('sell_price_min', 0),
                    'buy_price_max': data[0].get('buy_price_max', 0),
                    'item_name': item['LocalizedNames']['PT-BR'],
                    'index': item['Index']
                }
    return None

async def collect_data(start_index=0):
    async with aiohttp.ClientSession() as session:
        for item in items_data[start_index:]:
            for city in cities:
                data = await fetch_item_data(session, item, city)
                if data:
                    # Inserir ou atualizar dados no Supabase
                    supabase.table('item_prices').upsert(data).execute()
            
            print(f"Processed item: {item['UniqueName']}")
            
            # Atualizar o índice atual no Supabase
            supabase.table('collection_status').update({'current_index': int(item['Index'])}).eq('id', 1).execute()

            # Pequena pausa para evitar sobrecarga da API
            await asyncio.sleep(0.1)

@app.route('/collect.html')
def collect():
    # Obter o índice atual do Supabase
    result = supabase.table('collection_status').select('current_index').execute()
    current_index = result.data[0]['current_index'] if result.data else 0

    # Executar a coleta de dados
    asyncio.run(collect_data(current_index))
    
    return "Data collection started!"

if __name__ == "__main__":
    app.run(debug=True)
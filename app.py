from flask import Flask, render_template, jsonify
from pymongo import MongoClient
from flask_cors import CORS
from collections import OrderedDict

app = Flask(__name__)
CORS(app)

client = MongoClient('mongodb+srv://gaabriieel2233:5Skt7kY0rtJoiQS8@albion-data.e5j2tli.mongodb.net/')
db = client['albion-data']

items_collection = db['item_prices']
lucrative_items_collection = db['item_lucratives']

@app.route('/')
def index():
    return render_template('index.html')

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
    app.run(debug=True)

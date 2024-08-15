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
    pipeline = [
        {
            '$match': {
                'buy_price_max': {'$gt': 0},
                'sell_price_min': {'$gt': 0}
            }
        },
        {
            '$project': {
                'unique_name': 1,
                'city': 1,
                'buy_price_max': 1,
                'sell_price_min': 1,
                'profit': {
                    '$subtract': ['$sell_price_min', '$buy_price_max']
                },
                'profit_percentage': {
                    '$multiply': [
                        {
                            '$divide': [
                                {'$subtract': ['$sell_price_min', '$buy_price_max']},
                                '$buy_price_max'
                            ]
                        },
                        100
                    ]
                }
            }
        },
        {
            '$match': {
                'profit_percentage': {'$gt': 15}
            }
        },
        {
            '$sort': {'profit': -1}
        },
        {
            '$limit': 100
        }
    ]

    items = list(items_collection.aggregate(pipeline))
    return {item['unique_name']: item for item in items}


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

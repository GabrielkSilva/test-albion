const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());

mongoose.connect('mongodb+srv://gaabriieel2233:5Skt7kY0rtJoiQS8@albion-data.e5j2tli.mongodb.net/albion-data', {
    useNewUrlParser: true,
    useUnifiedTopology: true,
})
.then(() => console.log('MongoDB connected'))
.catch(err => console.error('MongoDB connection error:', err));

mongoose.connection.on('disconnected', () => {
    console.error('MongoDB disconnected. Trying to reconnect...');
    setTimeout(() => {
        mongoose.connect('mongodb+srv://gaabriieel2233:5Skt7kY0rtJoiQS8@albion-data.e5j2tli.mongodb.net/albion-data', {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        }).catch(err => console.error('MongoDB reconnection error:', err));
    }, 5000);
});

const itemSchema = new mongoose.Schema({
    unique_name: String,
    city: String,
    buy_price_max: Number,
    sell_price_min: Number,
});

const Item = mongoose.model('Item', itemSchema);

// Configurando o EJS como mecanismo de visualização
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

app.get('/profit', async (req, res) => {
    try {
        const profitableItems = await calculateProfitableItems();
        const limitedItems = profitableItems.slice(0, 100);
        res.render('profit', { items: limitedItems });
    } catch (error) {
        console.error("Error fetching profitable items: ", error);
        res.status(500).send("Server Error");
    }
});

app.get('/db', async (req, res) => {
    try {
        if (!mongoose.connection.readyState) {
            throw new Error("MongoDB connection is not established");
        }

        const collections = await mongoose.connection.db.listCollections().toArray();
        const allData = {};

        for (const collection of collections) {
            const data = await mongoose.connection.db.collection(collection.name).find().toArray();
            allData[collection.name] = data;
        }

        res.render('database', { tables: collections, data: allData });
    } catch (error) {
        console.error("Error fetching database: ", error);
        res.status(500).send("Server Error");
    }
});

app.get('/api/profitable_items', async (req, res) => {
    try {
        const profitableItems = await calculateProfitableItems();

        const serializableItems = profitableItems.reduce((acc, item) => {
            acc[item.unique_name] = {
                city: item.city,
                buy_price_max: item.buy_price_max,
                sell_price_min: item.sell_price_min,
                profit: item.profit,
                profit_percentage: item.profit_percentage
            };
            return acc;
        }, {});

        res.json(serializableItems);
    } catch (error) {
        console.error("Error fetching profitable items: ", error);
        res.status(500).json({ error: "Error fetching profitable items" });
    }
});

async function calculateProfitableItems() {
    const pipeline = [
        {
            $match: {
                buy_price_max: { $gt: 0 },
                sell_price_min: { $gt: 0 }
            }
        },
        {
            $project: {
                unique_name: 1,
                city: 1,
                buy_price_max: 1,
                sell_price_min: 1,
                profit: {
                    $subtract: ['$sell_price_min', '$buy_price_max']
                },
                profit_percentage: {
                    $multiply: [
                        {
                            $divide: [
                                { $subtract: ['$sell_price_min', '$buy_price_max'] },
                                '$buy_price_max'
                            ]
                        },
                        100
                    ]
                }
            }
        },
        {
            $match: {
                profit_percentage: { $gt: 15 }
            }
        },
        {
            $sort: { profit: -1 }
        },
        {
            $limit: 100
        }
    ];

    return await Item.aggregate(pipeline);
}

app.listen(process.env.PORT || 3000, () => {
    console.log('Server is running on port 3000');
});

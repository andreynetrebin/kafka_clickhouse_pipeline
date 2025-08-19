from flask import Flask, render_template, jsonify
from clickhouse_driver import Client
import time

app = Flask(__name__)

# Подключение к ClickHouse
ch_client = Client(host='clickhouse')

@app.route('/')
def dashboard():
    return render_template('index.html')

@app.route('/api/sales')
def get_sales():
    """Получение данных о продажах за последние 24 часа"""
    query = """
    SELECT 
        toStartOfHour(event_time) as hour,
        sum(quantity) as total_quantity,
        sum(price * quantity) as revenue
    FROM sales
    WHERE event_time >= now() - INTERVAL 1 DAY
    GROUP BY hour
    ORDER BY hour
    """
    data = ch_client.execute(query)
    return jsonify({
        'labels': [row[0].strftime('%H:%M') for row in data],
        'quantity': [row[1] for row in data],
        'revenue': [round(row[2], 2) for row in data]
    })

@app.route('/api/stock')
def get_stock_movements():
    """Топ 5 товаров по движениям на складе"""
    query = """
    SELECT 
        product_id,
        sum(if(movement_type='supply', quantity, 0)) as incoming,
        sum(if(movement_type='write_off', -quantity, 0)) as outgoing
    FROM stock_movements
    GROUP BY product_id
    ORDER BY (incoming + outgoing) DESC
    LIMIT 5
    """
    data = ch_client.execute(query)
    return jsonify({
        'products': [f"Product {row[0]}" for row in data],
        'incoming': [row[1] for row in data],
        'outgoing': [row[2] for row in data]
    })

@app.route('/api/recent_sales')
def get_recent_sales():
    return jsonify(
        ch_client.execute("SELECT * FROM sales ORDER BY event_time DESC LIMIT 10")
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)


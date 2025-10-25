from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

app = Flask(__name__)
CORS(app)

# Generate sample sales data
def generate_sample_data(days=365):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Generate realistic sales data with trends and seasonality
    base_sales = 1000
    trend = np.linspace(0, 500, len(date_range))
    seasonality = 200 * np.sin(np.linspace(0, 4 * np.pi, len(date_range)))
    noise = np.random.normal(0, 100, len(date_range))
    
    sales = base_sales + trend + seasonality + noise
    sales = np.maximum(sales, 0)  # Ensure non-negative sales
    
    data = {
        'date': date_range,
        'sales': sales,
        'units_sold': (sales / random.uniform(10, 50)).astype(int),
        'category': np.random.choice(['Electronics', 'Clothing', 'Home', 'Books', 'Toys'], len(date_range))
    }
    
    return pd.DataFrame(data)

# Global variable to store data
sales_data = generate_sample_data()

@app.route('/')
def home():
    return jsonify({
        'message': 'Amazon Sales Trend Analysis API',
        'version': '1.0',
        'endpoints': {
            '/api/sales': 'Get all sales data',
            '/api/sales/summary': 'Get sales summary statistics',
            '/api/sales/trend': 'Get sales trend data',
            '/api/sales/category': 'Get sales by category'
        }
    })

@app.route('/api/sales', methods=['GET'])
def get_sales():
    # Query parameters
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    category = request.args.get('category')
    
    filtered_data = sales_data.copy()
    
    if start_date:
        filtered_data = filtered_data[filtered_data['date'] >= pd.to_datetime(start_date)]
    if end_date:
        filtered_data = filtered_data[filtered_data['date'] <= pd.to_datetime(end_date)]
    if category:
        filtered_data = filtered_data[filtered_data['category'] == category]
    
    result = filtered_data.copy()
    result['date'] = result['date'].dt.strftime('%Y-%m-%d')
    
    return jsonify({
        'data': result.to_dict(orient='records'),
        'count': len(result)
    })

@app.route('/api/sales/summary', methods=['GET'])
def get_summary():
    summary = {
        'total_sales': float(sales_data['sales'].sum()),
        'average_daily_sales': float(sales_data['sales'].mean()),
        'total_units_sold': int(sales_data['units_sold'].sum()),
        'max_sales_day': sales_data.loc[sales_data['sales'].idxmax(), 'date'].strftime('%Y-%m-%d'),
        'max_sales_amount': float(sales_data['sales'].max()),
        'min_sales_day': sales_data.loc[sales_data['sales'].idxmin(), 'date'].strftime('%Y-%m-%d'),
        'min_sales_amount': float(sales_data['sales'].min())
    }
    
    return jsonify(summary)

@app.route('/api/sales/trend', methods=['GET'])
def get_trend():
    # Group by week or month
    period = request.args.get('period', 'week')  # week or month
    
    trend_data = sales_data.copy()
    
    if period == 'month':
        trend_data['period'] = trend_data['date'].dt.to_period('M')
    else:
        trend_data['period'] = trend_data['date'].dt.to_period('W')
    
    grouped = trend_data.groupby('period').agg({
        'sales': 'sum',
        'units_sold': 'sum'
    }).reset_index()
    
    grouped['period'] = grouped['period'].astype(str)
    
    return jsonify({
        'period': period,
        'data': grouped.to_dict(orient='records')
    })

@app.route('/api/sales/category', methods=['GET'])
def get_category_sales():
    category_summary = sales_data.groupby('category').agg({
        'sales': 'sum',
        'units_sold': 'sum'
    }).reset_index()
    
    category_summary = category_summary.sort_values('sales', ascending=False)
    
    return jsonify({
        'data': category_summary.to_dict(orient='records')
    })

@app.route('/api/sales/regenerate', methods=['POST'])
def regenerate_data():
    global sales_data
    days = request.json.get('days', 365) if request.json else 365
    sales_data = generate_sample_data(days)
    
    return jsonify({
        'message': 'Data regenerated successfully',
        'records': len(sales_data)
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

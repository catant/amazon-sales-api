from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import re
import logging

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_price(price_str):
    if not price_str:
        return None
    price_str = price_str.replace('€', '').replace(',', '.').strip()
    match = re.search(r'\d+\.?\d*', price_str)
    return float(match.group()) if match else None

def extract_asin_from_url(url):
    match = re.search(r'/dp/([A-Z0-9]{10})', url)
    return match.group(1) if match else None

def scrape_amazon_product(asin):
    url = f'https://www.amazon.es/dp/{asin}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        title = soup.select_one('#productTitle')
        title = title.get_text().strip() if title else 'Título no disponible'
        
        price = soup.select_one('.a-price .a-offscreen')
        price = clean_price(price.get_text()) if price else None
        
        rating = soup.select_one('span.a-icon-alt')
        rating_text = rating.get_text() if rating else None
        rating_value = float(rating_text.split()[0].replace(',', '.')) if rating_text else None
        
        image = soup.select_one('#landingImage')
        image_url = image.get('src') if image else None
        
        return {
            'asin': asin,
            'title': title,
            'price': price,
            'currency': 'EUR',
            'rating': rating_value,
            'image_url': image_url,
            'product_url': url
        }
        
    except Exception as e:
        logger.error(f'Error scraping ASIN {asin}: {str(e)}')
        return {'error': str(e), 'asin': asin}

@app.route('/')
def home():
    return jsonify({
        'status': 'API Amazon Sales funcionando',
        'endpoints': {
            '/product/<asin>': 'Obtener info de producto por ASIN',
            '/products': 'POST con {"asins": ["B0...", "B0..."]} o {"urls": ["https://...", ...]}'
        }
    })

@app.route('/product/<asin>')
def get_product(asin):
    if not re.match(r'^[A-Z0-9]{10}$', asin):
        return jsonify({'error': 'ASIN inválido'}), 400
    
    data = scrape_amazon_product(asin)
    return jsonify(data)

@app.route('/products', methods=['POST'])
def get_products():
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'Se requiere JSON'}), 400
    
    asins = data.get('asins', [])
    urls = data.get('urls', [])
    
    if urls:
        asins.extend([extract_asin_from_url(url) for url in urls])
        asins = [a for a in asins if a]
    
    if not asins:
        return jsonify({'error': 'Se requieren ASINs o URLs'}), 400
    
    results = [scrape_amazon_product(asin) for asin in asins[:10]]
    
    return jsonify({
        'total': len(results),
        'products': results
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

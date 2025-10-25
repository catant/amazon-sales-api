from flask import Flask, jsonify, request
from flask_cors import CORS
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from collections import defaultdict
import statistics

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DE UMBRALES ---
THRESHOLDS = {
    'minAvgUnits4W': 30,            # Unidades mínimas promedio en 4 semanas para ser considerado
    'minWeeksDown': 3,              # Semanas consecutivas bajando para alerta
    'minYoYDropPct': -0.15,         # -15% caída YoY mínima para alerta (4W)
    'minNormSlopeUnits': -0.05,     # Pendiente normalizada de unidades
    'minWeeksUpReturns': 3,         # (definido pero no usado)
    'minReturnRatio': 0.08,         # 8% ratio de retornos
    'minNormSlopeReturns': 0.05,    # Pendiente normalizada de retornos
    'minRevenue4W': 1000,           # (definido pero no usado)

    # --- NUEVOS UMBRALES ---
    'minWoWDropPct': -0.12,          # -12% vs semana anterior => WARNING
    'minYoYSameWeekDropPct': -0.15,  # -15% vs misma semana del año anterior => WARNING
}

# --- CLASES DE DATOS ---
class SalesRow:
    """Representa un registro de ventas de SharePoint"""
    def __init__(self, data: Dict[str, Any]):
        # Acepta nombres directos O nombres de SharePoint (field_X)
        self.ASIN = data.get('ASIN') or data.get('Title', '')
        self.ProductTitle = data.get('ProductTitle') or data.get('field_1', '')
        self.Brand = data.get('Brand') or data.get('field_2', '')
        self.StoreCode = data.get('StoreCode') or data.get('field_3', '')
        self.Revenue = float(data.get('Revenue') or data.get('field_4', 0.0))
        self.COGS = float(data.get('COGS') or data.get('field_5', 0.0))
        self.Units = int(data.get('Units') or data.get('field_6', 0))
        self.Returns = int(data.get('Returns') or data.get('field_7', 0))
        self.WeekStart = data.get('WeekStart') or data.get('field_8', '')
        self.FiscalWeek = data.get('FiscalWeek') or data.get('field_9', '')
        
    def get_week_date(self) -> datetime:
        """Convierte WeekStart a datetime"""
        try:
            return datetime.strptime(self.WeekStart, '%Y-%m-%d')
        except Exception:
            return datetime(2000, 1, 1)
    
    def get_year(self) -> int:
        """Obtiene el año del registro"""
        return self.get_week_date().year
    
    def get_week_number(self) -> int:
        """Obtiene el número de semana del año (ISO)"""
        return self.get_week_date().isocalendar()[1]

# --- FUNCIONES AUXILIARES ---
def calculate_slope(values: List[float]) -> float:
    """
    Calcula la pendiente de una serie temporal usando regresión lineal simple.
    Retorna la tendencia (positiva = crecimiento, negativa = caída)
    """
    n = len(values)
    if n < 2:
        return 0.0
    
    sx, sy, sxy, sxx = 0.0, 0.0, 0.0, 0.0
    for i, y in enumerate(values):
        x = float(i)
        sx += x
        sy += y
        sxy += x * y
        sxx += x * x
    
    denominator = n * sxx - sx * sx
    if denominator == 0:
        return 0.0
    
    slope = (n * sxy - sx * sy) / denominator
    return slope

def calculate_yoy_change(current_weeks: List[SalesRow], previous_year_weeks: List[SalesRow], metric: str = 'Units') -> float:
    """
    Calcula el cambio año sobre año (YoY) para una métrica específica.
    Retorna el porcentaje de cambio (ej: -0.15 = -15% de caída)
    """
    current_total = sum(getattr(w, metric) for w in current_weeks)
    previous_total = sum(getattr(w, metric) for w in previous_year_weeks)
    if previous_total == 0:
        return 0.0
    return (current_total - previous_total) / previous_total

def get_last_n_weeks(rows: List[SalesRow], n: int = 4) -> List[SalesRow]:
    """Obtiene las últimas N semanas de datos ordenados por fecha"""
    sorted_rows = sorted(rows, key=lambda x: x.get_week_date())
    return sorted_rows[-n:] if len(sorted_rows) >= n else sorted_rows

def get_same_weeks_previous_year(rows: List[SalesRow], current_weeks: List[SalesRow]) -> List[SalesRow]:
    """
    Encuentra las mismas semanas del año anterior.
    Por ejemplo, si current_weeks son las semanas 40-43 de 2024, 
    busca las semanas 40-43 de 2023.
    """
    if not current_weeks:
        return []
    
    # Rango de semanas actuales
    current_week_numbers = {w.get_week_number() for w in current_weeks}
    current_year = current_weeks[0].get_year()
    target_year = current_year - 1
    
    # Filtrar las semanas del año anterior que coincidan
    previous_year_weeks = [
        row for row in rows 
        if row.get_year() == target_year and row.get_week_number() in current_week_numbers
    ]
    return sorted(previous_year_weeks, key=lambda x: x.get_week_date())

def detect_consecutive_weeks_down(weeks: List[SalesRow], min_weeks: int = 3) -> bool:
    """Detecta si hay tendencia descendente consecutiva en unidades"""
    if len(weeks) < min_weeks:
        return False
    
    units = [w.Units for w in weeks]
    consecutive_down = 0
    for i in range(1, len(units)):
        if units[i] < units[i-1]:
            consecutive_down += 1
            if consecutive_down >= min_weeks - 1:
                return True
        else:
            consecutive_down = 0
    return False

def calculate_return_rate(weeks: List[SalesRow]) -> float:
    """Calcula el ratio de devoluciones sobre unidades vendidas"""
    total_units = sum(w.Units for w in weeks)
    total_returns = sum(w.Returns for w in weeks)
    if total_units == 0:
        return 0.0
    return total_returns / total_units

def find_same_week_previous_year(previous_year_weeks: List[SalesRow], current_week: SalesRow) -> Optional[SalesRow]:
    """Devuelve el registro de la misma semana ISO del año anterior si existe"""
    target_week_num = current_week.get_week_number()
    for row in previous_year_weeks:
        if row.get_week_number() == target_week_num:
            return row
    return None

# --- ANÁLISIS PRINCIPAL ---
def analyze_sales_trends(rows: List[SalesRow]) -> List[Dict[str, Any]]:
    """
    Analiza las tendencias de ventas por ASIN y StoreCode.
    Compara las últimas 4 semanas con el mismo período del año anterior.
    """
    # 1. Agrupar datos por ASIN|StoreCode
    grouped_data = defaultdict(list)
    for row in rows:
        key = f"{row.ASIN}|{row.StoreCode}"
        grouped_data[key].append(row)
    
    alerts = []
    
    for key, data_list in grouped_data.items():
        asin, store = key.split('|')
        
        # Ordenar por fecha
        data_list.sort(key=lambda x: x.get_week_date())
        
        # Obtener las últimas 4 semanas
        last_4_weeks = get_last_n_weeks(data_list, 4)
        if len(last_4_weeks) < 4:
            continue  # No hay suficientes datos para analizar
        
        # Obtener las mismas 4 semanas del año anterior
        previous_year_weeks = get_same_weeks_previous_year(data_list, last_4_weeks)
        
        # --- MÉTRICAS ACTUALES (últimas 4 semanas) ---
        units_current = [w.Units for w in last_4_weeks]
        revenue_current = [w.Revenue for w in last_4_weeks]
        returns_current = [w.Returns for w in last_4_weeks]
        
        avg_units = statistics.mean(units_current)
        total_revenue = sum(revenue_current)
        total_units = sum(units_current)
        total_returns = sum(returns_current)
        
        # --- TENDENCIAS ---
        units_slope = calculate_slope(units_current)
        returns_slope = calculate_slope(returns_current)
        
        # Normalizar pendientes (dividir por el promedio para tener % de cambio)
        normalized_units_slope = units_slope / avg_units if avg_units > 0 else 0.0
        avg_returns = statistics.mean(returns_current) if returns_current else 1.0
        normalized_returns_slope = returns_slope / avg_returns if avg_returns > 0 else 0.0
        
        # --- COMPARACIÓN YOY (4 semanas) ---
        yoy_units_change = 0.0
        yoy_revenue_change = 0.0
        yoy_data_available = len(previous_year_weeks) >= 3  # Requisito mínimo para YoY
        
        if yoy_data_available:
            yoy_units_change = calculate_yoy_change(last_4_weeks, previous_year_weeks, 'Units')
            yoy_revenue_change = calculate_yoy_change(last_4_weeks, previous_year_weeks, 'Revenue')
        
        # --- RATIO DE DEVOLUCIONES ---
        return_rate = calculate_return_rate(last_4_weeks)
        
        # --- DETECCIÓN DE ALERTAS ---
        alert_reasons = []
        alert_severity = 'INFO'
        
        # ALERTA 1: Tendencia negativa significativa en unidades
        if (normalized_units_slope < THRESHOLDS['minNormSlopeUnits'] and 
            avg_units >= THRESHOLDS['minAvgUnits4W']):
            alert_reasons.append(f"Tendencia negativa en ventas ({normalized_units_slope:.2%} por semana)")
            alert_severity = 'WARNING'
        
        # ALERTA 2: Caída YoY significativa (4W)
        if (yoy_data_available and yoy_units_change < THRESHOLDS['minYoYDropPct']):
            alert_reasons.append(f"Caída YoY de {yoy_units_change:.1%} en unidades (4 semanas)")
            alert_severity = 'CRITICAL'
        
        # ALERTA 3: Semanas consecutivas bajando
        if detect_consecutive_weeks_down(last_4_weeks, THRESHOLDS['minWeeksDown']):
            alert_reasons.append(f"{THRESHOLDS['minWeeksDown']}+ semanas consecutivas bajando")
            if alert_severity == 'INFO':
                alert_severity = 'WARNING'
        
        # ALERTA 4: Alto ratio de devoluciones
        if return_rate > THRESHOLDS['minReturnRatio']:
            alert_reasons.append(f"Alto ratio de devoluciones ({return_rate:.1%})")
            if alert_severity == 'INFO':
                alert_severity = 'WARNING'
        
        # ALERTA 5: Devoluciones en tendencia creciente
        if (normalized_returns_slope > THRESHOLDS['minNormSlopeReturns'] and 
            total_returns > 5):
            alert_reasons.append(f"Devoluciones creciendo ({normalized_returns_slope:.2%} por semana)")
            if alert_severity == 'INFO':
                alert_severity = 'WARNING'

        # --- NUEVAS ALERTAS: comparativas puntuales con la última semana ---
        last_week = last_4_weeks[-1]
        prev_week = last_4_weeks[-2]

        # 5.A) WARNING por caída WoW (última vs anterior)
        wow_change = None
        if prev_week.Units > 0:
            wow_change = (last_week.Units - prev_week.Units) / prev_week.Units
            if (wow_change < THRESHOLDS['minWoWDropPct'] and
                avg_units >= THRESHOLDS['minAvgUnits4W']):
                alert_reasons.append(
                    f"Bajada WoW de {wow_change:.1%} (semana {last_week.FiscalWeek} vs {prev_week.FiscalWeek})"
                )
                if alert_severity == 'INFO':
                    alert_severity = 'WARNING'

        # 5.B) WARNING por caída vs misma semana del año anterior (si existe)
        yoy_same_week_change = None
        same_week_prev_year = find_same_week_previous_year(previous_year_weeks, last_week) if previous_year_weeks else None
        if same_week_prev_year and same_week_prev_year.Units > 0:
            yoy_same_week_change = (last_week.Units - same_week_prev_year.Units) / same_week_prev_year.Units
            if (yoy_same_week_change < THRESHOLDS['minYoYSameWeekDropPct'] and
                avg_units >= THRESHOLDS['minAvgUnits4W']):
                alert_reasons.append(
                    f"Bajada vs misma semana del año anterior de {yoy_same_week_change:.1%} "
                    f"({last_week.FiscalWeek} vs {same_week_prev_year.FiscalWeek})"
                )
                if alert_severity == 'INFO':
                    alert_severity = 'WARNING'
        
        # Solo agregar a alertas si hay alguna razón
        if alert_reasons:
            product_info = {
                'ASIN': asin,
                'ProductTitle': last_4_weeks[0].ProductTitle,
                'Brand': last_4_weeks[0].Brand,
                'StoreCode': store,
                'Severity': alert_severity,
                'AlertReasons': alert_reasons,
                
                # Métricas actuales (últimas 4 semanas)
                'Current_4W': {
                    'AvgUnitsPerWeek': round(avg_units, 2),
                    'TotalUnits': total_units,
                    'TotalRevenue': round(total_revenue, 2),
                    'TotalReturns': total_returns,
                    'ReturnRate': round(return_rate, 3),
                    'UnitsTrend': round(normalized_units_slope, 4),
                    'ReturnsTrend': round(normalized_returns_slope, 4),
                },
                
                # Comparación YoY 4W
                'YoY_Comparison': {
                    'UnitsChange': round(yoy_units_change, 3),
                    'RevenueChange': round(yoy_revenue_change, 3),
                    'DataAvailable': yoy_data_available
                },

                # Comparativas puntuales (nuevas)
                'Comparisons': {
                    'WoWUnitsChange': round(wow_change, 3) if wow_change is not None else None,
                    'YoYSameWeekUnitsChange': round(yoy_same_week_change, 3) if yoy_same_week_change is not None else None,
                    'YoYSameWeekDataAvailable': same_week_prev_year is not None
                },
                
                # Detalle semanal
                'WeeklyDetail': [
                    {
                        'Week': w.FiscalWeek,
                        'Date': w.WeekStart,
                        'Units': w.Units,
                        'Revenue': round(w.Revenue, 2),
                        'Returns': w.Returns
                    } for w in last_4_weeks
                ]
            }
            alerts.append(product_info)
    
    # Ordenar por severidad y luego por caída YoY 4W
    severity_order = {'CRITICAL': 0, 'WARNING': 1, 'INFO': 2}
    alerts.sort(key=lambda x: (
        severity_order.get(x['Severity'], 3),
        x['YoY_Comparison']['UnitsChange']
    ))
    
    return alerts

# --- ENDPOINTS DE LA API ---
@app.route('/', methods=['GET'])
def home():
    """Endpoint de información"""
    return jsonify({
        'status': 'Sales Trend Analysis API',
        'version': '1.1',  # versión incrementada por nuevas reglas
        'endpoints': {
            '/analyze': 'POST - Analizar tendencias de ventas',
            '/health': 'GET - Health check'
        },
        'input_format': {
            'Opción 1 (Power Automate Select)': {
                'body': [
                    {
                        'ASIN': 'B0ABC123',
                        'ProductTitle': 'Product Name',
                        'Brand': 'Brand Name',
                        'StoreCode': 'IT',
                        'Revenue': 1234.56,
                        'Units': 100,
                        'Returns': 5,
                        'WeekStart': '2024-09-30',
                        'FiscalWeek': '2024-W40'
                    }
                ]
            },
            'Opción 2 (SharePoint)': {
                'body': {
                    'value': [
                        {
                            'Title': 'B0ABC123',
                            'field_1': 'Product Name',
                            'field_2': 'Brand Name',
                            'field_3': 'IT',
                            'field_4': 1234.56,
                            'field_6': 100,
                            'field_7': 5,
                            'field_8': '2024-09-30',
                            'field_9': '2024-W40'
                        }
                    ]
                }
            }
        }
    })

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@app.route('/analyze', methods=['POST'])
def analyze():
    """
    Endpoint principal para analizar tendencias de ventas.
    Espera JSON de Power Automate con datos de SharePoint.
    """
    try:
        req_body = request.get_json()
        if not req_body:
            return jsonify({'error': 'Se requiere un cuerpo JSON'}), 400
        
        # Extraer el array de datos - ACEPTA MÚLTIPLES FORMATOS
        if 'body' in req_body:
            if isinstance(req_body['body'], list):
                raw_items = req_body['body']  # ✅ {"body": [...]} - TU FORMATO
            elif isinstance(req_body['body'], dict) and 'value' in req_body['body']:
                raw_items = req_body['body']['value']  # ✅ {"body": {"value": [...]}}
            else:
                raw_items = req_body['body']
        elif 'value' in req_body:
            raw_items = req_body['value']  # ✅ {"value": [...]}
        elif isinstance(req_body, list):
            raw_items = req_body  # ✅ [...]
        else:
            return jsonify({'error': 'Formato JSON no reconocido. Se espera {body: [...]}, {body: {value: [...]}}, {value: [...]}, o [...]'}), 400
        
        if not raw_items:
            return jsonify({'error': 'No se encontraron datos para analizar'}), 400
        
        logger.info(f"Procesando {len(raw_items)} registros")
        
        # Convertir a objetos SalesRow
        sales_rows = []
        for item in raw_items:
            try:
                sales_rows.append(SalesRow(item))
            except Exception as e:
                logger.warning(f"Error procesando registro: {e}")
                continue
        
        if not sales_rows:
            return jsonify({'error': 'No se pudieron procesar los registros'}), 400
        
        # Ejecutar análisis
        alerts = analyze_sales_trends(sales_rows)
        
        logger.info(f"Análisis completado: {len(alerts)} alertas generadas")
        
        # Preparar respuesta
        response = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_records_processed': len(sales_rows),
                'total_alerts': len(alerts),
                'critical_alerts': sum(1 for a in alerts if a['Severity'] == 'CRITICAL'),
                'warning_alerts': sum(1 for a in alerts if a['Severity'] == 'WARNING'),
            },
            'alerts': alerts
        }
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Error en análisis: {str(e)}", exc_info=True)
        return jsonify({
            'error': 'Error interno del servidor',
            'details': str(e)
        }), 500

if __name__ == '__main__':
    # Ajusta host/port según tu despliegue
    app.run(host='0.0.0.0', port=5000, debug=True)

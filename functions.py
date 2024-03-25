import sqlite3
import numpy as np


def get_db_connection():
    conn = sqlite3.connect('db.sqlite')
    conn.row_factory = sqlite3.Row
    return conn


# в наличии и их среднее количество
def calculate_median_orders(conn, nmId, start_date, end_date):
    stock_query = '''
    SELECT date
    FROM stock_history
    WHERE nmId = ? AND date BETWEEN ? AND ? AND count > (
        SELECT if_available FROM PRODUCTS WHERE nmId = ?)
    GROUP BY date;
    '''
    stock_dates = conn.execute(stock_query, (nmId, start_date, end_date, nmId)).fetchall()

    order_counts = []
    for date in stock_dates:
        order_query = '''
        SELECT COUNT(*) as order_count
        FROM ORDERS
        WHERE nmId = ? AND date(date) = date(?);
        '''
        order_result = conn.execute(order_query, (nmId, date['date'])).fetchone()
        order_counts.append(order_result['order_count'] if order_result else 0)
    
    median_orders = np.median(order_counts) if order_counts else 0
    return median_orders


# Текущий остаток получаем 
def get_current_stock(conn, nmId):
    current_stock_query = '''
    SELECT SUM(cs.count) AS total_stock
    FROM current_stock cs
    WHERE cs.nmId = ?
    '''
    current_stock_result = conn.execute(current_stock_query, (nmId,)).fetchone()
    return current_stock_result['total_stock'] if current_stock_result and current_stock_result['total_stock'] is not None else "Нет данных"


def get_problematic_warehouses(conn, nmId, start_date, end_date):
    if_available_result = conn.execute('SELECT if_available FROM PRODUCTS WHERE nmId = ?', (nmId,)).fetchone()
    if_available = if_available_result['if_available'] if if_available_result else 0

    stock_history_results = conn.execute(
        '''
        SELECT warehouseName, date, count
        FROM stock_history
        WHERE nmId = ? AND date BETWEEN ? AND ? AND count > ?
        ''',
        (nmId, start_date, end_date, if_available)
    ).fetchall()

    # среднее количество заказов для каждого склада
    average_orders_per_warehouse = {}
    for record in stock_history_results:
        orders_result = conn.execute(
            '''
            SELECT COUNT(*) as order_count
            FROM ORDERS
            WHERE nmId = ? AND date(date) = date(?) AND warehouseName = ?
            ''',
            (nmId, record['date'], record['warehouseName'])
        ).fetchone()
        warehouse = record['warehouseName']
        if warehouse not in average_orders_per_warehouse:
            average_orders_per_warehouse[warehouse] = []
        average_orders_per_warehouse[warehouse].append(orders_result['order_count'] if orders_result else 0)

    # среднее количество заказов за две недели для каждого склада и  проблемные склады
    problematic_warehouses = []
    for warehouse, orders in average_orders_per_warehouse.items():
        avg_orders = sum(orders) / len(orders) if orders else 0
        current_stock_result = conn.execute(
            'SELECT count FROM current_stock WHERE nmId = ? AND warehouseName = ?;',
            (nmId, warehouse)
        ).fetchone()
        if current_stock_result and avg_orders > 0:
            days_left = current_stock_result['count'] / avg_orders
            if days_left < 15 or days_left > 30:
                problematic_warehouses.append(f"{warehouse} {int(days_left)}")

    return problematic_warehouses


# для заказы
def get_orders_data(conn, nmId, date):
    orders_query = '''
        SELECT COUNT(*) as order_count
        FROM ORDERS
        WHERE nmId = ? AND date(date) = date(?);
    '''
    result = conn.execute(orders_query, (nmId, date)).fetchone()
    return result['order_count'] if result else 0


# для Изменения
def get_changes_data(conn, nmId, date):
    changes_query = '''
        SELECT COALESCE(GROUP_CONCAT(text, '; '), 'нет') as changes
        FROM changes
        WHERE nmId = ? AND strftime('%Y-%m-%d', date) = ?;
    '''
    result = conn.execute(changes_query, (nmId, date)).fetchone()
    return result['changes']


# для ROI
def get_roi_data(conn, nmId, date):
    # Расчёт прибыли
    profit_query = '''
        SELECT COALESCE(SUM(profit), 0) as total_profit
        FROM ORDERS
        WHERE nmId = ? AND date(date) = date(?);
    '''
    profit_result = conn.execute(profit_query, (nmId, date)).fetchone()
    total_profit = profit_result['total_profit']
    
    # Расчёт затрат
    costs_query = '''
        SELECT (SELECT COALESCE(SUM(cost_price), 0) FROM ORDERS WHERE nmId = ? AND date(date) = date(?)) 
        + (SELECT COALESCE(SUM(Sum), 0) FROM ads_stats WHERE nmId = ? AND date(date) = date(?)) as total_costs;
    '''
    costs_result = conn.execute(costs_query, (nmId, date, nmId, date)).fetchone()
    total_costs = costs_result['total_costs']

    #  ROI
    roi = (total_profit / total_costs) if total_costs > 0 else 0
    return roi


# Получение прибыли без учета рекламы
def get_net_profit(conn, nmId, date):
    profit_query = '''
        SELECT COALESCE(SUM(profit), 0) as total_profit
        FROM ORDERS
        WHERE nmId = ? AND date(date) = date(?);
    '''
    profit_result = conn.execute(profit_query, (nmId, date)).fetchone()
    total_profit = profit_result['total_profit'] if profit_result else 0

    ad_spending_query = '''
        SELECT COALESCE(SUM(Sum), 0) as ad_spending
        FROM ads_stats
        WHERE nmId = ? AND date(date) = date(?);
    '''
    ad_spending_result = conn.execute(ad_spending_query, (nmId, date)).fetchone()
    ad_spending = ad_spending_result['ad_spending'] if ad_spending_result else 0

    net_profit = total_profit - ad_spending
    return int(round(net_profit))


# прибыли без учета рекламы из orders
def get_profit_without_ads(conn, nmId, date):
    profit_query = '''
        SELECT COALESCE(SUM(profit), 0) as total_profit
        FROM ORDERS
        WHERE nmId = ? AND date(date) = date(?);
    '''
    profit_result = conn.execute(profit_query, (nmId, date)).fetchone()
    return int(round(profit_result['total_profit'] )) if profit_result else 0


# получаю Расходы на рекламу
def get_ad_spending(conn, nmId, date):
    ad_spending_query = '''
        SELECT COALESCE(SUM(Sum), 0) as ad_spending
        FROM ads_stats
        WHERE nmId = ? AND date(date) = date(?);
    '''
    ad_spending_result = conn.execute(ad_spending_query, (nmId, date)).fetchone()
    return int(round(ad_spending_result['ad_spending'])) if ad_spending_result else 0


# Количество рекламных показов
def get_ad_spending_views(conn, nmId, date):
    ad_spending_query = '''
        SELECT COALESCE(SUM(Views), 0) as ad_spending
        FROM ads_stats
        WHERE nmId = ? AND date(date) = date(?);
    '''
    ad_spending_result = conn.execute(ad_spending_query, (nmId, date)).fetchone()
    return int(round(ad_spending_result['ad_spending'])) if ad_spending_result else 0


# Количество рекламных кликов
def get_ad_spending_licks(conn, nmId, date):
    ad_spending_query = '''
        SELECT COALESCE(SUM(Clicks), 0) as ad_spending
        FROM ads_stats
        WHERE nmId = ? AND date(date) = date(?);
    '''
    ad_spending_result = conn.execute(ad_spending_query, (nmId, date)).fetchone()
    return int(round(ad_spending_result['ad_spending']))  if ad_spending_result else 0


# Общее количество кликов
def get_ad_spending_licks_all(conn, nmId, date):
    ad_spending_query = '''
        SELECT COALESCE(SUM(OpenCardCount), 0) as ad_spending
        FROM stats
        WHERE nmId = ? AND date(date) = date(?);
    '''
    ad_spending_result = conn.execute(ad_spending_query, (nmId, date)).fetchone()
    return int(round(ad_spending_result['ad_spending'])) if ad_spending_result else 0


# Рекламные клики / рекламные показы из таблицы ads_stats 
def get_ad_spending_licks_views(conn, nmId, date):
    ad_spending_query = '''
        SELECT COALESCE(SUM(Clicks), 0) as total_clicks, COALESCE(SUM(Views), 0) as total_views
        FROM ads_stats
        WHERE nmId = ? AND date(date) = date(?);
    '''
    ad_spending_result = conn.execute(ad_spending_query, (nmId, date)).fetchone()
    if ad_spending_result and ad_spending_result['total_views'] > 0:
       tik = ad_spending_result['total_clicks'] / ad_spending_result['total_views']
       return int(round(tik))
    else:
        return 0
    

# Расходы на рекламу / рекламные клики из таблицы ads_stats
def get_ad_spending_licks_views_all(conn, nmId, date):
    ad_spending_query = '''
        SELECT COALESCE(SUM(Sum), 0) as total_clicks, COALESCE(SUM(Clicks), 0) as total_views
        FROM ads_stats
        WHERE nmId = ? AND date(date) = date(?);
    '''
    ad_spending_result = conn.execute(ad_spending_query, (nmId, date)).fetchone()
    if ad_spending_result and ad_spending_result['total_views'] > 0:
        tik = ad_spending_result['total_clicks'] / ad_spending_result['total_views']
        return int(round(tik))
    else:
        return 0
    

#Добавления в корзину / Общее количество кликов из таблицы  stats
def icks_views_all_stats(conn, nmId, date):
    ad_spending_query = '''
        SELECT COALESCE(SUM(addToCartCount), 0) as total_clicks, COALESCE(SUM(OpenCardCount), 0) as total_views
        FROM stats
        WHERE nmId = ? AND date(date) = date(?);
    '''
    ad_spending_result = conn.execute(ad_spending_query, (nmId, date)).fetchone()
    if ad_spending_result and ad_spending_result['total_views'] > 0:
        tik = ad_spending_result['total_clicks'] / ad_spending_result['total_views']
        return int(round(tik))
    else:
        return 0
    
#общее количество заказов для данного nmId и даты
def get_conversion_to_order(conn, nmId, date):

    orders_query = '''
        SELECT COUNT(*) as total_orders
        FROM ORDERS
        WHERE nmId = ? AND date(date) = date(?);
    '''
    orders_result = conn.execute(orders_query, (nmId, date)).fetchone()
    total_orders = orders_result['total_orders'] if orders_result else 0

    cart_adds_query = '''
        SELECT COUNT(*) as total_cart_adds
        FROM stats
        WHERE nmId = ? AND date(date) = date(?);
    '''
    cart_adds_result = conn.execute(cart_adds_query, (nmId, date)).fetchone()
    total_cart_adds = cart_adds_result['total_cart_adds'] if cart_adds_result else 0

    conversion_to_order = (total_orders / total_cart_adds) if total_cart_adds > 0 else 0
    return int(round(conversion_to_order))


#количества выкупов и заказов для данного nmId и даты
def get_conversion_to_purchase(conn, nmId, date):
    conversion_query = '''
        SELECT 
            COALESCE(SUM(buyoutCount), 0) as total_buyouts,
            COALESCE(SUM(ordersCount), 0) as total_orders
        FROM stats
        WHERE nmId = ? AND date(date) = date(?);
    '''
    conversion_result = conn.execute(conversion_query, (nmId, date)).fetchone()
    total_buyouts = conversion_result['total_buyouts'] if conversion_result else 0
    total_orders = conversion_result['total_orders'] if conversion_result else 0

    conversion_to_purchase = (total_buyouts / total_orders) if total_orders > 0 else 0
    return int(round(conversion_to_purchase)) 


#Cклады с наличием
def get_warehouses_with_stock(conn, nmId, date):
    if_available_result = conn.execute(
        'SELECT if_available FROM PRODUCTS WHERE nmId = ?',
        (nmId,)
    ).fetchone()
    if_available = if_available_result['if_available'] if if_available_result else 0

    warehouses_query = '''
        SELECT COUNT(DISTINCT warehouseName) as warehouse_count
        FROM stock_history
        WHERE nmId = ? AND date(date) = date(?) AND count > ?
    '''
    warehouses_result = conn.execute(warehouses_query, (nmId, date, if_available)).fetchone()
    warehouse_count = warehouses_result['warehouse_count'] if warehouses_result else 0

    return int(round(warehouse_count))  

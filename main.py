from flask import Flask, render_template
from datetime import datetime, timedelta
import sqlite3
import numpy as np
from flask import request
from functions import *


app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    conn = get_db_connection()

    today = datetime.now().date().strftime('%Y-%m-%d')
    end_date_dates = datetime.now().date()
    start_date = request.args.get('start_date', (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    date_range = [(datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=i)).strftime('%Y-%m-%d') for i in range((datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days + 1)]

    dates = [(end_date_dates - timedelta(days=x)).strftime('%Y-%m-%d') for x in range(20)]
    products_query = 'SELECT nmId, image, subject, KPI FROM PRODUCTS;'
    products = conn.execute(products_query).fetchall()

    fixed_start_date = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
    fixed_end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    

    items = []
    for product in products:
        median_orders = calculate_median_orders(conn, product['nmId'], start_date, end_date)
        required_stock = median_orders * 30

        total_stock = get_current_stock(conn, product['nmId'])
        problematic_warehouses = get_problematic_warehouses(conn, product['nmId'], fixed_start_date, fixed_end_date)

        orders_data = {date: get_orders_data(conn, product['nmId'], date) for date in dates}
        changes_data = {date: get_changes_data(conn, product['nmId'], date) for date in dates}
        roi_data = {date: get_roi_data(conn, product['nmId'], date) for date in dates}

        items.append({
            'nmId': product['nmId'],
            'image': product['image'],
            'subject': product['subject'],
            'KPI': product['KPI'],
            'average_orders': median_orders,  
            'required_stock': required_stock,
            'current_stock': total_stock,
            'problematic_warehouses': problematic_warehouses,
            'orders_data': orders_data,
            'roi_data': roi_data,
            'changes_data': changes_data
        })

    conn.close()
    return render_template('index.html', items=items, date_range = date_range, today=today, dates=dates)


@app.route('/voronki', methods=['GET'])
def voronki():
    conn = get_db_connection()

    end_date_dates = datetime.now().date()
    dates = [(end_date_dates - timedelta(days=x)).strftime('%Y-%m-%d') for x in range(20)]

    products_query = 'SELECT nmId, image FROM PRODUCTS;'
    products = conn.execute(products_query).fetchall()

    items = []
    for product in products:
        net_profits = {}
        profits_without_ads = {}
        orders_data = {}
        ad_spending = {}
        ad_views = {}
        ad_licks = {}
        ad_licks_all = {}
        ad_licks_licks_views = {}
        ad_licks_tik = {}
        ad_licks_stats = {}
        conversion_to_order = {}
        conversion_to_purchase = {}
        warehouses_with_stock = {}
        changes_data = {}
        
        for date in dates:
            orders_data[date] = get_orders_data(conn, product['nmId'], date)
            net_profits[date] = get_net_profit(conn, product['nmId'], date)
            profits_without_ads[date] = get_profit_without_ads(conn, product['nmId'], date)
            ad_spending[date] = get_ad_spending(conn, product['nmId'], date)
            ad_views[date] = get_ad_spending_views(conn, product['nmId'], date)
            ad_licks[date] = get_ad_spending_licks(conn, product['nmId'], date)
            ad_licks_all[date] = get_ad_spending_licks_all(conn, product['nmId'], date)
            ad_licks_licks_views[date] = get_ad_spending_licks_views(conn, product['nmId'], date)
            ad_licks_tik[date] = get_ad_spending_licks_views_all(conn, product['nmId'], date)
            ad_licks_stats[date] = icks_views_all_stats(conn, product['nmId'], date)
            conversion_to_order[date] = get_conversion_to_order(conn, product['nmId'], date)
            conversion_to_purchase[date] = get_conversion_to_purchase(conn, product['nmId'], date)
            warehouses_with_stock[date] = get_warehouses_with_stock(conn, product['nmId'], date)
            changes_data[date] = get_changes_data(conn, product['nmId'], date)
            
        items.append({
            'nmId': product['nmId'],
            'image': product['image'],
            'orders_data': orders_data, 
            'net_profits': net_profits,
            'profits_without_ads': profits_without_ads,
            'ad_spending': ad_spending,
            'ad_views': ad_views,
            'ad_licks': ad_licks,
            'ad_licks_all': ad_licks_all,
            'ad_licks_licks_views': ad_licks_licks_views,
            'ad_licks_tik': ad_licks_tik,
            'ad_licks_stats': ad_licks_stats,
            'conversion_to_order': conversion_to_order,
            'conversion_to_purchase': conversion_to_purchase,
            'warehouses_with_stock': warehouses_with_stock,
            'changes_data': changes_data,
        })

    conn.close()
    return render_template('voronki.html', items=items, dates=dates)


if __name__ == '__main__':
    app.run(debug=True)
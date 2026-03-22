from flask import Flask, render_template, redirect, url_for, session, request, jsonify, flash
from functools import wraps
import os
import webbrowser
import threading
from dotenv import load_dotenv
import functions as f
from database import init_db
import pandas as pd
import datetime
import json

# Allow OAuth over plain HTTP on localhost
os.environ.setdefault('OAUTHLIB_INSECURE_TRANSPORT', '1')

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'local-secret-key-change-me')

COSTURERAS = ['Alicia', 'Dani', 'Manuela', 'Mari', 'Marlen', 'M.Antonia', 'Marta']
PAYMENT_OPTIONS = ['No Pagado', 'Efectivo', 'Tarjeta', 'Bizum']


def login_required(func):
    @wraps(func)
    def decorated(*args, **kwargs):
        if 'logged_in' not in session:
            return redirect(url_for('login'))
        return func(*args, **kwargs)
    return decorated


def df_to_records(df):
    if df is None or df.empty:
        return []
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient='records')


# ─── Auth Routes ──────────────────────────────────────────────────────────────

@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'logged_in' in session:
        return redirect(url_for('home'))
    if request.method == 'POST':
        password = request.form.get('password', '')
        correct = os.environ.get('APP_PASSWORD', 'marta1234')
        if password == correct:
            session['logged_in'] = True
            return redirect(url_for('home'))
        flash('Contraseña incorrecta', 'danger')
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


# ─── Page Routes ──────────────────────────────────────────────────────────────

@app.route('/')
@login_required
def home():
    try:
        orders_df = f.get_orders_data()
        clients_df = f.get_clients_data()
        articles_df = f.get_articles_data()
        joined = f.ordersJoin(orders_df.copy(), clients_df.copy(), articles_df.copy())

        # Active orders only (Recogida_Cliente is null)
        active = joined[joined['Recogida_Cliente'].isna()].copy()

        # Column 1: Local para costurera (Entrega_Proveedor is null)
        col1 = active[active['Entrega_Proveedor'].isna()].copy()

        # Column 2: En la costurera (Entrega_Proveedor not null AND Recogida_Proveedor is null)
        col2 = active[active['Entrega_Proveedor'].notna() & active['Recogida_Proveedor'].isna()].copy()

        # Column 3: Local para entregar (Recogida_Proveedor not null AND Entrega_Cliente not null)
        col3 = active[active['Recogida_Proveedor'].notna() & active['Entrega_Cliente'].notna()].copy()

        return render_template(
            'home.html',
            col1=df_to_records(col1),
            col2=df_to_records(col2),
            col3=df_to_records(col3),
            costureras=COSTURERAS,
            payment_options=PAYMENT_OPTIONS
        )
    except Exception as e:
        flash(f'Error al cargar datos: {str(e)}', 'danger')
        return render_template('home.html', col1=[], col2=[], col3=[], costureras=COSTURERAS, payment_options=PAYMENT_OPTIONS)


@app.route('/articulos')
@login_required
def articulos():
    try:
        df = f.get_articles_data()
        records = df_to_records(df)
        return render_template('articulos.html', articulos=records)
    except Exception as e:
        flash(f'Error al cargar artículos: {str(e)}', 'danger')
        return render_template('articulos.html', articulos=[])


@app.route('/clientes')
@login_required
def clientes():
    try:
        df = f.get_clients_data()
        records = df_to_records(df)
        return render_template('clientes.html', clientes=records)
    except Exception as e:
        flash(f'Error al cargar clientes: {str(e)}', 'danger')
        return render_template('clientes.html', clientes=[])


@app.route('/buscar-pedidos')
@login_required
def buscar_pedidos():
    try:
        clients_df = f.get_clients_data()
        articles_df = f.get_articles_data()
        clients = df_to_records(clients_df)
        articles = df_to_records(articles_df)
        return render_template(
            'buscar_pedidos.html',
            pedidos=[],
            clients=clients,
            articles=articles,
            costureras=COSTURERAS,
            payment_options=PAYMENT_OPTIONS,
            searched=False
        )
    except Exception as e:
        flash(f'Error: {str(e)}', 'danger')
        return render_template('buscar_pedidos.html', pedidos=[], clients=[], articles=[], costureras=COSTURERAS, payment_options=PAYMENT_OPTIONS, searched=False)


@app.route('/buscar-pedidos/search', methods=['POST'])
@login_required
def buscar_pedidos_search():
    try:
        orders_df = f.get_orders_data()
        clients_df = f.get_clients_data()
        articles_df = f.get_articles_data()
        joined = f.ordersJoin(orders_df.copy(), clients_df.copy(), articles_df.copy())

        cliente_filter = request.form.get('cliente', '').strip()
        articulo_filter = request.form.get('articulo', '').strip()
        proveedor_filter = request.form.get('proveedor', '').strip()
        pagado_filter = request.form.get('pagado', '').strip()
        fecha_desde = request.form.get('fecha_desde', '').strip()
        fecha_hasta = request.form.get('fecha_hasta', '').strip()
        solo_activos = request.form.get('solo_activos') == 'on'

        df = joined.copy()

        if cliente_filter:
            df = df[df['Cliente'].apply(lambda x: f.normalize_string(cliente_filter) in f.normalize_string(str(x)))]
        if articulo_filter:
            df = df[df['Articulo'].apply(lambda x: f.normalize_string(articulo_filter) in f.normalize_string(str(x)))]
        if proveedor_filter:
            df = df[df['Proveedor'] == proveedor_filter]
        if pagado_filter:
            df = df[df['Pagado'] == pagado_filter]
        if fecha_desde:
            df = df[df['Entrega_Cliente'] >= fecha_desde]
        if fecha_hasta:
            df = df[df['Entrega_Cliente'] <= fecha_hasta]
        if solo_activos:
            df = df[df['Recogida_Cliente'].isna()]

        clients = df_to_records(clients_df)
        articles = df_to_records(articles_df)
        pedidos = df_to_records(df)

        return render_template(
            'buscar_pedidos.html',
            pedidos=pedidos,
            clients=clients,
            articles=articles,
            costureras=COSTURERAS,
            payment_options=PAYMENT_OPTIONS,
            searched=True,
            filters={
                'cliente': cliente_filter,
                'articulo': articulo_filter,
                'proveedor': proveedor_filter,
                'pagado': pagado_filter,
                'fecha_desde': fecha_desde,
                'fecha_hasta': fecha_hasta,
                'solo_activos': solo_activos
            }
        )
    except Exception as e:
        flash(f'Error en la búsqueda: {str(e)}', 'danger')
        return redirect(url_for('buscar_pedidos'))


@app.route('/insertar-pedidos')
@login_required
def insertar_pedidos():
    try:
        clients_df = f.get_clients_data()
        articles_df = f.get_articles_data()
        orders_df = f.get_orders_data()
        joined = f.ordersJoin(orders_df.copy(), clients_df.copy(), articles_df.copy())

        today_str = datetime.date.today().isoformat()
        today_orders = joined[joined['Entrega_Cliente'] == today_str] if 'Entrega_Cliente' in joined.columns else pd.DataFrame()

        clients = df_to_records(clients_df)
        articles = df_to_records(articles_df)
        today_orders_records = df_to_records(today_orders)

        return render_template(
            'insertar_pedidos.html',
            clients=clients,
            articles=articles,
            today_orders=today_orders_records,
            costureras=COSTURERAS,
            today=today_str
        )
    except Exception as e:
        flash(f'Error: {str(e)}', 'danger')
        return render_template('insertar_pedidos.html', clients=[], articles=[], today_orders=[], costureras=COSTURERAS, today=datetime.date.today().isoformat())


@app.route('/contabilidad')
@login_required
def contabilidad():
    try:
        orders_df = f.get_orders_data()

        # Filter orders with Recogida_Cliente not null (completed orders)
        completed = orders_df[orders_df['Recogida_Cliente'].notna()].copy()

        # Daily revenue/costs grouped by Recogida_Cliente
        daily_data = {}
        if not completed.empty:
            completed['Recogida_Cliente'] = pd.to_datetime(completed['Recogida_Cliente'], errors='coerce')
            completed = completed.dropna(subset=['Recogida_Cliente'])
            completed['date_str'] = completed['Recogida_Cliente'].dt.strftime('%Y-%m-%d')

            for date_str, group in completed.groupby('date_str'):
                importe = pd.to_numeric(group['Importe'], errors='coerce').sum()
                coste_material = pd.to_numeric(group['Coste_Material'], errors='coerce').sum()
                coste_proveedor = pd.to_numeric(group['Coste_Proveedor'], errors='coerce').sum()
                daily_data[date_str] = {
                    'importe': round(float(importe), 2),
                    'coste_material': round(float(coste_material), 2),
                    'coste_proveedor': round(float(coste_proveedor), 2),
                    'beneficio': round(float(importe - coste_material - coste_proveedor), 2)
                }

        # Monthly provider payments grouped by Pago_Proveedor
        paid_orders = orders_df[orders_df['Pago_Proveedor'].notna()].copy()
        monthly_provider = {}
        if not paid_orders.empty:
            paid_orders['Pago_Proveedor'] = pd.to_datetime(paid_orders['Pago_Proveedor'], errors='coerce')
            paid_orders = paid_orders.dropna(subset=['Pago_Proveedor'])
            paid_orders['month_str'] = paid_orders['Pago_Proveedor'].dt.strftime('%Y-%m')
            for month_str, group in paid_orders.groupby('month_str'):
                coste = pd.to_numeric(group['Coste_Proveedor'], errors='coerce').sum()
                monthly_provider[month_str] = round(float(coste), 2)

        sorted_dates = sorted(daily_data.keys())
        sorted_months = sorted(monthly_provider.keys())

        chart_daily = {
            'labels': sorted_dates,
            'importe': [daily_data[d]['importe'] for d in sorted_dates],
            'coste_material': [daily_data[d]['coste_material'] for d in sorted_dates],
            'coste_proveedor': [daily_data[d]['coste_proveedor'] for d in sorted_dates],
            'beneficio': [daily_data[d]['beneficio'] for d in sorted_dates],
        }

        chart_monthly = {
            'labels': sorted_months,
            'coste_proveedor': [monthly_provider[m] for m in sorted_months],
        }

        return render_template(
            'contabilidad.html',
            chart_daily=json.dumps(chart_daily),
            chart_monthly=json.dumps(chart_monthly)
        )
    except Exception as e:
        flash(f'Error al cargar contabilidad: {str(e)}', 'danger')
        return render_template('contabilidad.html', chart_daily='{}', chart_monthly='{}')


@app.route('/limite')
@login_required
def limite():
    try:
        orders_df = f.get_orders_data()
        clients_df = f.get_clients_data()
        articles_df = f.get_articles_data()
        joined = f.ordersJoin(orders_df.copy(), clients_df.copy(), articles_df.copy())

        # Pending orders with Limite date
        pending = joined[joined['Recogida_Cliente'].isna() & joined['Limite'].notna()].copy()

        events = []
        for _, row in pending.iterrows():
            title = f"{row.get('Cliente', 'N/A')} - {row.get('Articulo', 'N/A')}"
            events.append({
                'title': title,
                'start': str(row['Limite']),
                'id': str(row.get('ID', '')),
                'extendedProps': {
                    'descripcion': str(row.get('Descripcion', '')),
                    'proveedor': str(row.get('Proveedor', ''))
                }
            })

        return render_template('limite.html', events=json.dumps(events))
    except Exception as e:
        flash(f'Error al cargar límites: {str(e)}', 'danger')
        return render_template('limite.html', events='[]')


@app.route('/morosos')
@login_required
def morosos():
    try:
        orders_df = f.get_orders_data()
        clients_df = f.get_clients_data()
        articles_df = f.get_articles_data()
        joined = f.ordersJoin(orders_df.copy(), clients_df.copy(), articles_df.copy())

        # Orders delivered but not paid
        morosos_df = joined[joined['Recogida_Cliente'].notna() & (joined['Pagado'] == 'No Pagado')].copy()
        records = df_to_records(morosos_df)

        return render_template('morosos.html', morosos=records)
    except Exception as e:
        flash(f'Error al cargar morosos: {str(e)}', 'danger')
        return render_template('morosos.html', morosos=[])


@app.route('/proveedores')
@login_required
def proveedores():
    try:
        orders_df = f.get_orders_data()
        clients_df = f.get_clients_data()
        articles_df = f.get_articles_data()
        joined = f.ordersJoin(orders_df.copy(), clients_df.copy(), articles_df.copy())

        # Orders picked up from provider but not yet paid to provider
        unpaid_df = joined[joined['Recogida_Proveedor'].notna() & joined['Pago_Proveedor'].isna()].copy()
        records = df_to_records(unpaid_df)

        return render_template('proveedores.html', pedidos=records)
    except Exception as e:
        flash(f'Error al cargar proveedores: {str(e)}', 'danger')
        return render_template('proveedores.html', pedidos=[])


# ─── API Routes ───────────────────────────────────────────────────────────────

@app.route('/api/orders/<int:order_id>/move_forward', methods=['POST'])
@login_required
def api_move_forward(order_id):
    try:
        data = request.get_json()
        stage = int(data.get('stage', 1))
        costurera = data.get('costurera')
        pago = data.get('pago')
        result = f.move_order_forward(order_id, stage, costurera=costurera, pago=pago)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/orders/<int:order_id>/move_backward', methods=['POST'])
@login_required
def api_move_backward(order_id):
    try:
        data = request.get_json()
        stage = int(data.get('stage', 2))
        result = f.move_order_backward(order_id, stage)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/clientes/insert', methods=['POST'])
@login_required
def api_clientes_insert():
    try:
        data = request.get_json()
        nombre = data.get('nombre', '').strip()
        descripcion = data.get('descripcion', '').strip()
        telefono = data.get('telefono', '').strip()

        if not nombre:
            return jsonify({'success': False, 'error': 'El nombre es obligatorio'}), 400

        record = {
            'Nombre': nombre,
            'Descripcion': descripcion if descripcion else None,
            'Telefono': telefono if telefono else None
        }
        result = f.insert_record('Clientes', record)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/clientes/<int:cliente_id>/update', methods=['POST'])
@login_required
def api_clientes_update(cliente_id):
    try:
        data = request.get_json()
        result = f.update_record('Clientes', cliente_id, data)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/clientes/<int:cliente_id>/delete', methods=['POST'])
@login_required
def api_clientes_delete(cliente_id):
    try:
        f.delete_record('Clientes', cliente_id)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/articulos/<int:articulo_id>/delete', methods=['POST'])
@login_required
def api_articulos_delete(articulo_id):
    try:
        f.delete_record('Articulos', articulo_id)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/pedidos/insert', methods=['POST'])
@login_required
def api_pedidos_insert():
    try:
        data = request.get_json()

        record = {}
        fields = [
            'Cliente_id', 'Articulo_id', 'Descripcion', 'Cantidad',
            'Proveedor', 'Pagado', 'Entrega_Cliente', 'Limite',
            'Coste_Material', 'Coste_Proveedor', 'Importe',
            'Entrega_Proveedor', 'Recogida_Proveedor', 'Recogida_Cliente', 'Pago_Proveedor'
        ]
        for field in fields:
            val = data.get(field)
            if val == '' or val is None:
                record[field] = None
            else:
                record[field] = val

        # Ensure numeric fields are properly typed
        for num_field in ['Cantidad', 'Coste_Material', 'Coste_Proveedor', 'Importe']:
            if record.get(num_field) is not None:
                try:
                    record[num_field] = float(record[num_field])
                except (ValueError, TypeError):
                    record[num_field] = None

        # Default Pagado
        if not record.get('Pagado'):
            record['Pagado'] = 'No Pagado'

        result = f.insert_record('Pedidos', record)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/pedidos/<int:pedido_id>/update', methods=['POST'])
@login_required
def api_pedidos_update(pedido_id):
    try:
        data = request.get_json()
        # Convert empty strings to None
        clean_data = {}
        for k, v in data.items():
            clean_data[k] = None if v == '' else v
        result = f.update_record('Pedidos', pedido_id, clean_data)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/pedidos/<int:pedido_id>/delete', methods=['POST'])
@login_required
def api_pedidos_delete(pedido_id):
    try:
        f.delete_record('Pedidos', pedido_id)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/articulos/get/<int:articulo_id>', methods=['GET'])
@login_required
def api_articulos_get(articulo_id):
    try:
        articles_df = f.get_articles_data()
        article = articles_df[articles_df['ID'] == articulo_id]
        if article.empty:
            return jsonify({'success': False, 'error': 'Artículo no encontrado'}), 404
        record = df_to_records(article)[0]
        return jsonify({'success': True, 'data': record})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/proveedores/pagar', methods=['POST'])
@login_required
def api_proveedores_pagar():
    try:
        data = request.get_json()
        ids = data.get('ids', [])
        if not ids:
            return jsonify({'success': False, 'error': 'No se han seleccionado pedidos'}), 400

        today = datetime.date.today().isoformat()
        updated = []
        for order_id in ids:
            result = f.update_record('Pedidos', int(order_id), {'Pago_Proveedor': today})
            updated.append(result)

        return jsonify({'success': True, 'updated': len(updated)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


# ─── Backup Routes ────────────────────────────────────────────────────────────

@app.route('/backup')
@login_required
def backup():
    import backup as bk
    authorized = bk.is_authorized()
    history = bk.get_backup_history() if authorized else []
    return render_template('backup.html', authorized=authorized, history=history)


@app.route('/backup/authorize')
@login_required
def backup_authorize():
    import backup as bk
    flow = bk.get_flow()
    auth_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true',
        prompt='consent'
    )
    session['oauth_state'] = state
    return redirect(auth_url)


@app.route('/backup/oauth2callback')
@login_required
def backup_oauth2callback():
    import backup as bk
    try:
        flow = bk.get_flow()
        flow.fetch_token(authorization_response=request.url)
        bk.save_credentials(flow.credentials)
        flash('Google Drive conectado correctamente.', 'success')
    except Exception as e:
        flash(f'Error al conectar con Google Drive: {str(e)}', 'danger')
    return redirect(url_for('backup'))


@app.route('/backup/disconnect', methods=['POST'])
@login_required
def backup_disconnect():
    import backup as bk
    bk.disconnect()
    flash('Google Drive desconectado.', 'info')
    return redirect(url_for('backup'))


@app.route('/api/backup/run', methods=['POST'])
@login_required
def api_backup_run():
    import backup as bk
    try:
        db_path = os.path.join(os.path.dirname(__file__), 'data', 'marta.db')
        result = bk.run_backup(db_path)
        return jsonify({'success': True, **result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ──────────────────────────────────────────────────────────────────────────────

def open_browser():
    webbrowser.open('http://localhost:5000')


if __name__ == '__main__':
    init_db()
    # Open browser after 1 second to let Flask start
    threading.Timer(1.0, open_browser).start()
    app.run(debug=False, host='127.0.0.1', port=5000)

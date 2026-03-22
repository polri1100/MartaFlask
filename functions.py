import pandas as pd
import datetime
import os
import unidecode
from database import get_db


def normalize_string(s):
    if s is None:
        return ""
    return unidecode.unidecode(str(s)).lower().strip()


def obtainTable(tableName):
    try:
        with get_db() as conn:
            df = pd.read_sql_query(f'SELECT * FROM "{tableName}" ORDER BY ID DESC', conn)
            date_columns = {'Entrega_Cliente', 'Limite', 'Entrega_Proveedor',
                            'Recogida_Proveedor', 'Recogida_Cliente', 'Pago_Proveedor'}
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            return df
    except Exception as e:
        print(f"Error obtaining table {tableName}: {e}")
        return pd.DataFrame()


def get_orders_data():
    return obtainTable('Pedidos')


def get_clients_data():
    return obtainTable('Clientes')


def get_articles_data():
    return obtainTable('Articulos')


def obtainTableWithNormalized(tableName):
    df = obtainTable(tableName)
    if df.empty:
        return df
    if tableName == 'Articulos' and 'Articulo' in df.columns:
        df['Articulo_Normalized'] = df['Articulo'].apply(normalize_string)
    if tableName == 'Clientes' and 'Nombre' in df.columns:
        df['Nombre_Normalized'] = df['Nombre'].apply(normalize_string)
    return df


def returnMaxMinID(df):
    if df.empty or 'ID' not in df.columns:
        return None, None
    ids = df['ID'].dropna().tolist()
    if ids:
        return max(ids), min(ids)
    return None, None


def insert_record(tableName, data):
    try:
        with get_db() as conn:
            cols = ', '.join(f'"{k}"' for k in data.keys())
            placeholders = ', '.join('?' for _ in data)
            values = list(data.values())
            conn.execute(f'INSERT INTO "{tableName}" ({cols}) VALUES ({placeholders})', values)
            conn.commit()
            return True
    except Exception as e:
        print(f"Error inserting into {tableName}: {e}")
        raise


def update_record(tableName, record_id, data):
    try:
        with get_db() as conn:
            set_clause = ', '.join(f'"{k}" = ?' for k in data.keys())
            values = list(data.values()) + [record_id]
            conn.execute(f'UPDATE "{tableName}" SET {set_clause} WHERE ID = ?', values)
            conn.commit()
            return True
    except Exception as e:
        print(f"Error updating {tableName}: {e}")
        raise


def delete_record(tableName, record_id):
    try:
        with get_db() as conn:
            conn.execute(f'DELETE FROM "{tableName}" WHERE ID = ?', [record_id])
            conn.commit()
            return True
    except Exception as e:
        print(f"Error deleting from {tableName}: {e}")
        raise


def move_order_forward(order_id, stage, costurera=None, pago=None):
    today = datetime.date.today().isoformat()

    if stage == 1:
        # Local para costurera -> En la costurera
        data = {'Entrega_Proveedor': today}
        if costurera:
            data['Proveedor'] = costurera
        update_record('Pedidos', order_id, data)
    elif stage == 2:
        # En la costurera -> Local para entregar
        data = {'Recogida_Proveedor': today}
        update_record('Pedidos', order_id, data)
    elif stage == 3:
        # Local para entregar -> Entregado
        data = {'Recogida_Cliente': today}
        if pago:
            data['Pagado'] = pago
        update_record('Pedidos', order_id, data)
    else:
        raise ValueError(f"Invalid stage: {stage}")

    return True


def move_order_backward(order_id, stage):
    if stage == 2:
        # En la costurera -> Local para costurera
        data = {'Entrega_Proveedor': None}
        update_record('Pedidos', order_id, data)
    elif stage == 3:
        # Local para entregar -> En la costurera
        data = {'Recogida_Proveedor': None}
        update_record('Pedidos', order_id, data)
    else:
        raise ValueError(f"Invalid stage for backward move: {stage}")

    return True


def ordersJoin(orders_df=None, clients_df=None, articles_df=None):
    if orders_df is None:
        orders_df = get_orders_data()
    if clients_df is None:
        clients_df = get_clients_data()
    if articles_df is None:
        articles_df = get_articles_data()

    if orders_df.empty:
        return orders_df

    if not clients_df.empty and 'ID' in clients_df.columns and 'Nombre' in clients_df.columns:
        clients_map = clients_df.set_index('ID')['Nombre'].to_dict()
        orders_df['Cliente'] = orders_df['Cliente_id'].map(clients_map).fillna('')
    else:
        orders_df['Cliente'] = ''

    if not articles_df.empty and 'ID' in articles_df.columns and 'Articulo' in articles_df.columns:
        articles_map = articles_df.set_index('ID')['Articulo'].to_dict()
        orders_df['Articulo'] = orders_df['Articulo_id'].map(articles_map).fillna('')
    else:
        orders_df['Articulo'] = ''

    return orders_df


def searchFunction(df, search_term, columns):
    if not search_term or df.empty:
        return df
    norm_term = normalize_string(search_term)
    mask = pd.Series([False] * len(df), index=df.index)
    for col in columns:
        if col in df.columns:
            mask = mask | df[col].apply(
                lambda x: norm_term in normalize_string(str(x)) if x is not None else False
            )
    return df[mask]

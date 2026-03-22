import pandas as pd
import datetime
import os
import unidecode
from supabase import create_client, Client

_supabase_client = None


def get_supabase():
    global _supabase_client
    if _supabase_client is None:
        url = os.environ.get('SUPABASE_URL')
        key = os.environ.get('SUPABASE_KEY')
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables must be set")
        _supabase_client = create_client(url, key)
    return _supabase_client


def normalize_string(s):
    if not isinstance(s, str):
        return s
    return unidecode.unidecode(s).lower().strip()


def obtainTable(table_name):
    supabase = get_supabase()
    response = supabase.table(table_name).select("*").execute()
    if response.data:
        return pd.DataFrame(response.data)
    return pd.DataFrame()


def get_orders_data():
    return obtainTable('Pedidos')


def get_clients_data():
    return obtainTable('Clientes')


def get_articles_data():
    return obtainTable('Articulos')


def obtainTableWithNormalized(table_name, column):
    df = obtainTable(table_name)
    if df.empty:
        return df
    df[column + '_normalized'] = df[column].apply(normalize_string)
    return df


def returnMaxMinID(table_name):
    supabase = get_supabase()
    response = supabase.table(table_name).select("ID").execute()
    if response.data:
        ids = [row['ID'] for row in response.data if row.get('ID') is not None]
        if ids:
            return max(ids), min(ids)
    return None, None


def delete_record(table_name, record_id):
    supabase = get_supabase()
    response = supabase.table(table_name).delete().eq('ID', record_id).execute()
    return True


def insert_record(table_name, data):
    supabase = get_supabase()
    response = supabase.table(table_name).insert(data).execute()
    if response.data:
        return response.data[0]
    raise Exception(f"Failed to insert record into {table_name}")


def update_record(table_name, record_id, data):
    supabase = get_supabase()
    response = supabase.table(table_name).update(data).eq('ID', record_id).execute()
    if response.data:
        return response.data[0]
    raise Exception(f"Failed to update record {record_id} in {table_name}")


def move_order_forward(order_id, stage, costurera=None, pago=None):
    today = datetime.date.today().isoformat()
    supabase = get_supabase()

    if stage == 1:
        # Local para costurera -> En la costurera
        data = {'Entrega_Proveedor': today}
        if costurera:
            data['Proveedor'] = costurera
        response = supabase.table('Pedidos').update(data).eq('ID', order_id).execute()
    elif stage == 2:
        # En la costurera -> Local para entregar
        data = {'Recogida_Proveedor': today}
        response = supabase.table('Pedidos').update(data).eq('ID', order_id).execute()
    elif stage == 3:
        # Local para entregar -> Entregado
        data = {'Recogida_Cliente': today}
        if pago:
            data['Pagado'] = pago
        response = supabase.table('Pedidos').update(data).eq('ID', order_id).execute()
    else:
        raise ValueError(f"Invalid stage: {stage}")

    if response.data:
        return response.data[0]
    raise Exception(f"Failed to move order {order_id} forward from stage {stage}")


def move_order_backward(order_id, stage):
    supabase = get_supabase()

    if stage == 2:
        # En la costurera -> Local para costurera
        data = {'Entrega_Proveedor': None}
        response = supabase.table('Pedidos').update(data).eq('ID', order_id).execute()
    elif stage == 3:
        # Local para entregar -> En la costurera
        data = {'Recogida_Proveedor': None}
        response = supabase.table('Pedidos').update(data).eq('ID', order_id).execute()
    else:
        raise ValueError(f"Invalid stage for backward move: {stage}")

    if response.data:
        return response.data[0]
    raise Exception(f"Failed to move order {order_id} backward from stage {stage}")


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
            mask = mask | df[col].apply(lambda x: norm_term in normalize_string(str(x)) if x is not None else False)
    return df[mask]

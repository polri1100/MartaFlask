"""
Migration script: Supabase -> SQLite
Run once: python migrate_from_supabase.py
Requires: pip install supabase python-dotenv
Set SUPABASE_URL and SUPABASE_KEY in .env before running.
"""
import os
import sys
from dotenv import load_dotenv
from database import init_db, get_db

load_dotenv()

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: Set SUPABASE_URL and SUPABASE_KEY in your .env file before migrating.")
    sys.exit(1)

try:
    from supabase import create_client
except ImportError:
    print("ERROR: Run 'pip install supabase' before running migration.")
    sys.exit(1)


def migrate():
    print("Connecting to Supabase...")
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("Initializing SQLite database...")
    init_db()

    # Migrate Clientes
    print("Migrating Clientes...")
    result = sb.table('Clientes').select('*').order('ID').execute()
    clientes = result.data
    with get_db() as conn:
        conn.execute("DELETE FROM Clientes")
        for row in clientes:
            conn.execute(
                "INSERT OR REPLACE INTO Clientes (ID, Nombre, Descripcion, Telefono) VALUES (?, ?, ?, ?)",
                (row.get('ID'), row.get('Nombre'), row.get('Descripcion'), row.get('Telefono'))
            )
        conn.commit()
    print(f"  -> {len(clientes)} clientes migrados")

    # Migrate Articulos
    print("Migrating Articulos...")
    result = sb.table('Articulos').select('*').order('ID').execute()
    articulos = result.data
    with get_db() as conn:
        conn.execute("DELETE FROM Articulos")
        for row in articulos:
            conn.execute(
                """INSERT OR REPLACE INTO Articulos
                   (ID, Articulo, Descripcion, Coste_Material_Sugerido, Coste_Proveedor_Sugerido, Importe_Sugerido)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (row.get('ID'), row.get('Articulo'), row.get('Descripcion'),
                 row.get('Coste_Material_Sugerido', 0), row.get('Coste_Proveedor_Sugerido', 0),
                 row.get('Importe_Sugerido', 0))
            )
        conn.commit()
    print(f"  -> {len(articulos)} articulos migrados")

    # Migrate Pedidos
    print("Migrating Pedidos...")
    result = sb.table('Pedidos').select('*').order('ID').execute()
    pedidos = result.data
    with get_db() as conn:
        conn.execute("DELETE FROM Pedidos")
        for row in pedidos:
            conn.execute(
                """INSERT OR REPLACE INTO Pedidos
                   (ID, Cliente_id, Articulo_id, Descripcion, Cantidad, Proveedor, Pagado,
                    Entrega_Cliente, Limite, Coste_Material, Coste_Proveedor, Importe,
                    Entrega_Proveedor, Recogida_Proveedor, Recogida_Cliente, Pago_Proveedor)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (row.get('ID'), row.get('Cliente_id'), row.get('Articulo_id'),
                 row.get('Descripcion'), row.get('Cantidad'), row.get('Proveedor'),
                 row.get('Pagado', 'No Pagado'), row.get('Entrega_Cliente'), row.get('Limite'),
                 row.get('Coste_Material', 0), row.get('Coste_Proveedor', 0),
                 row.get('Importe', 0), row.get('Entrega_Proveedor'),
                 row.get('Recogida_Proveedor'), row.get('Recogida_Cliente'),
                 row.get('Pago_Proveedor'))
            )
        conn.commit()
    print(f"  -> {len(pedidos)} pedidos migrados")

    # Reset SQLite sequences to be higher than max imported IDs
    with get_db() as conn:
        for table in ['Clientes', 'Articulos', 'Pedidos']:
            row = conn.execute(f'SELECT MAX(ID) as max_id FROM "{table}"').fetchone()
            if row['max_id']:
                conn.execute(
                    f"INSERT OR REPLACE INTO sqlite_sequence (name, seq) VALUES ('{table}', {row['max_id']})"
                )
        conn.commit()

    print("\nMigration complete! Your data is now in data/marta.db")
    print("You can now run the app with: python app.py")


if __name__ == '__main__':
    migrate()

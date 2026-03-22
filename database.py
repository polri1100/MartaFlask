import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'data', 'marta.db')


def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    """Create tables if they don't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS Clientes (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                Nombre TEXT NOT NULL UNIQUE,
                Descripcion TEXT,
                Telefono TEXT UNIQUE
            );
            CREATE TABLE IF NOT EXISTS Articulos (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                Articulo TEXT NOT NULL,
                Descripcion TEXT,
                Coste_Material_Sugerido REAL DEFAULT 0,
                Coste_Proveedor_Sugerido REAL DEFAULT 0,
                Importe_Sugerido REAL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS Pedidos (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                Cliente_id INTEGER REFERENCES Clientes(ID),
                Articulo_id INTEGER REFERENCES Articulos(ID),
                Descripcion TEXT,
                Cantidad REAL DEFAULT 1,
                Proveedor TEXT,
                Pagado TEXT DEFAULT 'No Pagado',
                Entrega_Cliente DATE,
                Limite DATE,
                Coste_Material REAL DEFAULT 0,
                Coste_Proveedor REAL DEFAULT 0,
                Importe REAL DEFAULT 0,
                Entrega_Proveedor DATE,
                Recogida_Proveedor DATE,
                Recogida_Cliente DATE,
                Pago_Proveedor DATE
            );
        """)
        conn.commit()

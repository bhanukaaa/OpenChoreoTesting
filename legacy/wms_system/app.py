import socket
import threading
import os
import time
from utility.config import configs
from utility.db_helper import get_db_connection

HOST = os.getenv('HOST', '0.0.0.0')
PORT = int(os.getenv('PORT', 8004))


def get_connection():
    return get_db_connection(schema='legacy_wms')


def init_db():
    try:
        with get_connection() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS packages (
                    id           SERIAL PRIMARY KEY,
                    "orderID"    TEXT NOT NULL,
                    package_type TEXT,
                    package_name TEXT,
                    weight       REAL,
                    status       TEXT DEFAULT 'Pending'
                )
            ''')
            conn.commit()
        print(f"[*] Legacy WMS database initialized with schema legacy_wms", flush=True)
    except Exception as e:
        print(f"[!] DB Init Error: {e}", flush=True)


def register_package(order_id, pkg_type, weight_str, pkg_name):
    try:
        weight = float(weight_str)
    except (ValueError, TypeError):
        weight = 0.0

    with get_connection() as conn:
        cursor = conn.execute(
            'INSERT INTO packages ("orderID", package_type, weight, package_name, status) VALUES (%s, %s, %s, %s, \'Ready\') RETURNING id',
            (str(order_id), pkg_type, weight, pkg_name)
        )
        pkg_id = cursor.fetchone()['id']
        conn.commit()

    if configs.get("enableLegacyDelay"):
        time.sleep(configs["legacyDelay"].get("WMS", 1))

    return f"SUCCESS|{pkg_id}"


def void_package(package_id_str):
    package_id = int(str(package_id_str).replace('WMS-', ''))
    with get_connection() as conn:
        conn.execute('UPDATE packages SET status = %s WHERE id = %s',
                     ('CANCELLED', package_id))
        conn.commit()
    return f"SUCCESS|{package_id}"


def update_status_by_order(order_id, new_status):
    """Updates the status of all packages linked to a specific orderID."""
    try:
        with get_connection() as conn:
            conn.execute(
                'UPDATE packages SET status = %s WHERE "orderID" = %s', (new_status, str(order_id)))
            conn.commit()
        return f"SUCCESS|{order_id}"
    except Exception as e:
        return f"ERROR|{str(e)}"


def get_ready_orders():
    try:
        with get_connection() as conn:
            cursor = conn.execute(
                'SELECT "orderID" FROM packages WHERE status = \'Ready\'')
            rows = cursor.fetchall()
        order_ids = [str(row['orderID']) for row in rows]
        if order_ids:
            return f"SUCCESS|{','.join(order_ids)}"
        return "SUCCESS|NONE"
    except Exception as e:
        return f"ERROR|DB_QUERY_FAILED|{str(e)}"


def handle_client(client_socket):
    try:
        raw_request = client_socket.recv(1024).decode('utf-8').strip()
        if not raw_request:
            return
        print(f"[TCP] Received: {raw_request}", flush=True)
        parts = raw_request.split('|')
        command = parts[0]
        response = "ERROR|UNKNOWN_COMMAND"

        if command == 'GET_READY':
            response = get_ready_orders()
        elif command == 'REGISTER' and len(parts) >= 5:
            response = register_package(parts[1], parts[2], parts[3], parts[4])
        elif command == 'VOID' and len(parts) >= 2:
            response = void_package(parts[1])
        elif command == 'UPDATE_STATUS' and len(parts) >= 3:
            response = update_status_by_order(parts[1], parts[2])
        elif command == 'PING':
            response = "PONG"

        client_socket.send(response.encode('utf-8'))
    except Exception as e:
        print(f"[TCP] Error processing client: {e}", flush=True)
    finally:
        client_socket.close()


def start_server():
    init_db()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"[*] Legacy WMS TCP Server listening on {HOST}:{PORT}", flush=True)
    while True:
        client, addr = server.accept()
        threading.Thread(target=handle_client, args=(
            client,), daemon=True).start()


if __name__ == '__main__':
    start_server()

import os
import random
import time
from flask import Flask, request, jsonify
from utility.config import configs
from utility.db_helper import get_db_connection

app = Flask(__name__)


def get_connection():
    return get_db_connection(schema='legacy_ros')


def init_db():
    try:
        with get_connection() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS order_address (
                    order_id TEXT PRIMARY KEY,
                    address  TEXT
                )
            ''')
            conn.commit()
        print(f"[*] Legacy ROS Database initialized with schema legacy_ros", flush=True)
    except Exception as e:
        print(f"[!] ROS Init Error: {e}", flush=True)


# --- Internal Logic Functions ---

def get_address_pairs(order_ids):
    """
    Fetches list of dictionaries containing both ID and Address.
    Returns: [{'orderID': 'ORD1', 'address': '123 St'}, ...]
    """
    if not order_ids:
        return []

    placeholders = ', '.join(['%s'] * len(order_ids))
    query = f"SELECT order_id, address FROM order_address WHERE order_id IN ({placeholders})"

    try:
        with get_connection() as conn:
            cursor = conn.execute(query, [str(oid) for oid in order_ids])
            rows = cursor.fetchall()
            return [{"orderID": row['order_id'], "deliveryAddress": row['address']} for row in rows]
    except Exception as e:
        print(f"[ROS System] Database fetch error: {e}", flush=True)
        return []


def distribute_routes(pairs, num_trucks):
    """
    Randomly shuffles order pairs and distributes them across a set number of trucks.
    """
    if not pairs:
        return [[] for _ in range(num_trucks)]

    random.shuffle(pairs)
    routes = [[] for _ in range(num_trucks)]

    for i, pair in enumerate(pairs):
        routes[i % num_trucks].append(pair)

    return routes


# --- API Endpoints ---

@app.route('/health', methods=['GET'])
@app.route('/healthz', methods=['GET'])
@app.route('/live', methods=['GET'])
def liveness():
    return jsonify({"status": "healthy", "service": "legacy_ros"}), 200


@app.route('/ready', methods=['GET'])
@app.route('/readyz', methods=['GET'])
def readiness():
    try:
        with get_connection() as conn:
            conn.execute("SELECT 1")
        return jsonify({"status": "ready", "service": "legacy_ros"}), 200
    except Exception as e:
        return jsonify({"status": "not ready", "error": str(e)}), 503


@app.route('/legacy/address', methods=['POST'])
def upload_address():
    data = request.json or {}
    order_id = str(data.get("orderID", ""))
    address = data.get("deliveryAddress", "")

    if not order_id or not address:
        return jsonify({"error": "Missing orderID or address"}), 400

    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO order_address (order_id, address) 
                VALUES (%s, %s)
                ON CONFLICT (order_id) DO UPDATE SET address = EXCLUDED.address
                """,
                (order_id, address)
            )
            conn.commit()
        print(
            f"[ROS System] Address recorded: {order_id} -> {address}", flush=True)

        if configs.get("enableLegacyDelay"):
            time.sleep(configs["legacyDelay"].get("ROS", 1))

        return jsonify({"success": True}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/legacy/optimize', methods=['POST'])
def optimize():
    data = request.json or {}
    order_ids = data.get("orderIDs", [])
    num_trucks = int(data.get("numTrucks", 1))

    if not order_ids:
        return jsonify({"error": "No orderIDs provided"}), 400
    if num_trucks < 1:
        return jsonify({"error": "numTrucks must be at least 1"}), 400

    pairs = get_address_pairs(order_ids)

    if not pairs:
        return jsonify({"error": "No matching addresses found in the system"}), 404

    truck_routes = distribute_routes(pairs, num_trucks)

    print(
        f"[ROS System] Optimized {len(pairs)} orders into {num_trucks} routes.", flush=True)
    return jsonify({
        "success": True,
        "total_items": len(pairs),
        "routes": truck_routes
    })


@app.route('/legacy/address/<order_id>', methods=['DELETE'])
def remove_address(order_id):
    try:
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT 1 FROM order_address WHERE order_id = %s", (str(order_id),))
            if cursor.fetchone() is None:
                return jsonify({"error": "Order not found"}), 404

            conn.execute(
                "DELETE FROM order_address WHERE order_id = %s", (str(order_id),))
            conn.commit()

        print(
            f"[ROS System] ROLLBACK: Order {order_id} removed from pool.", flush=True)
        return jsonify({"success": True, "message": "Address removed"}), 200
    except Exception as e:
        print(f"[ROS System] Deletion Error: {e}", flush=True)
        return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":
    init_db()
    port = int(os.getenv('PORT', 8005))
    app.run(host='0.0.0.0', port=port)

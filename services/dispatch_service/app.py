import time
import json
import threading
import math
import os
from flask import Flask, jsonify
from utility.rabbit_helper import get_connection, publish_event
from utility.config import configs
from utility.db_helper import get_db_connection

app = Flask(__name__)

EXCHANGE_NAME = os.getenv('RABBITMQ_EXCHANGE', 'swifttrack_events')
PACKAGES_PER_TRUCK = 10


# --- Database Core ---

def get_connection_db():
    return get_db_connection(schema='dispatch')


def init_db():
    """Initializes the normalized Dispatch database and seeds dummy drivers."""
    with get_connection_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS drivers (
                "driverID" SERIAL PRIMARY KEY,
                name       TEXT,
                status     TEXT DEFAULT 'AVAILABLE'
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS manifests (
                "manifestID"   SERIAL PRIMARY KEY,
                "driverID"     INTEGER REFERENCES drivers("driverID"),
                "dispatchedAt" TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS manifest_entries (
                "entryID"         SERIAL PRIMARY KEY,
                "manifestID"      INTEGER REFERENCES manifests("manifestID"),
                "orderID"         TEXT,
                "deliveryAddress" TEXT
            )
        ''')

        cursor = conn.execute("SELECT COUNT(*) FROM drivers")
        if cursor.fetchone()[0] == 0:
            print("[*] Seeding drivers for Users 1-3...", flush=True)
            drivers = [('driver1',), ('driver2',), ('driver3',)]
            conn.executemany("INSERT INTO drivers (name) VALUES (%s)", drivers)
            print("[*] Dispatch DB: Seeded 3 drivers (IDs 1-3).", flush=True)

        conn.commit()


# --- Business Logic ---

def assign_drivers_and_save(routes):
    """
    Takes paired routes from ROS, finds available drivers, and 
    saves data across normalized tables.
    """
    with get_connection_db() as conn:
        cursor = conn.execute(
            'SELECT "driverID", name FROM drivers WHERE status = \'AVAILABLE\' LIMIT %s', (len(routes),))
        available_drivers = cursor.fetchall()

        for i, route_pairs in enumerate(routes):
            driver = available_drivers[i] if i < len(available_drivers) else None
            driver_id = driver['driverID'] if driver else None
            driver_name = driver['name'] if driver else "Unassigned"

            cur = conn.execute(
                'INSERT INTO manifests ("driverID", "dispatchedAt") VALUES (%s, %s) RETURNING "manifestID"',
                (driver_id, time.strftime('%Y-%m-%d %H:%M:%S'))
            )
            manifest_id = cur.fetchone()['manifestID']

            entry_data = [(manifest_id, str(item['orderID']), item['deliveryAddress'])
                          for item in route_pairs]

            conn.executemany(
                'INSERT INTO manifest_entries ("manifestID", "orderID", "deliveryAddress") VALUES (%s, %s, %s)',
                entry_data
            )

            if driver_id:
                conn.execute(
                    'UPDATE drivers SET status = \'BUSY\' WHERE "driverID" = %s', (driver_id,))

            print(
                f"[SUCCESS] Manifest #{manifest_id} created for {driver_name} ({len(entry_data)} stops).", flush=True)

        conn.commit()


def scheduleDeliveries(orderIDs, count):
    """Calculates truck requirements and requests optimized routes from ROS."""
    if count < 10:
        print(
            f"[!] Dispatch: {count} packages is below threshold (10). Holding...", flush=True)
        return

    num_trucks = math.ceil(count / PACKAGES_PER_TRUCK)
    print(
        f"[*] Dispatch: Requesting {num_trucks} routes for {count} orders...", flush=True)

    try:
        connection = get_connection()
        channel = connection.channel()
        payload = {"orderIDs": orderIDs, "numTrucks": num_trucks}

        publish_event(channel, EXCHANGE_NAME, 'dispatch.getRoutes', payload)

        connection.close()
    except Exception as e:
        print(f"[!] Dispatch Messaging Error: {e}", flush=True)


# --- RabbitMQ Event Listener ---

def start_event_listener():
    try:
        def callback(ch, method, properties, body):
            data = json.loads(body)
            if method.routing_key == 'wms.returnPackages':
                scheduleDeliveries(data.get('orderIDs', []), data.get('count', 0))
            elif method.routing_key == 'ros.returnRoutes':
                assign_drivers_and_save(data.get('routes', []))

        connection = get_connection()
        channel = connection.channel()

        queue_config = {
            'queue_name': 'dispatch_service_queue',
            'exclusive': False,
            'durable': True,
            'routing_keys': ['wms.returnPackages', 'ros.returnRoutes'],
            'auto_ack': True
        }

        from utility import rabbit_helper as rh
        rh.listen_to_events(channel, EXCHANGE_NAME, queue_config, callback)
        print("[*] Dispatch Listener active. Monitoring WMS and ROS...", flush=True)
    except Exception as e:
        print(f"[!] Listener Fatal Error: {e}", flush=True)


# --- Scheduler Thread ---

def start_periodic_publisher():
    """Asks WMS for ready packages periodically."""
    time.sleep(configs.get("dispatchStartDelay", 30))
    while True:
        try:
            connection = get_connection()
            channel = connection.channel()

            publish_event(channel, EXCHANGE_NAME, 'dispatch.getAvailablePackages', {"status": "polling"})

            connection.close()
            print("[POLL] Requested available packages from WMS.", flush=True)
        except Exception as e:
            print(f"[!] Poller Error: {e}", flush=True)
        time.sleep(configs.get("dispatchPeriod", 18000))


# --- API Endpoints ---

@app.route('/health', methods=['GET'])
@app.route('/healthz', methods=['GET'])
@app.route('/live', methods=['GET'])
def liveness():
    return jsonify({"status": "healthy", "service": "dispatch"}), 200


@app.route('/ready', methods=['GET'])
@app.route('/readyz', methods=['GET'])
def readiness():
    try:
        with get_connection_db() as conn:
            conn.execute("SELECT 1")
        return jsonify({"status": "ready", "service": "dispatch"}), 200
    except Exception as e:
        return jsonify({"status": "not ready", "error": str(e)}), 503


@app.route('/manifests', methods=['GET'])
def list_manifests():
    """Lists all generated manifests and their assigned drivers."""
    with get_connection_db() as conn:
        cursor = conn.execute('''
            SELECT m."manifestID", d.name as "driverName", m."dispatchedAt", COUNT(e."entryID") as "totalStops"
            FROM manifests m
            LEFT JOIN drivers d ON m."driverID" = d."driverID"
            LEFT JOIN manifest_entries e ON m."manifestID" = e."manifestID"
            GROUP BY m."manifestID", d.name, m."dispatchedAt"
            ORDER BY m."manifestID" DESC
        ''')
        return jsonify([dict(row) for row in cursor.fetchall()]), 200


@app.route('/manifests/<int:m_id>', methods=['GET'])
def get_manifest_detail(m_id):
    """Returns full details of a manifest including all individual stops."""
    with get_connection_db() as conn:
        query = '''
            SELECT m."manifestID", d.name as "driverName", m."dispatchedAt", 
                   e."orderID", e."deliveryAddress"
            FROM manifests m
            LEFT JOIN drivers d ON m."driverID" = d."driverID"
            JOIN manifest_entries e ON m."manifestID" = e."manifestID"
            WHERE m."manifestID" = %s
        '''
        rows = conn.execute(query, (m_id,)).fetchall()

        if not rows:
            return jsonify({"error": "Manifest not found"}), 404

        detail = {
            "manifestID": rows[0]["manifestID"],
            "driverName": rows[0]["driverName"],
            "dispatchedAt": rows[0]["dispatchedAt"],
            "stops": [{"orderID": r["orderID"], "deliveryAddress": r["deliveryAddress"]} for r in rows]
        }
        return jsonify(detail), 200


@app.route('/manifests/driver/<int:driver_id>', methods=['GET'])
def get_manifest_by_driver(driver_id):
    """Returns the most recent manifest for a given driver."""
    with get_connection_db() as conn:
        m_row = conn.execute(
            'SELECT "manifestID", "dispatchedAt" FROM manifests WHERE "driverID" = %s ORDER BY "manifestID" DESC LIMIT 1',
            (driver_id,)
        ).fetchone()

        if not m_row:
            return jsonify({"manifest": None, "message": "No manifest found for driver"}), 404

        manifest_id = m_row['manifestID']
        d_row = conn.execute('SELECT name FROM drivers WHERE "driverID" = %s', (driver_id,)).fetchone()
        driver_name = d_row['name'] if d_row else f"Driver {driver_id}"

        entries = conn.execute(
            'SELECT "orderID", "deliveryAddress" FROM manifest_entries WHERE "manifestID" = %s',
            (manifest_id,)
        ).fetchall()

        return jsonify({
            "manifestID": manifest_id,
            "driverID": driver_id,
            "driverName": driver_name,
            "dispatchedAt": m_row['dispatchedAt'],
            "stops": [{"orderID": r["orderID"], "deliveryAddress": r["deliveryAddress"]} for r in entries]
        }), 200


@app.route('/manifests/order/<order_id>', methods=['GET'])
def get_manifest_by_order(order_id):
    """Returns the manifest and driver for a specific order."""
    with get_connection_db() as conn:
        row = conn.execute('''
            SELECT m."manifestID", m."driverID", d.name as "driverName"
            FROM manifest_entries e
            JOIN manifests m ON e."manifestID" = m."manifestID"
            LEFT JOIN drivers d ON m."driverID" = d."driverID"
            WHERE e."orderID" = %s
            ORDER BY m."manifestID" DESC LIMIT 1
        ''', (str(order_id),)).fetchone()

        if not row:
            return jsonify({"error": "Order manifest not found"}), 404

        return jsonify(dict(row)), 200


if __name__ == '__main__':
    init_db()
    threading.Thread(target=start_event_listener, daemon=True).start()
    threading.Thread(target=start_periodic_publisher, daemon=True).start()
    port = int(os.getenv('PORT', 8006))
    app.run(host='0.0.0.0', port=port)

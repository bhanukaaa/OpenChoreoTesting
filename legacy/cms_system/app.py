import os
import datetime
import time
import xml.etree.ElementTree as ET
from utility.config import configs
from utility.db_helper import get_db_connection

from flask import Flask, request, Response, jsonify

app = Flask(__name__)


# ── DB initialisation ─────────────────────────────────────────────────────────

def _get_conn():
    return get_db_connection(schema='legacy_cms')


def _init_db() -> None:
    """Create tables if they do not already exist."""
    with _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id              SERIAL PRIMARY KEY,
                "userID"        TEXT NOT NULL,
                "deliveryAddress" TEXT NOT NULL,
                "receiverName"  TEXT NOT NULL,
                "packageType"   TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'ACTIVE',
                "createdAt"     TEXT NOT NULL,
                "cancelledAt"   TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS order_packages (
                id          SERIAL PRIMARY KEY,
                "orderID"   INTEGER NOT NULL REFERENCES orders(id),
                "packageID" TEXT NOT NULL
            )
        """)
        conn.commit()


# ── Core CMS functions ────────────────────────────────────────────────────────

def recordOrder(
    userID: str,
    address: str,
    receiverName: str,
    packageType: str,
    packageIDs: list,
) -> dict:
    if not userID:
        raise ValueError("userID is required.")
    if not address:
        raise ValueError("address is required.")
    if not receiverName:
        raise ValueError("receiverName is required.")
    if not packageType:
        raise ValueError("packageType is required.")
    if not packageIDs:
        raise ValueError("packageIDs must contain at least one package.")

    created_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    with _get_conn() as conn:
        cursor = conn.execute(
            """
            INSERT INTO orders ("userID", "deliveryAddress", "receiverName", "packageType", status, "createdAt")
            VALUES (%s, %s, %s, %s, 'ACTIVE', %s) RETURNING id
            """,
            (userID, address, receiverName, packageType, created_at),
        )
        order_id = cursor.fetchone()['id']

        conn.executemany(
            'INSERT INTO order_packages ("orderID", "packageID") VALUES (%s, %s)',
            [(order_id, pkg) for pkg in packageIDs],
        )
        conn.commit()

    print(f"[CMS] Order recorded  → ID {order_id}", flush=True)
    return {"cmsRefID": order_id}


def cancelOrderRecord(cmsRefID: int) -> dict:
    if cmsRefID is None:
        raise ValueError("cmsRefID is required.")

    with _get_conn() as conn:
        row = conn.execute(
            "SELECT id, status FROM orders WHERE id = %s", (cmsRefID,)
        ).fetchone()

        if row is None:
            print(
                f"[CMS] Cancel failed   → ID {cmsRefID} not found.", flush=True)
            return {"success": False}

        if row["status"] == "CANCELLED":
            print(
                f"[CMS] Cancel skipped  → ID {cmsRefID} already cancelled.", flush=True)
            return {"success": False}

        cancelled_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        conn.execute(
            'UPDATE orders SET status = \'CANCELLED\', "cancelledAt" = %s WHERE id = %s',
            (cancelled_at, cmsRefID),
        )
        conn.commit()

    print(f"[CMS] Order cancelled → ID {cmsRefID}", flush=True)
    return {"success": True}


def getOrderRecord(cmsRefID: int):
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM orders WHERE id = %s", (cmsRefID,)
        ).fetchone()

        if row is None:
            return None

        packages = conn.execute(
            'SELECT "packageID" FROM order_packages WHERE "orderID" = %s', (
                cmsRefID,)
        ).fetchall()

    return {
        **dict(row),
        "packageIDs": [p["packageID"] for p in packages],
    }


# ── Flask XML endpoints ───────────────────────────────────────────────────────

@app.route('/health', methods=['GET'])
@app.route('/healthz', methods=['GET'])
@app.route('/live', methods=['GET'])
def liveness():
    return jsonify({"status": "healthy", "service": "legacy_cms"}), 200


@app.route('/ready', methods=['GET'])
@app.route('/readyz', methods=['GET'])
def readiness():
    try:
        with _get_conn() as conn:
            conn.execute("SELECT 1")
        return jsonify({"status": "ready", "service": "legacy_cms"}), 200
    except Exception as e:
        return jsonify({"status": "not ready", "error": str(e)}), 503


@app.route('/legacy/orders', methods=['POST'])
def receive_order():
    content_type = request.headers.get('Content-Type', '')

    if 'application/xml' not in content_type:
        return Response("<error>Invalid Media Type</error>", status=415, mimetype='application/xml')

    xml_data = request.data.decode('utf-8')

    try:
        root = ET.fromstring(xml_data.strip())

        user_id = root.findtext('userID', '').strip()
        address = root.findtext('address', '').strip()
        receiver_name = root.findtext('receiverName', '').strip()
        package_type = root.findtext('packageType', '').strip()
        package_ids = [p.text.strip()
                       for p in root.findall('packageIDs/packageID') if p.text]

        result = recordOrder(user_id, address,
                             receiver_name, package_type, package_ids)

        if configs.get("enableLegacyDelay"):
            time.sleep(configs["legacyDelay"].get("CMS", 1))

        return Response(
            f"<response><status>SUCCESS</status><cmsRefID>{result['cmsRefID']}</cmsRefID></response>",
            status=200,
            mimetype='application/xml',
        )

    except ValueError as e:
        return Response(
            f"<response><status>ERROR</status><message>{e}</message></response>",
            status=400,
            mimetype='application/xml',
        )
    except ET.ParseError as e:
        return Response(
            f"<response><status>ERROR</status><message>Malformed XML: {e}</message></response>",
            status=400,
            mimetype='application/xml',
        )


@app.route('/legacy/orders/<int:cms_ref_id>/cancel', methods=['POST'])
def cancel_order(cms_ref_id: int):
    result = cancelOrderRecord(cms_ref_id)
    status_code = 200 if result["success"] else 404
    success_str = "true" if result["success"] else "false"
    return Response(
        f"<response><success>{success_str}</success></response>",
        status=status_code,
        mimetype='application/xml',
    )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    _init_db()

    HOST = os.getenv('HOST', '0.0.0.0')
    PORT = int(os.getenv('PORT', 8003))

    print(f"[CMS] Starting Flask server on {HOST}:{PORT}...", flush=True)
    app.run(host=HOST, port=PORT, debug=False)

import os
import uuid
from flask import Flask, request, jsonify
from utility.db_helper import get_db_connection

app = Flask(__name__)

VALID_ROLES = {'vendor', 'driver'}


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_connection():
    return get_db_connection(schema='auth')


def init_db():
    with get_connection() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                "userID"    SERIAL PRIMARY KEY,
                username    TEXT NOT NULL UNIQUE,
                password    TEXT NOT NULL,
                role        TEXT NOT NULL DEFAULT 'vendor'
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                token      TEXT PRIMARY KEY,
                "userID"   INTEGER NOT NULL REFERENCES users("userID"),
                expires_at TIMESTAMP DEFAULT (NOW() + INTERVAL '24 hours')
            )
        ''')

        # Check if users table is empty, then seed
        try:
            cur = conn.execute("SELECT COUNT(*) FROM users")
            count = cur.fetchone()[0]
            if count == 0:
                print(
                    "[*] Seeding initial users (Drivers 1-3, Vendors 4-6)...", flush=True)
                # Drivers (IDs 1-3)
                drivers = [
                    (1, 'driver1', '123', 'driver'),
                    (2, 'driver2', '123', 'driver'),
                    (3, 'driver3', '123', 'driver')
                ]
                conn.executemany(
                    'INSERT INTO users ("userID", username, password, role) VALUES (%s, %s, %s, %s)', drivers)

                # Vendors (IDs 4-6)
                vendors = [
                    (4, 'vendor1', '123', 'vendor'),
                    (5, 'vendor2', '123', 'vendor'),
                    (6, 'vendor3', '123', 'vendor')
                ]
                conn.executemany(
                    'INSERT INTO users ("userID", username, password, role) VALUES (%s, %s, %s, %s)', vendors)

                # Reset sequence after manual ID inserts
                conn.execute("SELECT setval(pg_get_serial_sequence('users', 'userID'), COALESCE(MAX(\"userID\"), 1)) FROM users")
        except Exception as e:
            print(f"[!] User seeding error: {e}", flush=True)

        conn.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_user(conn, username):
    """Look up a user by username — usernames are unique."""
    return conn.execute(
        "SELECT * FROM users WHERE username = %s", (username,)
    ).fetchone()


def create_user(conn, username, password, role):
    """Insert a new user and return their generated userID."""
    cur = conn.execute(
        'INSERT INTO users (username, password, role) VALUES (%s, %s, %s) RETURNING "userID"',
        (username, password, role)
    )
    userID = cur.fetchone()['userID']
    print(
        f"[*] New user created: {username} (role={role}, id={userID})", flush=True)
    return userID


def create_session(conn, userID):
    """Insert a new session token for a user and return the token."""
    token = str(uuid.uuid4())
    conn.execute(
        'INSERT INTO sessions (token, "userID") VALUES (%s, %s)',
        (token, userID)
    )
    return token


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route('/health', methods=['GET'])
@app.route('/healthz', methods=['GET'])
@app.route('/live', methods=['GET'])
def liveness():
    return jsonify({"status": "healthy", "service": "auth"}), 200


@app.route('/ready', methods=['GET'])
@app.route('/readyz', methods=['GET'])
def readiness():
    try:
        with get_connection() as conn:
            conn.execute("SELECT 1")
        return jsonify({"status": "ready", "service": "auth"}), 200
    except Exception as e:
        return jsonify({"status": "not ready", "error": str(e)}), 503


@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username', '').strip()
    password = data.get('password', '').strip()

    if not username or not password:
        return jsonify({"error": "username and password are required"}), 400

    try:
        with get_connection() as conn:
            user = get_user(conn, username)

            if user is None:
                role = "vendor"
                userID = create_user(conn, username, password, role)
                print(
                    f"[*] Auto-registered new user: {username} (role={role})", flush=True)

            elif user['password'] != password:
                return jsonify({"error": "Invalid password"}), 401

            else:
                userID = user['userID']
                role = user['role']
                print(
                    f"[*] Existing user logged in: {username} (role={role})", flush=True)

            token = create_session(conn, userID)
            conn.commit()

        print(f"[*] Token generated for {username}: {token}", flush=True)
        return jsonify({"token": token, "userID": userID, "username": username, "role": role}), 200

    except Exception as e:
        print(f"[!] Database error in /login: {e}", flush=True)
        return jsonify({"error": "Database error"}), 500


@app.route('/verify', methods=['POST'])
def verify():
    data = request.get_json() or {}
    token = data.get('token', '').strip()

    if token.startswith('Bearer '):
        token = token.split(' ', 1)[1].strip()

    if not token:
        return jsonify({"valid": False, "error": "token is required"}), 400

    try:
        with get_connection() as conn:
            row = conn.execute('''
                SELECT u."userID", u.username, u.role
                FROM sessions s
                JOIN users u ON s."userID" = u."userID"
                WHERE s.token = %s
                AND s.expires_at > NOW()
            ''', (token,)).fetchone()

        if row:
            return jsonify({
                "valid":    True,
                "userID":   row['userID'],
                "username": row['username'],
                "role":     row['role']
            }), 200

        return jsonify({"valid": False, "error": "Invalid or expired token"}), 401

    except Exception as e:
        print(f"[!] Database error in /verify: {e}", flush=True)
        return jsonify({"error": "Database error"}), 500


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    init_db()
    port = int(os.getenv('PORT', 8002))
    app.run(host='0.0.0.0', port=port)

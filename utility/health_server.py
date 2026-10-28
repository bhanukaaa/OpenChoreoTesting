import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ('/health', '/healthz', '/live', '/ready', '/readyz'):
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status":"healthy"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress noisy health probe logs
        pass


def start_health_server(default_port=8080, service_name="worker"):
    """Starts a non-blocking background HTTP health probe server."""
    port_env = os.getenv('HEALTH_PORT', os.getenv('PORT'))
    port = int(port_env) if port_env else default_port

    if port <= 0:
        return

    def run_server():
        try:
            server = HTTPServer(('0.0.0.0', port), HealthHandler)
            print(f"[*] Health probe server for {service_name} listening on port {port}", flush=True)
            server.serve_forever()
        except Exception as e:
            print(f"[!] Health probe server failed to bind on port {port}: {e}", flush=True)

    t = threading.Thread(target=run_server, daemon=True)
    t.start()

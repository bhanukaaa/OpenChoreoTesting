import os
import json
import socket
import utility.rabbit_helper as rabbit_helper

EXCHANGE = os.getenv('RABBITMQ_EXCHANGE', 'swifttrack_events')
LEGACY_WMS_HOST = os.getenv('LEGACY_WMS_HOST', 'wms_system')
LEGACY_WMS_PORT = int(os.getenv('LEGACY_WMS_PORT', 8004))


def handle_order(ch, method, properties, body):
    data = json.loads(body)
    order_id = data.get('orderID')
    package_name = data.get('packageName', 'Unknown')
    package_type = data.get('packageType', 'Standard')
    package_weight = data.get('packageWeight', 1.0)

    print(
        f"[*] WMS: Received Order {order_id}. Connecting to Legacy TCP WMS...", flush=True)
    package_id = registerPackage(
        order_id, package_type, package_weight, package_name)

    if package_id:
        newData = {
            "orderID": order_id,
            "packageIDs": [package_id],
            "userID": data.get('userID'),
            "receiverName": data.get('receiverName'),
            "deliveryAddress": data.get('deliveryAddress'),
            "packageType": package_type
        }
        conn = rabbit_helper.get_connection()
        new_ch = conn.channel()

        rabbit_helper.publish_event(new_ch, EXCHANGE, 'wms.package.created', newData)
        
        conn.close()
        print(
            f"[+] WMS: Package {package_id} registered for order {order_id}.", flush=True)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    else:
        print(
            f"[!] WMS: registerPackage failed for order {order_id}. Notifying system.", flush=True)
        conn = rabbit_helper.get_connection()
        new_ch = conn.channel()

        rabbit_helper.publish_event(new_ch, EXCHANGE, 'wms.order.cancelled', {
            "orderID": order_id,
            "cmsRefID": None,
            "packageID": None,
            "reason": "WMS registerPackage failed"
        })
        
        conn.close()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def registerPackage(order_id, packageType, packageWeight, packageName):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((LEGACY_WMS_HOST, LEGACY_WMS_PORT))
        payload = f"REGISTER|{order_id}|{packageType}|{packageWeight}|{packageName}"
        s.sendall(payload.encode('utf-8'))
        data = s.recv(1024).decode('utf-8')
        s.close()
        return data.split("|")[1] if data.startswith("SUCCESS|") else None
    except Exception as e:
        print(f"[!] Error registering: {e}", flush=True)
        return None


def voidPackage(packageID):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((LEGACY_WMS_HOST, LEGACY_WMS_PORT))
        payload = f"VOID|{packageID}"
        s.sendall(payload.encode('utf-8'))
        data = s.recv(1024).decode('utf-8')
        s.close()
        return data.startswith("SUCCESS|")
    except Exception as e:
        print(f"[!] Error voiding: {e}", flush=True)
        return False


def handle_dispatch_lock(ch, method, properties, body):
    """Consumes dispatch.getRoutes and updates package status to DISPATCHED."""
    data = json.loads(body)
    order_ids = data.get('orderIDs', [])
    print(
        f"[*] WMS: Locking {len(order_ids)} orders as DISPATCHED...", flush=True)

    for oid in order_ids:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((LEGACY_WMS_HOST, LEGACY_WMS_PORT))
            s.sendall(f"UPDATE_STATUS|{oid}|DISPATCHED".encode('utf-8'))
            s.close()
        except Exception as e:
            print(f"[!] Error locking order {oid}: {e}", flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def get_ready_packages():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((LEGACY_WMS_HOST, LEGACY_WMS_PORT))
        s.sendall("GET_READY".encode('utf-8'))
        data = s.recv(4096).decode('utf-8')
        s.close()
        if data.startswith("SUCCESS|"):
            content = data.split("|")[1]
            return [] if content == "NONE" else content.split(",")
        return []
    except Exception as e:
        print(f"[!] Error fetching ready: {e}", flush=True)
        return []


def handle_dispatch_request(ch, method, properties, body):
    order_ids = get_ready_packages()
    response_data = {"orderIDs": order_ids, "count": len(order_ids)}
    conn = rabbit_helper.get_connection()
    new_ch = conn.channel()

    rabbit_helper.publish_event(new_ch, EXCHANGE, 'wms.returnPackages', response_data)

    conn.close()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def handle_cancelled_order(ch, method, properties, body):
    data = json.loads(body)
    package_id = data.get('packageID')
    order_id = data.get('orderID')

    if package_id:
        voidPackage(package_id)
    
    # publish wms.order.cancelled
    conn = rabbit_helper.get_connection()
    new_ch = conn.channel()

    rabbit_helper.publish_event(new_ch, EXCHANGE, 'wms.order.cancelled', {
        "orderID": order_id,
        "cmsRefID": data.get('cmsRefID'),
        "packageID": package_id,
        "reason": "Order cancelled by CMS"
    })
    
    conn.close()

    ch.basic_ack(delivery_tag=method.delivery_tag)



def handle_start_delivery(ch, method, properties, body):
    data = json.loads(body)
    order_ids = data.get('orderIDs', [])
    print(
        f"[*] WMS: Marking {len(order_ids)} orders as IN_TRANSIT...", flush=True)

    for oid in order_ids:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((LEGACY_WMS_HOST, LEGACY_WMS_PORT))
            s.sendall(f"UPDATE_STATUS|{oid}|IN_TRANSIT".encode('utf-8'))
            s.close()
        except Exception as e:
            print(
                f"[!] Error updating order {oid} to IN_TRANSIT: {e}", flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    from utility.health_server import start_health_server
    start_health_server(default_port=8084, service_name="wms_adapter")

    def callback(ch, method, properties, body):
        if method.routing_key == 'dispatch.getAvailablePackages':
            handle_dispatch_request(ch, method, properties, body)
        elif method.routing_key == 'dispatch.getRoutes':
            handle_dispatch_lock(ch, method, properties, body)
        elif method.routing_key == 'cms.order.cancelled':
            handle_cancelled_order(ch, method, properties, body)
        elif method.routing_key == 'delivery.startDelivery':
            handle_start_delivery(ch, method, properties, body)
        else:
            handle_order(ch, method, properties, body)
    
    conn = rabbit_helper.get_connection()
    ch = conn.channel()
    
    queue_config = {
        'queue_name': 'wms_queue',
        'durable': True,
        'routing_keys': [
            'order.initiated',
            'cms.order.cancelled',
            'dispatch.getAvailablePackages',
            'dispatch.getRoutes',
            'delivery.startDelivery'
        ],
        'auto_ack': False
    }
    
    rabbit_helper.listen_to_events(ch, EXCHANGE, queue_config, callback)
    print("[*] WMS Adapter waiting for events...", flush=True)

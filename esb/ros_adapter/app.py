import os
import json
import requests
import utility.rabbit_helper as rabbit_helper

# --- Configuration ---
EXCHANGE = os.getenv('RABBITMQ_EXCHANGE', 'swifttrack_events')
LEGACY_ROS_HOST = os.getenv('LEGACY_ROS_HOST', 'ros_system')
LEGACY_ROS_PORT = int(os.getenv('LEGACY_ROS_PORT', 8005))
LEGACY_ROS_URL = os.getenv('LEGACY_ROS_URL', f"http://{LEGACY_ROS_HOST}:{LEGACY_ROS_PORT}")

# --- Legacy System API Calls ---


def uploadAddress(order_id, address):
    """Call Legacy ROS REST API to add a delivery location."""
    try:
        url = f"{LEGACY_ROS_URL}/legacy/address"
        payload = {"orderID": order_id, "deliveryAddress": address}
        response = requests.post(url, json=payload, timeout=10)
        return response.status_code == 201
    except Exception as e:
        print(f"[!] ROS: Error in uploadAddress: {e}", flush=True)
        return False


def removeAddress(order_id):
    """Call Legacy ROS REST API to remove a delivery location."""
    try:
        url = f"{LEGACY_ROS_URL}/legacy/address/{order_id}"
        response = requests.delete(url, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"[!] ROS: Error in removeAddress: {e}", flush=True)
        return False


def getOptimizedRoutes(order_ids, num_trucks):
    """Call Legacy ROS REST API to get randomized truck routes."""
    try:
        url = f"{LEGACY_ROS_URL}/legacy/optimize"
        payload = {"orderIDs": order_ids, "numTrucks": num_trucks}
        response = requests.post(url, json=payload, timeout=15)

        if response.status_code == 200:
            return response.json().get("routes")
        else:
            print(
                f"[!] ROS: Optimization failed with status {response.status_code}", flush=True)
            return None
    except Exception as e:
        print(f"[!] ROS: Error in getOptimizedRoutes: {e}", flush=True)
        return None

# --- RabbitMQ Event Handlers ---


def handle_get_routes(ch, method, properties, body):
    """Consume 'dispatch.getRoutes' -> optimize -> publish 'ros.returnRoutes'."""
    data = json.loads(body)
    order_ids = data.get('orderIDs', [])
    num_trucks = data.get('numTrucks', 1)

    # Calling the updated Legacy ROS
    routes = getOptimizedRoutes(order_ids, num_trucks)

    if routes is not None:
        conn = rabbit_helper.get_connection()
        new_ch = conn.channel()

        # The 'routes' variable now contains the nested lists of objects
        rabbit_helper.publish_event(new_ch, EXCHANGE, 'ros.returnRoutes', {"routes": routes})
        
        conn.close()
        print(f"[+] ROS: Published paired routes for manifest generation.", flush=True)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def handle_order_recorded(ch, method, properties, body):
    data = json.loads(body)
    order_id = data.get('orderID')
    address = data.get('deliveryAddress')
    cms_ref_id = data.get('cmsRefID')
    package_ids = data.get('packageIDs', [])

    if uploadAddress(order_id, address):
        conn = rabbit_helper.get_connection()
        new_ch = conn.channel()

        rabbit_helper.publish_event(new_ch, EXCHANGE, 'ros.address.added', {
            "orderID": order_id,
            "cmsRefID": cms_ref_id,
            "packageIDs": package_ids
        })

        conn.close()
        print(f"[+] ROS: Address added for {order_id}.", flush=True)
    else:
        print(f"[!] ROS: Triggering CMS + WMS compensation for order {order_id}.", flush=True)
        conn = rabbit_helper.get_connection()
        new_ch = conn.channel()
        rabbit_helper.publish_event(new_ch, EXCHANGE, 'ros.order.cancelled', {
            "orderID": order_id,
            "cmsRefID": cms_ref_id,
            "packageID": package_ids[0] if package_ids else None,
            "reason": "ROS uploadAddress failed"
        })
        conn.close()

    ch.basic_ack(delivery_tag=method.delivery_tag)


def handle_route_cancel(ch, method, properties, body):
    data = json.loads(body)
    order_id = data.get('orderID')

    if removeAddress(order_id):
        conn = rabbit_helper.get_connection()
        new_ch = conn.channel()

        rabbit_helper.publish_event(new_ch, EXCHANGE, 'ros.address.deleted', {"orderID": order_id})
        
        conn.close()
        print(f"[+] ROS: Address removed for {order_id}.", flush=True)

    ch.basic_ack(delivery_tag=method.delivery_tag)

# --- Main ---

if __name__ == "__main__":
    from utility.health_server import start_health_server
    start_health_server(default_port=8085, service_name="ros_adapter")

    def callback(ch, method, properties, body):
        if method.routing_key == 'dispatch.getRoutes':
            handle_get_routes(ch, method, properties, body)
        elif method.routing_key == 'order.route.cancel':
            handle_route_cancel(ch, method, properties, body)
        elif method.routing_key == 'cms.order.recorded':
            handle_order_recorded(ch, method, properties, body)
    
    conn = rabbit_helper.get_connection()
    ch = conn.channel()
    
    queue_config = {
        'queue_name': 'ros_queue',
        'durable': True,
        'routing_keys': [
            'cms.order.recorded',
            'order.route.cancel',
            'dispatch.getRoutes'
        ],
        'dead_letter_exchange': 'swifttrack_dlx',
        'dead_letter_routing_key': 'ros_dead',
        'dead_letter_queue_name': 'ros_dead_letter_queue',
        'auto_ack': False
    }
    
    rabbit_helper.listen_to_events(ch, EXCHANGE, queue_config, callback)
    print("[*] ROS Adapter active and listening for route optimization requests...", flush=True)

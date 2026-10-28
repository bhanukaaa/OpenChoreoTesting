import os
import json
import time
import requests
import xml.etree.ElementTree as ET
import utility.rabbit_helper as rabbit_helper

EXCHANGE = os.getenv('RABBITMQ_EXCHANGE', 'swifttrack_events')
LEGACY_CMS_HOST = os.getenv('LEGACY_CMS_HOST', 'cms_system')
LEGACY_CMS_PORT = int(os.getenv('LEGACY_CMS_PORT', 8003))
LEGACY_CMS_URL = os.getenv('LEGACY_CMS_URL', f"http://{LEGACY_CMS_HOST}:{LEGACY_CMS_PORT}/legacy/orders")
QUEUE_NAME = os.getenv('CMS_QUEUE_NAME', 'cms_queue')


def recordOrder(user_id, address, receiver_name, package_type, package_ids):
    package_id_tags = "".join(
        f"<packageID>{p}</packageID>\n" for p in package_ids)

    xml_payload = f"""
        <?xml version="1.0" encoding="UTF-8"?>
        <order_record>
            <userID>{user_id}</userID>
            <address>{address}</address>
            <receiverName>{receiver_name}</receiverName>
            <packageType>{package_type}</packageType>
            <packageIDs>{package_id_tags}</packageIDs>
        </order_record>
    """

    try:
        headers = {'Content-Type': 'application/xml'}
        response = requests.post(
            LEGACY_CMS_URL, data=xml_payload, headers=headers)

        if response.status_code != 200:
            print(
                f"[!] CMS: recordOrder failed, status: {response.status_code}", flush=True)
            return {"cmsRefID": None}

        response_xml = ET.fromstring(response.text)

        status = response_xml.findtext("status", "").strip()
        if status != "SUCCESS":
            message = response_xml.findtext("message", "Unknown error")
            print(
                f"[!] CMS: recordOrder rejected by legacy system: {message}", flush=True)
            return {"cmsRefID": None}

        cms_ref_id = int(response_xml.findtext("cmsRefID"))
        print(
            f"[+] CMS: Order recorded successfully, cmsRefID: {cms_ref_id}", flush=True)
        return {"cmsRefID": cms_ref_id}

    except Exception as e:
        print(f"[!] CMS: recordOrder exception: {str(e)}", flush=True)
        return {"cmsRefID": None}


def cancelOrderRecord(cms_ref_id):
    try:
        cancel_url = f"{LEGACY_CMS_URL}/{cms_ref_id}/cancel"
        response = requests.post(cancel_url)
        response_xml = ET.fromstring(response.text)
        success_text = response_xml.findtext(
            "success", "false").strip().lower()
        success = success_text == "true"

        if success:
            print(
                f"[+] CMS: Order {cms_ref_id} cancelled successfully", flush=True)
        else:
            print(
                f"[!] CMS: Cancel failed for {cms_ref_id} — not found or already cancelled", flush=True)

        return {"success": success}

    except Exception as e:
        print(f"[!] CMS: cancelOrderRecord exception: {str(e)}", flush=True)
        return {"success": False}


def compensate_wms(publish_channel, order_id, package_ids):
    """
    Saga compensation — called when CMS fails after WMS already registered the package.
    Publishes order.cancelled so WMS voids the package.
    """
    print(
        f"[!] CMS: Triggering WMS compensation for order {order_id}.", flush=True)
    
    rabbit_helper.publish_event(publish_channel, EXCHANGE, 'cms.order.cancelled', {
        "orderID": order_id,
        "packageID": package_ids[0] if package_ids else None,
        "reason": "CMS recordOrder failed"
    })


def handle_package_created(ch, method, properties, body, publish_channel):
    try:
        data = json.loads(body)
        print(
            f"[*] CMS: Received package event for Package {data['packageIDs']}", flush=True)

        result = recordOrder(
            user_id=data['userID'],
            address=data['deliveryAddress'],
            receiver_name=data['receiverName'],
            package_type=data['packageType'],
            package_ids=data['packageIDs']
        )

        if result["cmsRefID"] is None:
            compensate_wms(publish_channel,
                           data['orderID'], data.get('packageIDs', []))
            ch.basic_nack(delivery_tag=method.delivery_tag,
                          requeue=False)  # send to DLQ
            return

        newData = {
            "orderID": data['orderID'],
            "packageIDs": data['packageIDs'],
            "userID": data['userID'],
            "receiverName": data['receiverName'],
            "deliveryAddress": data['deliveryAddress'],
            "cmsRefID": result["cmsRefID"]
        }

        rabbit_helper.publish_event(publish_channel, EXCHANGE, 'cms.order.recorded', newData)
        
        print(f"[+] CMS: Published cms.order.recorded event downstream", flush=True)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"[!] CMS: Error processing message: {str(e)}", flush=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_cancelled_order(ch, method, properties, body, publish_channel):
    try:
        data = json.loads(body)
        cms_ref_id = data.get('cmsRefID')

        if cms_ref_id:
            print(
                f"[*] CMS: Cancelling Order with CMS Ref ID {cms_ref_id}", flush=True)
            cancelOrderRecord(cms_ref_id)
        else:
            print("[!] CMS: Cancellation received but no cmsRefID found.", flush=True)

        ch.basic_ack(delivery_tag=method.delivery_tag)

        # publish cms.order.cancelled
        rabbit_helper.publish_event(publish_channel, EXCHANGE, 'cms.order.cancelled', {
            "orderID": data.get('orderID'),
            "packageID": data.get('packageID'),
            "reason": data.get('reason', 'Order cancelled')
        })

    except Exception as e:
        print(f"[!] CMS Error processing cancellation: {e}", flush=True)


if __name__ == "__main__":
    from utility.health_server import start_health_server
    start_health_server(default_port=8083, service_name="cms_adapter")

    conn = rabbit_helper.get_connection()
    consume_channel = conn.channel()
    publish_channel = conn.channel()

    def callback(ch, method, properties, body):
        if method.routing_key == 'ros.order.cancelled':
            handle_cancelled_order(ch, method, properties, body, publish_channel)
        elif method.routing_key == 'wms.package.created':
            handle_package_created(ch, method, properties, body, publish_channel)
        else:
            print(
                f"[!] CMS: Unknown routing key: {method.routing_key}", flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    queue_config = {
        'queue_name': QUEUE_NAME,
        'durable': True,
        'routing_keys': [
            'wms.package.created',
            'ros.order.cancelled'
        ],
        'dead_letter_exchange': 'swifttrack_dlx',
        'dead_letter_routing_key': 'cms_dead',
        'dead_letter_queue_name': 'cms_dead_letter_queue',
        'auto_ack': False
    }
    
    rabbit_helper.listen_to_events(consume_channel, EXCHANGE, queue_config, callback)
    print("[*] CMS Adapter waiting for events (package created & cancelled)...", flush=True)

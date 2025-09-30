import websocket
import json
import ssl

# Counter for the number of messages received
message_count = 0
# The number of messages to receive before closing
messages_to_receive = 10000

def on_message(ws, message):
    global message_count
    if message_count < messages_to_receive:
        print("--- NEW MESSAGE ---")
        # Pretty-print the JSON message
        payload = json.loads(message)
        print(json.dumps(payload, indent=2))
        message_count += 1
    else:
        # Once we have enough messages, close the connection
        ws.close()
        print("\nReceived 3 messages. Connection closed.")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### Connection closed ###")

if __name__ == "__main__":
    # Using SSL for a secure connection
    ws = websocket.WebSocketApp("wss://stream.binance.us:9443/ws/btcusdt@depth/btcusdc@depth/btcusdc@depth",
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)

    # The wss endpoint requires SSL
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
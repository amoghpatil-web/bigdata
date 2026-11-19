import socket
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(("localhost", 9999))
server.listen(1)
print("Waiting for Spark to connect on port 9999...")
conn, _ = server.accept()
print("Spark connected! Type words and press Enter (type 'exit' to stop):")
try:
    while True:
        msg = input("> ")
        if msg.lower() == "exit":
            break
        conn.sendall((msg + "\n").encode("utf-8"))

finally:
    conn.close()
    server.close()

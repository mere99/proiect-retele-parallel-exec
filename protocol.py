"""
Protocol de retea pentru cluster-ul de executie paralela.

Foloseste acelasi framing ca la curs (s4 text-proto_tcp):
  mesaj = "<TOTAL_LENGTH> <PAYLOAD>"
unde PAYLOAD este JSON UTF-8.

Exemplu payload:
  {"type": "register", "node_id": "node2:9002", "listen_port": 9002}
"""

import json
import socket

# Dimensiunea bucatii la recv (ca in exemplele de la curs).
BUFFER_SIZE = 4096


def encode_message(payload: dict) -> bytes:
    """
    Impacheteaza un dict ca mesaj cu header de lungime.
    TOTAL_LENGTH = numarul total de caractere al liniei (cifre + spatiu + JSON).
    """
    body = json.dumps(payload, ensure_ascii=True)
    content_length = len(body)
    total_length = content_length + len(str(content_length)) + 1
    frame = f"{total_length} {body}"
    return frame.encode("utf-8")


def decode_message(data: str) -> dict:
    """Parseaza payload-ul JSON din mesajul deja citit complet."""
    # Primul token este TOTAL_LENGTH; restul este JSON.
    parts = data.split(" ", 1)
    if len(parts) < 2:
        raise ValueError("invalid frame: missing payload")
    return json.loads(parts[1])


def recv_message(sock: socket.socket) -> dict | None:
    """
    Citeste un mesaj complet de pe socket.
    Returneaza None daca conexiunea s-a inchis.
    """
    data = sock.recv(BUFFER_SIZE)
    if not data:
        return None

    string_data = data.decode("utf-8")
    full_data = string_data

    try:
        message_length = int(string_data.split(" ")[0])
    except (ValueError, IndexError):
        raise ValueError("invalid frame header")

    remaining = message_length - len(string_data)
    while remaining > 0:
        chunk = sock.recv(BUFFER_SIZE)
        if not chunk:
            return None
        piece = chunk.decode("utf-8")
        full_data += piece
        remaining -= len(piece)

    return decode_message(full_data)


def send_message(sock: socket.socket, payload: dict) -> None:
    """Trimite un mesaj JSON cu framing."""
    sock.sendall(encode_message(payload))

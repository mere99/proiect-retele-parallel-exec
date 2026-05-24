"""
Nod hibrid: server + client in acelasi proces (tema 19).

Flux principal:
1. Porneste server TCP pe NODE_PORT (accepta conexiuni cluster).
2. Incearca sa se conecteze la lista BOOTSTRAP (primul succes).
3. Trimite REGISTER cu portul de ascultare.
4. Propaga PEER_JOIN / PEER_LEAVE / LOAD_UPDATE in cluster.
5. Executa cereri paralele local sau pe nodul cu load minim.
"""

import os
import socket
import sys
import threading
import time

from cluster_state import ClusterState
from executor import Executor
from protocol import recv_message, send_message

# --- configurare din mediu (Docker) sau valori locale ---
NODE_HOST = os.getenv("NODE_HOST", "0.0.0.0")
NODE_PORT = int(os.getenv("NODE_PORT", "9001"))
NODE_NAME = os.getenv("NODE_NAME", "node1")
# Lista "host:port,host:port" - incercam pe rand pana la primul succes.
BOOTSTRAP = os.getenv("BOOTSTRAP", "")
# Daca 1, ruleaza un demo automat dupa conectare (util in video).
AUTO_DEMO = os.getenv("AUTO_DEMO", "0") == "1"

is_running = True


class HybridNode:
    def __init__(self):
        self.host = NODE_HOST
        self.port = NODE_PORT
        self.node_id = f"{NODE_NAME}:{NODE_PORT}"
        self.state = ClusterState(self.node_id, NODE_NAME, NODE_PORT)
        self.executor = Executor()

        # Conexiuni persistente: node_id -> socket
        self.peers_lock = threading.Lock()
        self.peer_sockets = {}
        # Conexiune catre upstream (nodul la care suntem client)
        self.upstream_sock = None
        self.upstream_lock = threading.Lock()
        # Evita intercalarea bytes la trimiteri concurente pe acelasi socket.
        self._send_locks = {}
        self._send_locks_meta = threading.Lock()

    def safe_send(self, sock: socket.socket, payload: dict) -> None:
        key = id(sock)
        with self._send_locks_meta:
            if key not in self._send_locks:
                self._send_locks[key] = threading.Lock()
            lock = self._send_locks[key]
        with lock:
            send_message(sock, payload)

    # ---------- propagare mesaje in cluster ----------

    def broadcast(self, payload: dict, except_sock: socket.socket | None = None) -> None:
        """Trimite mesajul tuturor peer-ilor conectati, mai putin except_sock."""
        with self.peers_lock:
            targets = list(self.peer_sockets.items())

        for _nid, sock in targets:
            if sock is except_sock:
                continue
            try:
                self.safe_send(sock, payload)
            except OSError:
                pass

        with self.upstream_lock:
            up = self.upstream_sock
        if up is not None and up is not except_sock:
            try:
                self.safe_send(up, payload)
            except OSError:
                pass

    def propagate_load(self) -> None:
        load = self.state.set_load(self.state.load)
        msg = {
            "type": "load_update",
            "node_id": self.node_id,
            "load": load,
        }
        print(f"[LOAD] {self.node_id} load={load}")
        self.broadcast(msg)

    def notify_peer_join(self, node_id: str, host: str, port: int, load: int = 0) -> None:
        self.state.upsert_peer(node_id, host, port, load)
        msg = {
            "type": "peer_join",
            "node_id": node_id,
            "host": host,
            "port": port,
            "load": load,
        }
        print(f"[CLUSTER] peer join {node_id} ({host}:{port}) load={load}")
        self.broadcast(msg)

    def notify_peer_leave(self, node_id: str) -> None:
        self.state.remove_peer(node_id)
        msg = {"type": "peer_leave", "node_id": node_id}
        print(f"[CLUSTER] peer leave {node_id}")
        self.broadcast(msg)

    # ---------- executie paralela ----------

    def handle_exec_local(
        self,
        requester: str,
        reply_sock: socket.socket,
        class_name: str,
        method_name: str,
        args_list: list,
        thread_count: int,
        request_id: str,
    ) -> None:
        """Ruleaza metoda pe fire reale si trimite rezultate partiale."""
        def send_safe(payload: dict) -> None:
            self.safe_send(reply_sock, payload)

        def send_partial(tid, body):
            send_safe(
                {
                    "type": "exec_result",
                    "request_id": request_id,
                    "thread_id": tid,
                    "requester": requester,
                    **body,
                }
            )

        finished = threading.Event()

        def on_done(_errors):
            self.state.add_load(-thread_count)
            self.propagate_load()
            send_safe(
                {
                    "type": "exec_done",
                    "request_id": request_id,
                    "requester": requester,
                }
            )
            finished.set()

        self.state.add_load(thread_count)
        self.propagate_load()

        if not self.executor.has_class(class_name):
            send_safe(
                {
                    "type": "error",
                    "request_id": request_id,
                    "message": f"class {class_name} not loaded; send class_data first",
                }
            )
            finished.set()
            return

        try:
            self.executor.run_parallel(
                class_name,
                method_name,
                args_list,
                thread_count,
                on_result=send_partial,
                on_done=on_done,
            )
            # Asteptam finalizarea inainte ca handle_connection sa mai citeasca de pe socket.
            finished.wait(timeout=120)
        except Exception as exc:
            self.state.add_load(-thread_count)
            self.propagate_load()
            send_safe(
                {
                    "type": "error",
                    "request_id": request_id,
                    "message": str(exc),
                }
            )
            finished.set()

    def request_exec_on_remote(
        self,
        target_host: str,
        target_port: int,
        class_name: str,
        method_name: str,
        args_list: list,
        thread_count: int,
    ) -> None:
        """Client: deschide conexiune scurta catre serverul ales."""
        request_id = f"{self.node_id}-{int(time.time() * 1000)}"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((target_host, int(target_port)))
            if not self.executor.has_class(class_name):
                local_path = os.path.join(
                    os.path.dirname(__file__), "tasks", f"{class_name}.py"
                )
                with open(local_path, "r", encoding="utf-8") as f:
                    source = f.read()
                self.safe_send(
                    s,
                    {
                        "type": "class_data",
                        "class_name": class_name,
                        "source": source,
                    },
                )
                ack = recv_message(s)
                if not ack or ack.get("type") == "error":
                    print("[ERROR] class transfer failed:", ack)
                    return

            self.safe_send(
                s,
                {
                    "type": "exec_request",
                    "request_id": request_id,
                    "requester": self.node_id,
                    "class_name": class_name,
                    "method_name": method_name,
                    "args": args_list,
                    "thread_count": thread_count,
                },
            )

            while True:
                msg = recv_message(s)
                if not msg:
                    break
                mtype = msg.get("type")
                if mtype == "exec_result":
                    print(
                        f"[RESULT] thread={msg.get('thread_id')} "
                        f"ok={msg.get('ok')} value={msg.get('result', msg.get('error'))}"
                    )
                elif mtype == "exec_done":
                    print(f"[DONE] request {request_id}")
                    break
                elif mtype == "error":
                    print("[ERROR]", msg.get("message"))
                    break

    def submit_exec(self, class_name: str, method_name: str, args_list: list, thread_count: int):
        """
        Alege serverul cu load minim (criteriu din README) si trimite cererea.
        """
        target_id, host, port = self.state.pick_min_load_server()
        print(
            f"[EXEC] target={target_id} ({host}:{port}) "
            f"threads={thread_count} {class_name}.{method_name}({args_list})"
        )
        if target_id == self.node_id:
            # Executie locala: loopback in acelasi container/proces.
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(("127.0.0.1", self.port))
                self._run_local_exec_session(
                    s, class_name, method_name, args_list, thread_count
                )
        else:
            self.request_exec_on_remote(
                host, port, class_name, method_name, args_list, thread_count
            )

    def _run_local_exec_session(
        self, sock, class_name, method_name, args_list, thread_count
    ):
        request_id = f"local-{int(time.time() * 1000)}"
        if not self.executor.has_class(class_name):
            path = os.path.join(os.path.dirname(__file__), "tasks", f"{class_name}.py")
            with open(path, "r", encoding="utf-8") as f:
                self.safe_send(
                    sock,
                    {"type": "class_data", "class_name": class_name, "source": f.read()},
                )
                recv_message(sock)

        self.safe_send(
            sock,
            {
                "type": "exec_request",
                "request_id": request_id,
                "requester": self.node_id,
                "class_name": class_name,
                "method_name": method_name,
                "args": args_list,
                "thread_count": thread_count,
            },
        )
        while True:
            msg = recv_message(sock)
            if not msg:
                break
            mtype = msg.get("type")
            if mtype == "exec_result":
                print(
                    f"[RESULT] thread={msg.get('thread_id')} "
                    f"value={msg.get('result', msg.get('error'))}"
                )
            elif mtype == "exec_done":
                print(f"[DONE] request finished")
                break
            elif mtype == "error":
                print("[ERROR]", msg.get("message"))
                break

    # ---------- handler conexiune TCP ----------

    def handle_connection(self, conn: socket.socket, addr) -> None:
        """
        Bucla pentru un client conectat (pattern handle_client de la curs).
        """
        peer_node_id = None
        with conn:
            while is_running:
                try:
                    msg = recv_message(conn)
                except (ValueError, OSError):
                    break
                if msg is None:
                    break

                mtype = msg.get("type")

                if mtype == "register":
                    peer_node_id = msg["node_id"]
                    host = msg.get("host", addr[0])
                    port = int(msg.get("listen_port", 0))
                    with self.peers_lock:
                        self.peer_sockets[peer_node_id] = conn
                    self.notify_peer_join(peer_node_id, host, port, 0)
                    self.safe_send(
                        conn,
                        {
                            "type": "ok",
                            "node_id": self.node_id,
                            "host": NODE_NAME,
                            "listen_port": self.port,
                            "load": self.state.load,
                        },
                    )
                    # Trimitem snapshot la noul peer.
                    for nid, info in self.state.snapshot().items():
                        if nid == peer_node_id:
                            continue
                        self.safe_send(
                            conn,
                            {
                                "type": "peer_join",
                                "node_id": nid,
                                "host": info["host"],
                                "port": info["port"],
                                "load": info["load"],
                            },
                        )

                elif mtype in ("peer_join", "peer_leave", "load_update"):
                    self._apply_cluster_message(msg, propagate=True, except_sock=conn)

                elif mtype == "class_data":
                    try:
                        self.executor.load_from_source(msg["class_name"], msg["source"])
                        self.safe_send(conn, {"type": "ok", "message": "class loaded"})
                    except Exception as exc:
                        self.safe_send(conn, {"type": "error", "message": str(exc)})

                elif mtype == "exec_request":
                    self.handle_exec_local(
                        msg.get("requester", "unknown"),
                        conn,
                        msg["class_name"],
                        msg["method_name"],
                        msg.get("args", []),
                        int(msg["thread_count"]),
                        msg.get("request_id", "noid"),
                    )

                elif mtype == "disconnect":
                    break

        if peer_node_id:
            with self.peers_lock:
                self.peer_sockets.pop(peer_node_id, None)
            self.notify_peer_leave(peer_node_id)

    # ---------- client upstream ----------

    def connect_upstream(self) -> None:
        if not BOOTSTRAP.strip():
            print("[INFO] no BOOTSTRAP; this node is cluster root")
            return

        servers = [s.strip() for s in BOOTSTRAP.split(",") if s.strip()]
        # Reincercari: in Docker nodurile pornesc aproape simultan.
        for attempt in range(30):
            for target in servers:
                host, _, port_str = target.partition(":")
                try:
                    port = int(port_str)
                except ValueError:
                    continue
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((host, port))
                    with self.upstream_lock:
                        self.upstream_sock = sock
                    self.safe_send(
                        sock,
                        {
                            "type": "register",
                            "node_id": self.node_id,
                            "host": NODE_NAME,
                            "listen_port": self.port,
                        },
                    )
                    print(f"[CONNECT] upstream {host}:{port}")
                    # Citim raspunsul initial (ok + lista peer_join) inainte de thread.
                    sock.settimeout(2.0)
                    try:
                        while True:
                            msg = recv_message(sock)
                            if not msg:
                                break
                            self._apply_cluster_message(msg, propagate=False)
                    except OSError:
                        pass
                    finally:
                        sock.settimeout(None)
                    threading.Thread(
                        target=self._upstream_reader, args=(sock,), daemon=True
                    ).start()
                    return
                except OSError as err:
                    print(f"[WARN] connect {host}:{port} attempt {attempt + 1}: {err}")
            time.sleep(1)

        print("[WARN] no upstream server available after retries")

    def _apply_cluster_message(self, msg: dict, propagate: bool, except_sock=None) -> None:
        """Actualizeaza starea locala; optional repropaga in cluster."""
        mtype = msg.get("type")
        if mtype == "ok":
            self.state.upsert_peer(
                msg["node_id"],
                msg.get("host", "unknown"),
                int(msg.get("listen_port", 0)),
                int(msg.get("load", 0)),
            )
        elif mtype == "peer_join":
            self.state.upsert_peer(
                msg["node_id"], msg["host"], int(msg["port"]), int(msg.get("load", 0))
            )
            if propagate:
                self.broadcast(msg, except_sock=except_sock)
        elif mtype == "peer_leave":
            self.state.remove_peer(msg["node_id"])
            if propagate:
                self.broadcast(msg, except_sock=except_sock)
        elif mtype == "load_update":
            self.state.update_peer_load(msg["node_id"], int(msg["load"]))
            if propagate:
                self.broadcast(msg, except_sock=except_sock)

    def _upstream_reader(self, sock: socket.socket) -> None:
        """Citeste notificari de la serverul parinte."""
        try:
            while is_running:
                msg = recv_message(sock)
                if msg is None:
                    break
                self._apply_cluster_message(msg, propagate=True, except_sock=sock)
        finally:
            print("[INFO] upstream connection closed")
            with self.upstream_lock:
                if self.upstream_sock is sock:
                    self.upstream_sock = None
            try:
                sock.close()
            except OSError:
                pass

    # ---------- server accept loop (ca accept_loop de la curs) ----------

    def accept_loop(self, server: socket.socket) -> None:
        while is_running:
            try:
                client, addr = server.accept()
            except OSError:
                break
            print(f"[CONNECT] {addr} connected")
            t = threading.Thread(target=self.handle_connection, args=(client, addr), daemon=True)
            t.start()

    def run_auto_demo(self) -> None:
        """Scenariu scurt pentru video: doua executii, a doua pe nod incarcat."""
        time.sleep(3)
        print("[DEMO] exec 1 - 4 threads on best server")
        self.submit_exec("Calculator", "square", [1, 2, 3, 4], 4)
        time.sleep(2)
        # Simulam nod curent supraincarcat -> a doua cerere merge pe alt server.
        self.state.set_load(50)
        self.propagate_load()
        print("[DEMO] exec 2 - 2 threads (load balancing demo, self load=50)")
        self.submit_exec("Calculator", "square", [5, 6], 2)

    def keep_alive(self) -> None:
        """Mentine procesul activ (Docker / nod radacina fara consola)."""
        try:
            while is_running:
                time.sleep(3600)
        except KeyboardInterrupt:
            pass

    def run_interactive(self) -> None:
        """
        Consola: exec <Class> <method> <arg1,arg2> <threads>
        Exemplu: exec Calculator square 1,2,3,4 4
        """
        print("[INFO] commands: exec <Class> <method> <args> <threads> | status | quit")
        while is_running:
            try:
                line = input("node> ").strip()
            except EOFError:
                break
            if not line:
                continue
            if line == "quit":
                break
            if line == "status":
                for nid, info in self.state.snapshot().items():
                    print(f"  {nid} load={info['load']} @ {info['host']}:{info['port']}")
                continue
            parts = line.split()
            if parts[0] == "exec" and len(parts) >= 5:
                cls, method, args_raw, tc = parts[1], parts[2], parts[3], int(parts[4])
                args_list = []
                for a in args_raw.split(","):
                    a = a.strip()
                    if not a:
                        continue
                    args_list.append(int(a) if a.isdigit() else a)
                self.submit_exec(cls, method, args_list, tc)
            else:
                print("[WARN] unknown command")

    def main(self) -> None:
        server = None
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((NODE_HOST, NODE_PORT))
            server.listen(50)
            print(f"[START] hybrid node {self.node_id} on {NODE_HOST}:{NODE_PORT}")

            threading.Thread(target=self.accept_loop, args=(server,), daemon=True).start()
            self.connect_upstream()

            if AUTO_DEMO:
                threading.Thread(target=self.run_auto_demo, daemon=True).start()
                self.keep_alive()
            elif sys.stdin.isatty():
                self.run_interactive()
            else:
                # Container fara TTY: nu citim de la stdin.
                self.keep_alive()
        except BaseException as err:
            print(f"[ERROR] {err}")
        finally:
            global is_running
            is_running = False
            if server:
                server.close()
            print("[STOP] node closed")


if __name__ == "__main__":
    HybridNode().main()

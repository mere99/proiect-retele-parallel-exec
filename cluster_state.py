"""
Stare partajata a unui nod in cluster (pattern State + Lock de la curs).
"""

import threading


class ClusterState:
    """
    Tine evidenta:
    - nodurilor cunoscute (node_id -> host, port, load)
    - gradului local de incarcare (numar de fire active)
    """

    def __init__(self, node_id: str, host: str, listen_port: int):
        self.node_id = node_id
        self.host = host
        self.listen_port = listen_port
        self.peers = {}  # node_id -> {"host", "port", "load"}
        self.load = 0
        self.lock = threading.Lock()

    def set_load(self, value: int) -> int:
        with self.lock:
            self.load = max(0, value)
            return self.load

    def add_load(self, delta: int) -> int:
        with self.lock:
            self.load = max(0, self.load + delta)
            return self.load

    def upsert_peer(self, node_id: str, host: str, port: int, load: int = 0) -> None:
        with self.lock:
            self.peers[node_id] = {"host": host, "port": int(port), "load": int(load)}

    def remove_peer(self, node_id: str) -> None:
        with self.lock:
            self.peers.pop(node_id, None)

    def update_peer_load(self, node_id: str, load: int) -> None:
        with self.lock:
            if node_id in self.peers:
                self.peers[node_id]["load"] = int(load)

    def snapshot(self) -> dict:
        """Copie pentru selectia serverului cu incarcare minima."""
        with self.lock:
            all_nodes = {
                self.node_id: {
                    "host": self.host,
                    "port": self.listen_port,
                    "load": self.load,
                }
            }
            for pid, info in self.peers.items():
                all_nodes[pid] = dict(info)
            return all_nodes

    def pick_min_load_server(self) -> tuple[str, str, int]:
        """
        Criteriu documentat: alegem nodul cu load minim.
        La egalitate: preferam nodul local (self) pentru latenta mica.
        """
        nodes = self.snapshot()
        ranked = sorted(
            nodes.items(),
            key=lambda item: (item[1]["load"], 1 if item[0] == self.node_id else 0),
        )
        best_id, info = ranked[0]
        return best_id, info["host"], info["port"]

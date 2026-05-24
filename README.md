# Proiect 19 – Executie paralela la distanta

Sistem distribuit in care fiecare nod este **hibrid** (server + client), formand un cluster. Implementare in **Python** cu socket-uri TCP, aliniata stilului de la curs (`s4` text protocol framing, `c3` TCP multiclient, `State` + `Lock`).

## Cerinte acoperite

| Cerinta | Implementare |
|--------|----------------|
| Conectare cluster + propagare noduri | `register`, `peer_join`, `peer_leave`, broadcast |
| Grad de incarcare | `load` = fire active; `load_update` propagat |
| Executie paralela reala | `threading.Thread` per fir |
| Server cu load minim | `ClusterState.pick_min_load_server()` |
| Transfer clasa | mesaj `class_data` + `importlib` |
| Rezultate partiale | mesaje `exec_result` per fir |
| Deconectare | inchidere socket + `peer_leave` |
| Docker | `docker compose up --build` |

## Pornire rapida (Docker)

```bash
cd retele-proiect19
docker compose up --build
```

- **node1** – radacina cluster (port 9001)
- **node2**, **node3** – se conecteaza la `node1:9001`
- **node3** ruleaza demo automat (`AUTO_DEMO=1`): doua executii paralele

Loguri utile: `[CLUSTER]`, `[LOAD]`, `[EXEC]`, `[RESULT]`.

## Pornire locala (3 terminale)

```bash
# Terminal 1 – radacina
set NODE_NAME=node1&& set NODE_PORT=9001&& set BOOTSTRAP=&& python node.py

# Terminal 2
set NODE_NAME=node2&& set NODE_PORT=9002&& set BOOTSTRAP=127.0.0.1:9001&& python node.py

# Terminal 3 – client care cere executii
set NODE_NAME=node3&& set NODE_PORT=9003&& set BOOTSTRAP=127.0.0.1:9001&& python node.py
```

In consola node3:

```text
node> status
node> exec Calculator square 1,2,3,4 4
```

## Protocol (mesaje JSON cu framing de la curs)

Format pe fir TCP:

```text
<TOTAL_LENGTH> <JSON_PAYLOAD>
```

Tipuri principale:

- `register` – client anunta `node_id`, `listen_port`
- `peer_join` / `peer_leave` – propagare membri cluster
- `load_update` – actualizare incarcare
- `class_data` – transfer sursa clasa Python
- `exec_request` – cerere executie (`class_name`, `method_name`, `args`, `thread_count`)
- `exec_result` – rezultat partial per fir
- `exec_done` – toate firele s-au terminat

## Criteriu selectie server

Nodul cu **load minim** din vederea locala a clusterului; la egalitate se prefera **nodul curent**.

## Structura cod

```text
retele-proiect19/
  protocol.py       # framing + send/recv
  cluster_state.py  # peers + load (Lock)
  executor.py       # incarcare dinamica + thread-uri
  node.py           # nod hibrid principal
  tasks/Calculator.py
  docker-compose.yml
  Dockerfile
  README.md
```

## Prezentare live (seminar)

1. `docker compose up --build` – 3 noduri
2. Loguri `peer join` – propagare in cluster
3. `status` / loguri `[LOAD]` – balansare
4. `exec Calculator square 1,2,3,4 4` – executie paralela, rezultate pe fir
5. (optional) transfer clasa: sterge `loaded_classes/` pe un nod, ruleaza din nou `exec`
6. (optional) `docker stop p19_node2` – deconectare, `peer leave` in loguri

## Dependinte

Doar biblioteca standard Python 3.10+.

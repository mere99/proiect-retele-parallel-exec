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

## Cum pornim proiectul

### Ce ai nevoie

- **Docker Desktop** (pornit) – pentru varianta recomandata la evaluare
- sau **Python 3.10+** – pentru rulare locala in 3 terminale
- Git (optional, pentru clone)

### Varianta 1 – Docker (recomandat)

Din folderul proiectului (radacina repo-ului, acolo unde este `docker-compose.yml`):

```bash
cd retele-proiect19
docker compose up --build
```

Ce se intampla:

1. Se construiesc 3 containere: **node1**, **node2**, **node3**
2. **node1** (port 9001) – radacina cluster, fara upstream
3. **node2** (9002) si **node3** (9003) – se conecteaza la `node1:9001`
4. **node3** ruleaza automat un demo (`AUTO_DEMO=1`): doua executii paralele

In acelasi terminal vezi logurile tuturor nodurilor. Cauta:

- `[CLUSTER] peer join` – noduri in cluster
- `[EXEC]` – server ales pentru executie
- `[RESULT]` – rezultate pe fir
- `[LOAD]` – grad de incarcare

**Loguri doar pentru un nod** (alt terminal):

```bash
docker logs -f rp19_node1
docker logs -f rp19_node3
```

**Oprire cluster:**

```bash
docker compose down
```

### Rulezi si alte proiecte Docker in paralel?

Da. Proiectul **fresh** / toxwatch (porturi 27017, 8081, etc.) **nu intra in conflict** cu acest proiect (porturi **9001–9003**).

Conflictul apare doar daca exista containere vechi cu acelasi **nume fix** (`p19_node1` de la o rulare anterioara). Containerele acestui repo se numesc **`rp19_node1`**, `rp19_node2`, `rp19_node3`.

Daca tot vezi eroare „container name already in use”:

```bash
docker rm -f p19_node1 p19_node2 p19_node3
docker compose up --build
```

In Docker Desktop: cauta `p19_` sau `rp19_` in lista de containere.

**Demo manual** (node3 cu consola interactiva) – opreste compose, apoi in `docker-compose.yml` seteaza pentru `node3`:

`AUTO_DEMO: "0"`, rebuild, si:

```bash
docker compose run --rm -it node3 python node.py
```

Comenzi in consola: `status`, `exec Calculator square 1,2,3,4 4`, `quit`.

---

### Varianta 2 – Local, 3 terminale (fara Docker)

Cloneaza / deschide proiectul, apoi deschide **3 terminale** in acelasi folder.

**Windows CMD:**

```bat
REM Terminal 1 – radacina cluster
set NODE_NAME=node1
set NODE_PORT=9001
set BOOTSTRAP=
python node.py

REM Terminal 2
set NODE_NAME=node2
set NODE_PORT=9002
set BOOTSTRAP=127.0.0.1:9001
python node.py

REM Terminal 3 – cereri de executie
set NODE_NAME=node3
set NODE_PORT=9003
set BOOTSTRAP=127.0.0.1:9001
python node.py
```

**Git Bash / Linux:**

```bash
# Terminal 1
export NODE_NAME=node1 NODE_PORT=9001 BOOTSTRAP=
python node.py

# Terminal 2
export NODE_NAME=node2 NODE_PORT=9002 BOOTSTRAP=127.0.0.1:9001
python node.py

# Terminal 3
export NODE_NAME=node3 NODE_PORT=9003 BOOTSTRAP=127.0.0.1:9001
python node.py
```

In **terminalul 3** (prompt `node>`):

```text
node> status
node> exec Calculator square 1,2,3,4 4
node> quit
```

Format comanda: `exec <Clasa> <metoda> <arg1,arg2,...> <numar_fire>`

Exemplu: `exec Calculator square 5,6,7 3` – metoda `square` pe clasa `Calculator`, 3 fire.

**Oprire:** `quit` in fiecare terminal sau Ctrl+C.

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
6. (optional) `docker stop rp19_node2` – deconectare, `peer leave` in loguri

## Dependinte

Doar biblioteca standard Python 3.10+.

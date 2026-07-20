# TOB · Góndola + P1 (en la web, corriendo de verdad)

El **motor** de TOB, capa 1, al aire. Sin spikes, sin datos a mano: la ingesta
baja los RSS del coro en un server, arma el índice real de las últimas 72 h y
P1 cose la costura entre medios cada 6 h.

## Piezas

- `ingesta_gondola.py` — baja RSS · filtra 72 h · indexa (SQLite) · escribe `gondola.json` (góndola + grietas candidatas de P1).
- `.github/workflows/gondola.yml` — cron **cada 6 h** (la cadencia de P1). Corre la ingesta y commitea `gondola.json`. Botón **Run workflow** para correr a mano.
- `index.html` — el **Administrador general**. Lee `gondola.json`: columna izquierda = la góndola corriendo, columna derecha = P1 corriendo.
- `gondola.json` — lo genera el workflow. No se edita a mano.

## Montaje (una vez)

1. Repo en GitHub con estos archivos en `main`.
2. **Settings → Pages** → Source: *Deploy from a branch* → `main` / `/ (root)`. Queda la URL pública del admin.
3. **Actions** → workflow *TOB · Góndola + P1* → **Run workflow** para la primera corrida (no esperar 6 h).
4. Listo: el admin en la URL de Pages muestra la góndola y P1 con dato real, y se refresca solo cada 6 h.

## Correr local (opcional)

```bash
pip install -r requirements.txt
python3 ingesta_gondola.py     # escribe gondola.json + gondola.sqlite
python3 -m http.server 8080    # abrir http://localhost:8080
```

## Crecer

- Sumar/quitar medios: editar `COROS` / `COROS_TEMA` en `ingesta_gondola.py`.
- Cambiar la cadencia: `cron` en el workflow (`0 */6 * * *` → cada 6 h).
- Acá cuelga el resto de TOB: Medios, Programas, Motor, Bandeja — el admin ya tiene los elementos reservados.

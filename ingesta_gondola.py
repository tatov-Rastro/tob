#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TOB · Ingesta de la Góndola  (el motor, capa 1)  +  P1 (la costura)
-------------------------------------------------------------------
Baja las notas de los medios del coro por RSS, las filtra a las últimas 72 h,
las indexa (fuente · fecha · título · url · entidad) y agrupa la MISMA noticia
entre medios (clusters) — la base de la costura de P1.

Corre en GitHub Actions (cron cada 6 h). Sin IA, barato: solo HTTP + parseo.
El valor es el ÍNDICE/ORDEN, no guardar el texto.

Salida:
  · gondola.sqlite  — el índice (para consultar histórico)
  · gondola.json    — lo que lee el Administrador general (góndola + P1 candidatas)

Requisitos:  pip install feedparser
Uso:         python3 ingesta_gondola.py
"""

import feedparser, sqlite3, hashlib, re, json, os, sys
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict
from itertools import combinations
from difflib import SequenceMatcher

VENTANA_HORAS = 72          # <-- la ventana de la góndola
DB   = "gondola.sqlite"
OUT  = "gondola.json"

# DOS CADENCIAS:
#   modo "feed" → amplía la góndola seguido (refresca notas, conserva la última P1)
#   modo "p1"   → corre la costura/detectores en los horarios fijos (08/13/18)
MODO = (sys.argv[1] if len(sys.argv) > 1 else "p1").lower()

# ── PASO CREATIVO: un LLM escribe el titular que orienta (el gancho), no la plantilla.
#    Se activa SOLO si existe ANTHROPIC_API_KEY. Si no, cae en la plantilla del admin.
try:
    import anthropic
except Exception:
    anthropic = None
MODELO = os.environ.get("TOB_MODEL", "claude-3-5-haiku-latest")
_llm = None
def _cliente():
    global _llm
    if _llm is None and anthropic and os.environ.get("ANTHROPIC_API_KEY"):
        _llm = anthropic.Anthropic()
    return _llm

def titular_creativo(versiones):
    cli = _cliente()
    if not cli:
        return None
    titulos = "\n".join("- " + v["titulo"] for v in versiones[:6])
    prompt = ("Varios medios cubren la MISMA noticia. Estos son sus títulos:\n" + titulos +
        "\n\nEscribí UN solo titular en español rioplatense, breve (máximo 12 palabras), creativo y "
        "periodístico, que tire el gancho de la historia y oriente hacia dónde puede ir. "
        "NO menciones que los medios coinciden o difieren; ese no es el tema. "
        "Devolvé solo el titular, sin comillas ni explicación.")
    try:
        m = cli.messages.create(model=MODELO, max_tokens=40,
            messages=[{"role": "user", "content": prompt}])
        return m.content[0].text.strip().strip('"').strip()
    except Exception as ex:
        print(f"  ⚠ titular LLM: {ex.__class__.__name__} — cae en plantilla")
        return None

# ─────────────────────────────────────────────────────────────
# EL CORO — generalistas por país + especialistas por tema.
# (RSS de referencia; ajustá la URL si un medio cambia su feed.)
# ─────────────────────────────────────────────────────────────
COROS = {
 "AR": {
   "La Nación":     "https://www.lanacion.com.ar/arc/outboundfeeds/rss/?outputType=xml",
   "Clarín":        "https://www.clarin.com/rss/lo-ultimo/",
   "Infobae":       "https://www.infobae.com/arc/outboundfeeds/rss/?outputType=xml",
   "Página/12":     "https://www.pagina12.com.ar/rss/portada",
   "Ámbito":        "https://www.ambito.com/rss/pages/home.xml",
   "El Cronista":   "https://www.cronista.com/files/rss/economia.xml",
   "Perfil":        "https://www.perfil.com/feed",
   "El Destape":    "https://www.eldestapeweb.com/rss/home.xml",
   "TN":            "https://tn.com.ar/feed/",
   "Letra P":       "https://www.letrap.com.ar/arc/outboundfeeds/rss/?outputType=xml",
   "Cenital":       "https://cenital.com/feed/",
   "elDiarioAR":    "https://www.eldiarioar.com/rss/",
   "MDZ":           "https://www.mdzol.com/rss/home.xml",
   "La Voz":        "https://www.lavoz.com.ar/arc/outboundfeeds/rss/?outputType=xml",
   "Rosario3":      "https://www.rosario3.com/arc/outboundfeeds/rss/?outputType=xml",
   "Tiempo Arg.":   "https://www.tiempoar.com.ar/feed/",
   "Crónica":       "https://www.cronica.com.ar/feed",
   "Chequeado":     "https://chequeado.com/feed/",
 },
 "UY": {
   "El Observador":     "https://www.elobservador.com.uy/rss/pages/home.xml",
   "El País (UY)":      "https://www.elpais.com.uy/rss/",
   "La Diaria":         "https://ladiaria.com.uy/feeds/articulos/",
   "Montevideo Portal": "https://www.montevideo.com.uy/anxml.aspx?59",
   "LARED21":           "https://www.lr21.com.uy/feed",
   "Subrayado":         "https://www.subrayado.com.uy/rss.xml",
   "Caras y Caretas":   "https://www.carasycaretas.com.uy/feed/",
   "Semanario Voces":   "https://www.voces.com.uy/feed/",
   "Brecha":            "https://brecha.com.uy/feed/",
   "La Mañana":         "https://www.lamanana.com.uy/feed/",
 },
 "CL": {
   "La Tercera":    "https://www.latercera.com/arc/outboundfeeds/rss/?outputType=xml",
   "Emol":          "https://www.emol.com/rss/rss.asp?canal=nacional",
   "BioBioChile":   "https://www.biobiochile.cl/rss/",
   "El Mostrador":  "https://www.elmostrador.cl/feed/",
   "CIPER":         "https://www.ciperchile.cl/feed/",
   "The Clinic":    "https://www.theclinic.cl/feed/",
   "El Desconcierto":"https://www.eldesconcierto.cl/feed/",
   "CNN Chile":     "https://www.cnnchile.com/feed/",
   "Cooperativa":   "https://www.cooperativa.cl/noticias/site/tax/port/all/rss_3___1.xml",
   "El Ciudadano":  "https://www.elciudadano.com/feed/",
 },
}
COROS_TEMA = {
 "Economía":   {"iProfesional":"https://www.iprofesional.com/rss","El Economista":"https://eleconomista.com.ar/feed/",
                "BAE Negocios":"https://www.baenegocios.com/rss/ultimas-noticias.xml","Diario Financiero":"https://www.df.cl/rss"},
 "Deportes":   {"Olé":"https://www.ole.com.ar/rss/ultimas-noticias/","TyC Sports":"https://www.tycsports.com/rss/",
                "Doble Amarilla":"https://www.dobleamarilla.com.ar/rss","AS Chile":"https://chile.as.com/rss/futbol/primera.xml"},
 "Espectáculos":{"Ciudad Magazine":"https://www.ciudad.com.ar/rss","Primicias Ya":"https://www.primiciasya.com/rss/home.xml",
                "Rating Cero":"https://www.ratingcero.com/rss"},
}

# ─────────────────────────────────────────────────────────────
# SEGUNDA NATURALEZA DEL FEED: fuentes oficiales / instituciones.
# El poder que actualiza información con documentos. Insumo de P2 (dato duro)
# y de los detectores fuente=poder y silencio. (RSS reales donde existen;
# el gobierno de la región casi no expone RSS — esos caen en cero honesto.)
# ─────────────────────────────────────────────────────────────
OFICIALES = {
 "FMI":            "https://www.imf.org/en/news/rss",
 "ONU Noticias":   "https://news.un.org/es/feed/subscribe/es/news/all/rss.xml",
 "Banco Mundial":  "https://www.worldbank.org/en/news/all?format=rss",
 "Argentina.gob":  "https://www.argentina.gob.ar/rss/noticias",
 "Presidencia UY": "https://www.gub.uy/presidencia/comunicacion/noticias/rss.xml",
 "Gobierno Chile": "https://www.gob.cl/noticias/rss/",
}

STOP = set(("de la el en y a los las un una que con por para del al su se es más como o e lo son fue ser han hay tras sin sobre entre este esta "
            "según mientras cuando donde quien como cual todo cada otro otra hoy ayer tras ante bajo desde hasta "
            "confirman denuncian aseguran afirman revelan anuncian buscan piden video fotos urgente mirá última nuevo nueva").split())

def entidades(titulo):
    toks = re.findall(r"[A-Za-zÁÉÍÓÚÑáéíóúñ0-9]{4,}", titulo.lower())
    return [t for t in toks if t not in STOP]

# PROPIOS: nombres propios del título (palabras con Mayúscula inicial, salvo la primera).
# Son los más discriminantes para saber que dos medios hablan de LA MISMA noticia.
def propios(titulo):
    palabras = re.findall(r"[0-9A-Za-zÁÉÍÓÚÑÜáéíóúñü]{4,}", titulo)
    out = set()
    for w in palabras:                                # incluye la 1ª: los propios suelen liderar el titular
        if (w[0].isupper() or w[0].isdigit()) and w.lower() not in STOP:
            out.add(w.lower())
    return out

def cluster_key(titulo):
    ents = sorted(propios(titulo)) or sorted(set(entidades(titulo)))
    return " ".join(ents[:6])

def parse_fecha(e):
    for k in ("published_parsed","updated_parsed"):
        v = e.get(k)
        if v: return datetime(*v[:6], tzinfo=timezone.utc)
    return None

def init_db():
    c = sqlite3.connect(DB)
    c.execute("""CREATE TABLE IF NOT EXISTS notas(
        id TEXT PRIMARY KEY, medio TEXT, pais TEXT, tema TEXT,
        titulo TEXT, url TEXT, fecha TEXT, ckey TEXT, ingesta TEXT)""")
    c.commit(); return c

def bajar(medio, url):
    try:
        f = feedparser.parse(url, request_headers={"User-Agent":"TOB-gondola/1.0"})
        return f.entries or []
    except Exception as ex:
        print(f"  ⚠ {medio}: bloqueado/no-RSS ({ex.__class__.__name__}) — cero honesto, se declara")
        return []

def correr():
    c = init_db()
    corte = datetime.now(timezone.utc) - timedelta(hours=VENTANA_HORAS)
    ahora = datetime.now(timezone.utc).isoformat()
    fuentes = [(p, m, u, None) for p, d in COROS.items() for m, u in d.items()]
    fuentes += [(None, m, u, tema) for tema, d in COROS_TEMA.items() for m, u in d.items()]

    nuevas = 0; vistos = 0; por_medio = {}
    for pais, medio, url, tema in fuentes:
        for e in bajar(medio, url):
            titulo = (e.get("title") or "").strip()
            link   = (e.get("link")  or "").strip()
            if not titulo or not link: continue
            fecha = parse_fecha(e)
            if fecha and fecha < corte:      # <-- filtro 72 h
                continue
            vistos += 1
            nid = hashlib.sha1(link.encode()).hexdigest()[:16]   # dedup por url
            ck  = cluster_key(titulo)
            c.execute("INSERT OR IGNORE INTO notas VALUES(?,?,?,?,?,?,?,?,?)",
                (nid, medio, pais or "-", tema or "general", titulo, link,
                 fecha.isoformat() if fecha else "", ck, ahora))
            if c.total_changes: nuevas += 1
            por_medio[medio] = por_medio.get(medio, 0) + 1
    c.commit()

    # ── purga: fuera de ventana ──
    c.execute("DELETE FROM notas WHERE fecha!='' AND fecha < ?", (corte.isoformat(),))
    c.commit()

    # ── LA GÓNDOLA: notas de la ventana ──
    notas = [dict(medio=r[0], pais=r[1], tema=r[2], titulo=r[3], url=r[4], fecha=r[5])
             for r in c.execute(
               "SELECT medio,pais,tema,titulo,url,fecha,ckey FROM notas ORDER BY fecha DESC")]
    total = c.execute("SELECT COUNT(*) FROM notas").fetchone()[0]

    # ── MODO FEED: solo ampliar la góndola. Conserva la última corrida de P1. ──
    if MODO == "feed":
        prev = {}
        try:
            with open(OUT, encoding="utf-8") as f: prev = json.load(f)
        except Exception: pass
        prev["feed_generado"] = ahora
        prev["ventana_horas"] = VENTANA_HORAS
        prev["notas"] = notas
        tot = dict(prev.get("totales", {}))
        tot.update(notas=total, medios=len(por_medio), vistas=vistos, nuevas=nuevas)
        prev["totales"] = tot
        prev.setdefault("p1_generado", None)
        prev.setdefault("clusters", []); prev.setdefault("primicias", [])
        prev.setdefault("silencios", [])
        prev.setdefault("detectores", dict(activos=["calco","divergencia","coro","timing","primicia"],
                                           pendientes=["fuente=poder","silencio"]))
        with open(OUT, "w", encoding="utf-8") as f:
            json.dump(prev, f, ensure_ascii=False, indent=1)
        print(f"TOB · FEED {ahora[:16]} — góndola: {total} notas · {len(por_medio)} medios "
              f"(P1 conservada de {str(prev.get('p1_generado'))[:16]})")
        c.close(); return

    # ── P1: la costura — misma noticia en >=2 medios, por nombres propios compartidos ──
    N = len(notas)
    props = [propios(n["titulo"]) for n in notas]
    df = Counter(tok for s in props for tok in s)
    inv = defaultdict(list)
    for i, s in enumerate(props):
        for tok in s:
            inv[tok].append(i)

    parent = list(range(N))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]; x = parent[x]
        return x
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb: parent[max(ra, rb)] = min(ra, rb)

    # pares candidatos: comparten propios discriminantes (ignoramos tokens ultra-comunes = temas, no historias)
    shared = Counter()
    for tok, idxs in inv.items():
        if len(idxs) < 2 or len(idxs) > 40: continue
        for a, b in combinations(sorted(set(idxs)), 2): shared[(a, b)] += 1
    def parecido(a, b):
        return SequenceMatcher(None, notas[a]["titulo"].lower(), notas[b]["titulo"].lower()).ratio()
    # cose SÓLO si además los títulos se parecen: misma noticia, no mismo tema
    for (a, b), sh in shared.items():
        s = parecido(a, b)
        if (sh >= 2 and s >= 0.34) or (sh >= 1 and s >= 0.50):
            union(a, b)

    grupos = defaultdict(list)
    for i in range(N): grupos[find(i)].append(i)

    def spread_min(vers):                        # ventana entre la 1ª y la última publicación
        fs = []
        for v in vers:
            try: fs.append(datetime.fromisoformat(v["fecha"]))
            except Exception: pass
        if len(fs) < 2: return None
        fs.sort()
        return int((fs[-1] - fs[0]).total_seconds() // 60)

    clusters = []; en_cluster = set()
    for _, idxs in grupos.items():
        medios = {notas[i]["medio"] for i in idxs}
        if len(medios) < 2: continue            # grieta = costura entre >=2 medios
        en_cluster.update(idxs)
        vers = [notas[i] for i in idxs]
        tits = [v["titulo"] for v in vers]
        sim = 0.0; pares = 0                     # calco vs divergencia = parecido entre títulos
        for a, b in combinations(tits[:8], 2):
            sim += SequenceMatcher(None, a.lower(), b.lower()).ratio(); pares += 1
        senal = "calco" if (pares and sim/pares >= 0.55) else "divergencia"
        pc = Counter(tok for i in idxs for tok in props[i])
        ckey = " ".join(t for t, _ in pc.most_common(5)) or "—"
        vmin = spread_min(vers)                   # TIMING: publicaron casi juntos = coordinación
        timing = bool(vmin is not None and len(medios) >= 3 and vmin <= 45)
        clusters.append(dict(
            ckey=ckey, n_medios=len(medios), senal=senal,
            paises=sorted({v["pais"] for v in vers if v["pais"] and v["pais"] != '-'}),
            ventana_min=vmin, timing=timing,
            versiones=[dict(medio=v["medio"], titulo=v["titulo"], url=v["url"]) for v in vers],
        ))
    clusters.sort(key=lambda c: (c["timing"], c["n_medios"]), reverse=True)

    # ── el gancho: el LLM escribe el titular de cada grieta (si hay key; si no, None → plantilla) ──
    for cl in clusters:
        cl["titular"] = titular_creativo(cl["versiones"])

    # ── PRIMICIA SOLITARIA: un medio solo, con nombres propios que ningún otro menciona ──
    primicias = []
    for i in range(N):
        if i in en_cluster: continue
        tit = notas[i]["titulo"]
        unicos = [t for t in props[i] if df[t] == 1 and len(t) >= 5]
        # primicia = historia PROPIA y focalizada: pocos nombres específicos, no un refrito de agencia
        if not (2 <= len(unicos) <= 4): continue
        if tit.count(',') >= 2 or tit.lower().count(' y ') >= 2: continue
        primicias.append(dict(
            medio=notas[i]["medio"], pais=notas[i]["pais"],
            titulo=tit, url=notas[i]["url"], fecha=notas[i]["fecha"], unicos=len(unicos)))
    primicias.sort(key=lambda p: p["fecha"], reverse=True)   # la más nueva primero
    primicias = primicias[:10]

    # ── SEGUNDA NATURALEZA: fuentes oficiales / instituciones (insumo de P2, dato duro) ──
    oficial_items = []; of_por_fuente = {}
    for fuente, url in OFICIALES.items():
        for e in bajar(fuente, url):
            titulo = (e.get("title") or "").strip(); link = (e.get("link") or "").strip()
            if not titulo or not link: continue
            fecha = parse_fecha(e)
            if fecha and fecha < corte: continue
            oficial_items.append(dict(fuente=fuente, titulo=titulo, url=link,
                fecha=fecha.isoformat() if fecha else "", props=propios(titulo)))
            of_por_fuente[fuente] = of_por_fuente.get(fuente, 0) + 1

    # FUENTE=PODER: la grieta cuyos propios matchean un item oficial → los medios replican al poder
    for cl in clusters:
        ckprops = set()
        for v in cl["versiones"]: ckprops |= propios(v["titulo"])
        cl["fuente_poder"] = None
        for o in oficial_items:
            if len(ckprops & o["props"]) >= 2:
                cl["fuente_poder"] = o["fuente"]; break

    # SILENCIO: lo que el poder dijo y NINGÚN medio tocó — el perro que no ladró
    props_medios = set()
    for s in props: props_medios |= s
    silencios = []
    for o in oficial_items:
        fuertes = [t for t in o["props"] if len(t) >= 5]
        if fuertes and not (o["props"] & props_medios):
            silencios.append(dict(fuente=o["fuente"], titulo=o["titulo"], url=o["url"], fecha=o["fecha"]))
    silencios.sort(key=lambda x: x["fecha"], reverse=True)
    silencios = silencios[:10]

    # detectores: fuente=poder y silencio se activan SOLO si respondieron fuentes oficiales
    activos = ["calco", "divergencia", "coro", "timing", "primicia"]
    pendientes = []
    (activos.extend if oficial_items else pendientes.extend)(["fuente=poder", "silencio"])

    n_poder = sum(1 for cl in clusters if cl.get("fuente_poder"))
    payload = dict(
        p1_generado=ahora, feed_generado=ahora, ventana_horas=VENTANA_HORAS,
        totales=dict(notas=total, medios=len(por_medio), clusters=len(clusters),
                     primicias=len(primicias), oficiales=len(of_por_fuente),
                     fuente_poder=n_poder, silencios=len(silencios),
                     vistas=vistos, nuevas=nuevas),
        detectores=dict(activos=activos, pendientes=pendientes),
        notas=notas, clusters=clusters, primicias=primicias, silencios=silencios,
    )
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)

    # ── consola ──
    print("\n" + "="*60)
    print(f"TOB · Góndola — ingesta {ahora[:16]}  (ventana {VENTANA_HORAS} h)")
    print("="*60)
    print(f"  notas en el índice : {total}")
    print(f"  vistas esta corrida: {vistos}   ·  nuevas: {nuevas}")
    print(f"  fuentes definidas  : {len(fuentes)}   ·  respondieron: {len(por_medio)}   ·  cero honesto: {len(fuentes)-len(por_medio)}")
    print(f"  clusters (>=2 medios): {len(clusters)}   ← acá empieza P1 (la costura)")
    print(f"  primicias solitarias : {len(primicias)}   ·  con timing: {sum(1 for c in clusters if c['timing'])}")
    print(f"  oficiales activas    : {len(of_por_fuente)}/{len(OFICIALES)}   ·  fuente=poder: {n_poder}   ·  silencios: {len(silencios)}")
    for cl in clusters[:12]:
        t = " ⏱" if cl['timing'] else ""
        p = f" ⚖{cl['fuente_poder']}" if cl.get('fuente_poder') else ""
        print(f"    · {cl['n_medios']} medios [{cl['senal']}{t}]{p}  →  {cl['ckey'][:44]}")
    print("="*60)
    print(f"  escrito: {OUT}  ({os.path.getsize(OUT)//1024} KB)")
    c.close()

if __name__ == "__main__":
    correr()

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

import feedparser, sqlite3, hashlib, re, json, os
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict
from itertools import combinations
from difflib import SequenceMatcher

VENTANA_HORAS = 72          # <-- la ventana de la góndola
DB   = "gondola.sqlite"
OUT  = "gondola.json"

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
   "Perfil":        "https://www.perfil.com/rss/ultimomomento.xml",
   "El Destape":    "https://www.eldestapeweb.com/rss/home.xml",
   "TN":            "https://tn.com.ar/feed/",
 },
 "UY": {
   "El Observador":     "https://www.elobservador.com.uy/rss/pages/home.xml",
   "El País (UY)":      "https://www.elpais.com.uy/rss/",
   "La Diaria":         "https://ladiaria.com.uy/feeds/articulos/",
   "Montevideo Portal": "https://www.montevideo.com.uy/anxml.aspx?59",
   "LARED21":           "https://www.lr21.com.uy/feed",
 },
 "CL": {
   "La Tercera":    "https://www.latercera.com/arc/outboundfeeds/rss/?outputType=xml",
   "Emol":          "https://www.emol.com/rss/rss.asp?canal=nacional",
   "BioBioChile":   "https://www.biobiochile.cl/rss/",
   "El Mostrador":  "https://www.elmostrador.cl/feed/",
 },
}
COROS_TEMA = {
 "Economía":   {"iProfesional":"https://www.iprofesional.com/rss","El Economista":"https://eleconomista.com.ar/feed/"},
 "Deportes":   {"Olé":"https://www.ole.com.ar/rss/ultimas-noticias/","TyC Sports":"https://www.tycsports.com/rss/","Doble Amarilla":"https://www.dobleamarilla.com.ar/rss"},
 "Espectáculos":{"Ciudad Magazine":"https://www.ciudad.com.ar/rss","Primicias Ya":"https://www.primiciasya.com/rss/home.xml"},
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

    clusters = []
    for _, idxs in grupos.items():
        medios = {notas[i]["medio"] for i in idxs}
        if len(medios) < 2: continue            # grieta = costura entre >=2 medios
        vers = [notas[i] for i in idxs]
        tits = [v["titulo"] for v in vers]
        sim = 0.0; pares = 0                     # calco vs divergencia = parecido entre títulos
        for a, b in combinations(tits[:8], 2):
            sim += SequenceMatcher(None, a.lower(), b.lower()).ratio(); pares += 1
        senal = "calco" if (pares and sim/pares >= 0.55) else "divergencia"
        pc = Counter(tok for i in idxs for tok in props[i])
        ckey = " ".join(t for t, _ in pc.most_common(5)) or "—"
        clusters.append(dict(
            ckey=ckey, n_medios=len(medios), senal=senal,
            paises=sorted({v["pais"] for v in vers if v["pais"] and v["pais"] != '-'}),
            versiones=[dict(medio=v["medio"], titulo=v["titulo"], url=v["url"]) for v in vers],
        ))
    clusters.sort(key=lambda c: c["n_medios"], reverse=True)

    total = c.execute("SELECT COUNT(*) FROM notas").fetchone()[0]
    payload = dict(
        generado=ahora, ventana_horas=VENTANA_HORAS, cadencia_p1_horas=6,
        totales=dict(notas=total, medios=len(por_medio), clusters=len(clusters),
                     vistas=vistos, nuevas=nuevas),
        notas=notas, clusters=clusters,
    )
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)

    # ── consola ──
    print("\n" + "="*60)
    print(f"TOB · Góndola — ingesta {ahora[:16]}  (ventana {VENTANA_HORAS} h)")
    print("="*60)
    print(f"  notas en el índice : {total}")
    print(f"  vistas esta corrida: {vistos}   ·  nuevas: {nuevas}")
    print(f"  medios activos     : {len(por_medio)}")
    print(f"  clusters (>=2 medios): {len(clusters)}   ← acá empieza P1 (la costura)")
    for cl in clusters[:12]:
        print(f"    · {cl['n_medios']} medios [{cl['senal']}]  →  {cl['ckey'][:48]}")
    print("="*60)
    print(f"  escrito: {OUT}  ({os.path.getsize(OUT)//1024} KB)")
    c.close()

if __name__ == "__main__":
    correr()

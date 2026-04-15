# Pypeweb — Spécification technique

## Contexte

**Pyperun** est le moteur de pipeline IoT (headless). Il expose une API HTTP Flask (`api_server.py`) et une couche Python pure (`pyperun/core/api.py`).

**Pypeweb** est l'outil de pilotage de pyperun. C'est un projet séparé qui se connecte à pyperun **uniquement via son API HTTP** — jamais en important directement son code.

Il existera plusieurs variantes de pypeweb selon les besoins utilisateurs.

---

## Vision produit

| Fonction | Description |
|----------|-------------|
| 1. Visualiser les exécutions | Historique des runs, streaming live des étapes |
| 2. Surveiller le pipeline | État de chaque dataset/step, alertes en cas d'erreur |
| 3. Lancer des flows | Depuis l'interface, avec paramètres optionnels |
| 4. Créer des datasets | Formulaire équivalent à `pyperun init` |
| 5. Modifier des flows | Éditeur JSON dans le navigateur |
| 6. Ordonnancer des flows | Exécutions planifiées (cron) |
| 7. Générer un rapport | Documentation synthétique d'un flow : ce qu'il fait, étape par étape, avec ses paramètres effectifs |
| 8. Piloter via LLM | Interface agent conversationnel (Claude) via MCP server |

---

## Architecture globale

```
pyperun (moteur)
  ├── pyperun/core/api.py      ← fonctions Python pures
  ├── api_server.py            ← serveur Flask HTTP (port 5000)
  └── pyperun/core/mcp.py      ← MCP server (à venir — dans pyperun, pas pypeweb)

pypeweb-core                   ← client HTTP pyperun, templates de base, auth
pypeweb-monitor                ← lecture seule (status, runs, logs)
pypeweb-control                ← monitor + run/edit flows + datasets
pypeweb-agent                  ← control + chat LLM (Claude) via MCP
```

Chaque niveau de pypeweb dépend du précédent. Pas de duplication.

---

## API pyperun existante

L'API HTTP est déjà implémentée dans `api_server.py` :

```
GET    /api/flows                      Liste des flows
GET    /api/flows/<flow>/steps         Étapes d'un flow
GET    /api/treatments                 Liste des traitements
GET    /api/treatments/<name>          Détail d'un traitement
GET    /api/presets                    Presets disponibles
GET    /api/status                     État du pipeline (tous datasets)
POST   /api/datasets                   Créer un dataset (init)
DELETE /api/datasets/<dataset>         Supprimer un dataset
POST   /api/run/<flow>                 Lancer un flow (async → retourne run_id)
GET    /api/runs?limit=50              Historique des runs
GET    /api/runs/<run_id>              Événements d'un run (polling)
GET    /health                         Health check
```

Authentification optionnelle via header `Authorization: Bearer <PYPERUN_API_KEY>`.

---

## Stack technique

**Principes directeurs : KISS + HATEOAS**

- Pas de framework JS complexe (pas de React, Vue, etc.)
- Rendu HTML côté serveur
- Chaque page porte ses propres actions (liens, formulaires)

```
Flask + Jinja2    templates HTML côté serveur
HTMX              interactions dynamiques (formulaires, polling) sans écrire de JS
SSE               Server-Sent Events pour le streaming live des runs
Tailwind CSS      via CDN pour démarrer, puis build (npm) pour la prod
```

---

## Étape 1 : pypeweb-monitor (scope du stagiaire)

Démarrer par le niveau le plus simple — **lecture seule**. Aucune action d'écriture.

### Ce qu'il fait

- Visualiser l'état du pipeline (tous les datasets, tous les steps)
- Lister l'historique des runs
- Voir le détail d'un run en live (streaming des étapes)

### Structure de projet

```
pypeweb-monitor/
  app.py                  ← Flask app, toutes les routes
  pyperun_client.py       ← client HTTP vers l'API pyperun (requests)
  templates/
    base.html             ← layout commun (nav, head)
    status.html           ← état du pipeline
    runs.html             ← historique des runs
    run_detail.html       ← détail d'un run (live via SSE ou HTMX polling)
    flow_report.html      ← rapport synthétique d'un flow (config + params effectifs)
  static/
    style.css
  requirements.txt        ← flask, htmx (via CDN), requests
  README.md
```

### Routes

```
GET  /                          → redirect /status
GET  /status                    → état pipeline (tous datasets/steps)
GET  /runs                      → historique des runs (limit=50)
GET  /runs/<run_id>             → détail d'un run, streaming live des étapes
GET  /flows/<flow>/report       → rapport synthétique du flow (config + params effectifs)
GET  /flows/<flow>/report?fmt=json  → données brutes pour export/LLM
```

HATEOAS : chaque page porte ses liens. Depuis `/status`, lien vers les runs du flow. Depuis `/runs`, lien vers le détail.

### Streaming des runs

Utiliser **SSE (Server-Sent Events)** plutôt que polling brut :

```
GET /stream/runs/<run_id>   ← endpoint SSE Flask
```

HTMX côté client souscrit au stream et met à jour le DOM à chaque événement reçu, sans rechargement de page.

### pyperun_client.py

Wrapper minimal autour de `requests` :

```python
class PyperunClient:
    def __init__(self, base_url, api_key=None): ...
    def get_status(self) -> list[dict]: ...
    def list_runs(self, limit=50) -> list[dict]: ...
    def get_run_events(self, run_id) -> dict: ...
    def stream_run(self, run_id): ...  # générateur SSE
```

`base_url` et `api_key` depuis variables d'environnement (`PYPERUN_URL`, `PYPERUN_API_KEY`).

---

## Rapport de flow

### Objectif

Comprendre simplement **ce que fait un flow** — pas ce qu'il a exécuté, mais ce qu'il est configuré pour faire. Répondre à la question : *"j'ai lancé ce pipeline la semaine dernière, qu'est-ce qu'il fait exactement ?"*

Référence visuelle : `doc/AnalysysPremanipGrace.html` — document rédigé à la main pour le flow `premanip-grace`. Le rapport pypeweb génère automatiquement l'équivalent depuis la config réelle.

### Route

```
GET /flows/<flow_name>/report          → page HTML (rapport lisible)
GET /flows/<flow_name>/report?fmt=json → données brutes JSON (pour LLM ou export)
```

### Ce que contient le rapport

**En-tête du flow :**
- Nom, dataset associé, période (`from` / `to` si définis)
- Nombre d'étapes, état actuel (up-to-date / incomplete / error)
- Lien vers le dernier run

**Pour chaque étape (dans l'ordre du flow) :**
- Nom du treatment, répertoire input → output
- Paramètres **effectifs** (après fusion : defaults treatment.json + flow.params + step.params)
  - Mis en valeur : les params qui diffèrent des defaults (= ce qui a été customisé)
- Description fonctionnelle courte (template par type de treatment — voir ci-dessous)

**Résumé de l'état pipeline :**
- Tableau récapitulatif : étape / fichiers / dernière modif / statut
- Lien vers l'historique des runs

### Sources de données (API pyperun)

```
GET /api/flows/<flow>/steps    → liste des étapes avec leurs params merged
GET /api/treatments/<name>     → schéma du treatment (defaults, types, descriptions)
GET /api/status                → état actuel (fichiers, last mod, statut)
GET /api/runs?flow=<flow>      → dernier run du flow
```

Les paramètres effectifs = `treatment.json defaults` ← `flow.params` ← `step.params` (même hiérarchie que pyperun).

### Templates de description par treatment

Chaque treatment a une phrase-template qui se remplit avec les params effectifs :

| Treatment | Description générée |
|-----------|---------------------|
| `parse` | "Parse les fichiers bruts depuis `{input}`, produit des parquets typés par domaine et par jour." |
| `clean` | "Supprime les doublons, applique les bornes min/max, élimine les spikes (fenêtre {spike_window}s)." |
| `resample` | "Re-échantillonne à `{freq}`, comble les gaps ≤ `{max_gap_fill_s}`s par forward-fill." |
| `transform` | "Applique `{n}` transformation(s) : {liste des fonctions sur les colonnes ciblées}." |
| `normalize` | "Normalise le domaine `{domain}` par méthode `{method}` (percentiles `{pmin}`–`{pmax}`). fit={fit}." |
| `aggregate` | "Agrège sur `{n}` fenêtres temporelles ({windows}) avec métriques : {metrics}." |
| `to_postgres` | "Exporte vers PostgreSQL `{host}/{dbname}`, tables `{table_template}`, mode `{mode}`." |
| `exportcsv` | "Exporte le domaine `{domain}` (agrégation `{aggregation}`) en CSV, timezone `{tz}`." |
| `exportparquet` | "Exporte le domaine `{domain}` (agrégation `{aggregation}`) en Parquet." |

### Mise en évidence des customisations

Un param est **customisé** s'il diffère de la valeur default du `treatment.json`. L'interface les distingue visuellement (badge "custom" ou couleur différente) pour qu'on voie d'un coup d'œil ce qui a été adapté pour ce flow.

### V2 : rapport enrichi par LLM

En v2, l'endpoint `?fmt=json` permet d'alimenter un LLM (Claude) qui génère :
- Une description en prose du flow complet en langage naturel
- Un résumé des choix de configuration et de leur motivation
- Des suggestions si des paramètres semblent incohérents

Ce rapport LLM peut être déclenché depuis pypeweb-agent via un bouton "Expliquer ce flow".

### Placement dans l'architecture

Le rapport est **read-only** → il appartient à `pypeweb-monitor` dès la v1.
L'endpoint JSON (`?fmt=json`) est la base pour l'enrichissement LLM en v2.

---

## Ce qui vient après (v2+)

Une fois pypeweb-monitor livré et validé :

**pypeweb-control** ajoute :
- Lancer un flow (formulaire + paramètres optionnels)
- Créer un dataset
- Éditer un flow (éditeur JSON dans le navigateur)
- Ordonnancement (cron)
- Alertes (email/webhook sur erreur)

**pypeweb-agent** ajoute :
- Interface de chat avec Claude
- Claude pilote pyperun via MCP server
- Exemples : "ajoute une métrique au flow X", "lance le pipeline sur les 7 derniers jours"

**MCP server** (dans pyperun, pas pypeweb) :
- Expose `list_flows`, `run_flow`, `get_status`, `get_run_events`, `create_dataset`, `edit_flow_step_params` comme tools MCP
- Utilisable directement depuis Claude Code ou tout agent LLM

---

## MCP Server (implémenté)

Le MCP server est dans `pyperun/mcp.py`. Il expose pyperun comme un ensemble de tools pour Claude Code ou tout agent LLM compatible MCP.

### Installation

```bash
pip install -e ".[mcp]"
```

### Lancement

```bash
python -m pyperun.mcp          # stdio (défaut, pour Claude Code)
python -m pyperun.mcp --sse    # SSE sur le port 5001
```

### Configuration Claude Code

Ajouter dans `~/.claude/claude_desktop_config.json` ou `.mcp.json` à la racine du projet :

```json
{
    "mcpServers": {
        "pyperun": {
            "command": "python",
            "args": ["-m", "pyperun.mcp"],
            "cwd": "/chemin/vers/le/projet"
        }
    }
}
```

### Tools disponibles

**Lecture seule :**

| Tool | Description |
|------|-------------|
| `list_flows` | Liste tous les flows disponibles |
| `get_status` | État du pipeline pour tous les flows |
| `list_steps(flow_name)` | Étapes d'un flow avec leurs params |
| `describe_treatment(name)` | Schéma complet d'un treatment |
| `list_runs(limit)` | Historique des runs |
| `get_run_events(run_id)` | Événements détaillés d'un run |

**Écriture :**

| Tool | Description |
|------|-------------|
| `run_flow(name, ...)` | Lance un flow de façon **bloquante** — attend la fin |
| `init_dataset(dataset, preset)` | Scaffolde un nouveau dataset |

`delete_dataset` est intentionnellement absent (action destructive).

### Paramètres de run_flow

```
name            : Nom du flow (ex: "valvometry-daily")
time_from       : ISO 8601, ex: "2026-01-01T00:00:00Z"
time_to         : ISO 8601
last            : True = traitement incrémental (nouvelles données seulement)
from_step       : Démarrer depuis cette étape (inclusive)
to_step         : S'arrêter à cette étape (inclusive)
step            : Exécuter une seule étape
output_mode     : "append" | "replace" | "full-replace"
params_override : JSON string de surcharges, ex: '{"freq": "1s"}'
```

Retourne `{run_id, status, n_steps_done, error}`.

---

## Ce que le stagiaire doit livrer pour pypeweb-monitor

- [ ] Repo `pypeweb-monitor` avec structure ci-dessus
- [ ] `pyperun_client.py` fonctionnel (configuré via env vars)
- [ ] 4 pages : `/status`, `/runs`, `/runs/<run_id>`, `/flows/<flow>/report`
- [ ] Streaming live d'un run via SSE
- [ ] Rapport de flow : paramètres effectifs par étape, mise en valeur des customisations, templates de description
- [ ] `README.md` : installation, configuration, lancement
- [ ] Testé contre une instance pyperun réelle

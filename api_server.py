"""
Pyperun Flask API server.

Usage:
    pip install flask
    flask --app api_server run --host 0.0.0.0 --port 5000

    # ou en production (avec gunicorn) :
    pip install gunicorn
    gunicorn -w 1 -b 0.0.0.0:5000 api_server:app
    # NOTE : utiliser 1 seul worker pour éviter les runs parallèles non intentionnels

Endpoints:
    GET    /api/flows                          Liste des flows disponibles
    GET    /api/flows/<flow>/steps             Étapes d'un flow
    GET    /api/treatments                     Liste des traitements
    GET    /api/treatments/<name>              Détail d'un traitement
    GET    /api/presets                        Liste des presets disponibles
    GET    /api/status                         État du pipeline (tous les datasets)
    POST   /api/datasets                       Créer un nouveau dataset (init)
    DELETE /api/datasets/<dataset>             Supprimer un dataset
    POST   /api/run/<flow>                     Lance un flow en arrière-plan
    GET    /api/runs?limit=50                  Historique des runs
    GET    /api/runs/<run_id>                  Événements d'un run (polling)
"""

import os
from threading import Thread

from flask import Flask, jsonify, request, abort

from pyperun.core.api import (
    list_flows,
    list_steps,
    list_treatments,
    describe_treatment,
    list_presets,
    get_status,
    init_dataset,
    delete_dataset,
    list_runs,
    get_run_events,
)
from pyperun.core.flow import run_flow
from pyperun.core.logger import new_run_id

app = Flask(__name__)

# Clé secrète optionnelle : si définie, toutes les requêtes doivent porter
# le header  Authorization: Bearer <PYPERUN_API_KEY>
_API_KEY = os.environ.get("PYPERUN_API_KEY")

_REDACT_PARAMS = {"password"}


def _check_auth():
    if not _API_KEY:
        return
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {_API_KEY}":
        abort(401, "Unauthorized")


def _redact_params(params: dict) -> dict:
    """Masque les valeurs sensibles dans un dict de params."""
    return {k: ("***" if k in _REDACT_PARAMS else v) for k, v in params.items()}


@app.before_request
def before_request():
    _check_auth()


# ---------------------------------------------------------------------------
# Presets
# ---------------------------------------------------------------------------

@app.get("/api/presets")
def api_list_presets():
    """Liste des presets disponibles (built-in + presets.json)."""
    return jsonify(list_presets())


# ---------------------------------------------------------------------------
# Datasets — lifecycle
# ---------------------------------------------------------------------------

@app.post("/api/datasets")
def api_init_dataset():
    """
    Créer un nouveau dataset (équivalent de pyperun init).

    Body JSON :
    {
        "dataset":   "MY-EXPERIMENT",   ← obligatoire
        "preset":    "full",            ← défaut: "full"
        "flow_name": null,              ← défaut: dataset.lower()
        "raw":       null,              ← chemin vers les CSV existants (symlink)
        "force":     false              ← écraser un flow existant
    }

    Réponse 201 :
    {
        "dataset":      "MY-EXPERIMENT",
        "flow":         "my-experiment",
        "flow_path":    "flows/my-experiment.json",
        "action":       "created",
        "created_dirs": ["datasets/MY-EXPERIMENT/00_raw", ...]
    }
    """
    body = request.get_json(silent=True) or {}
    dataset = body.get("dataset")
    if not dataset:
        abort(400, "'dataset' is required")

    try:
        result = init_dataset(
            dataset=dataset,
            preset=body.get("preset", "full"),
            flow_name=body.get("flow_name"),
            raw=body.get("raw"),
            force=body.get("force", False),
        )
    except ValueError as e:
        abort(400, str(e))
    except FileExistsError as e:
        abort(409, str(e))
    except FileNotFoundError as e:
        abort(400, str(e))

    return jsonify(result), 201


@app.delete("/api/datasets/<dataset>")
def api_delete_dataset(dataset):
    """
    Supprimer un dataset et ses flows (équivalent de pyperun delete -y).

    Réponse 200 :
    {
        "deleted_dataset":  "MY-EXPERIMENT",
        "deleted_dirs":     ["datasets/MY-EXPERIMENT"],
        "deleted_flows":    ["flows/my-experiment.json"],
        "raw_symlink_kept": null
    }
    """
    try:
        result = delete_dataset(dataset=dataset)
    except FileNotFoundError as e:
        abort(404, str(e))

    return jsonify(result)


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------

@app.get("/api/flows")
def api_list_flows():
    """Liste des flows disponibles."""
    return jsonify(list_flows())


@app.get("/api/flows/<flow_name>/steps")
def api_list_steps(flow_name):
    """Étapes d'un flow avec leurs paramètres (passwords masqués)."""
    try:
        steps = list_steps(flow_name)
    except FileNotFoundError:
        abort(404, f"Flow '{flow_name}' not found")

    for s in steps:
        if s.get("params"):
            s["params"] = _redact_params(s["params"])
    return jsonify(steps)


# ---------------------------------------------------------------------------
# Treatments
# ---------------------------------------------------------------------------

@app.get("/api/treatments")
def api_list_treatments():
    """Liste des traitements disponibles."""
    return jsonify(list_treatments())


@app.get("/api/treatments/<name>")
def api_describe_treatment(name):
    """Détail d'un traitement (params, types, valeurs par défaut)."""
    try:
        return jsonify(describe_treatment(name))
    except FileNotFoundError:
        abort(404, f"Treatment '{name}' not found")


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

@app.get("/api/status")
def api_status():
    """État du pipeline pour tous les datasets (fichiers, dernière modif, up-to-date)."""
    return jsonify(get_status())


# ---------------------------------------------------------------------------
# Run a flow (async — retourne le run_id immédiatement)
# ---------------------------------------------------------------------------

@app.post("/api/run/<flow_name>")
def api_run_flow(flow_name):
    """
    Lance un flow en arrière-plan et retourne immédiatement le run_id.

    Body JSON (tous les champs sont optionnels) :
    {
        "from":        "2026-01-01T00:00:00Z",
        "to":          "2026-04-01T00:00:00Z",
        "last":        false,
        "step":        null,
        "from_step":   null,
        "to_step":     null,
        "output_mode": "append"
    }

    Réponse 202 :
    {"run_id": "a3f9b2c1", "flow": "valvometry_daily", "status": "started"}

    Polling : GET /api/runs/<run_id> toutes les 2s jusqu'à status=success|error.
    """
    known = [f["name"] for f in list_flows()]
    if flow_name not in known:
        abort(404, f"Flow '{flow_name}' not found")

    body = request.get_json(silent=True) or {}

    # Validation des args mutuellement exclusifs
    if body.get("last") and (body.get("from") or body.get("to")):
        abort(400, "'last' is mutually exclusive with 'from'/'to'")
    if body.get("step") and (body.get("from_step") or body.get("to_step")):
        abort(400, "'step' is mutually exclusive with 'from_step'/'to_step'")

    from pyperun.core.timefilter import parse_iso_utc
    try:
        time_from = parse_iso_utc(body["from"]) if body.get("from") else None
        time_to   = parse_iso_utc(body["to"])   if body.get("to")   else None
    except Exception as e:
        abort(400, f"Invalid date format: {e}")

    run_id = new_run_id()

    def _run():
        try:
            run_flow(
                flow_name,
                time_from=time_from,
                time_to=time_to,
                last=body.get("last", False),
                step=body.get("step"),
                from_step=body.get("from_step"),
                to_step=body.get("to_step"),
                output_mode=body.get("output_mode", "append"),
                run_id=run_id,
            )
        except SystemExit:
            pass  # run_flow fait SystemExit(1) sur erreur — normal en thread

    Thread(target=_run, daemon=True).start()

    return jsonify({"run_id": run_id, "flow": flow_name, "status": "started"}), 202


# ---------------------------------------------------------------------------
# Run history & polling
# ---------------------------------------------------------------------------

@app.get("/api/runs")
def api_list_runs():
    """
    Historique des runs récents.

    Query param : ?limit=50 (défaut)
    """
    limit = request.args.get("limit", 50, type=int)
    return jsonify(list_runs(limit=limit))


@app.get("/api/runs/<run_id>")
def api_run_events(run_id):
    """
    Événements d'un run spécifique — à appeler en polling toutes les 2s.

    Réponse :
    {
        "run_id": "a3f9b2c1",
        "flow": "valvometry_daily",
        "status": "running" | "success" | "error",
        "n_steps_total": 6,
        "n_steps_done": 3,
        "events": [
            {"ts": "...", "treatment": "parse",  "status": "start",   ...},
            {"ts": "...", "treatment": "parse",  "status": "success", "duration_ms": 1240},
            {"ts": "...", "treatment": "clean",  "status": "start",   ...},
            ...
        ]
    }
    Le polling peut s'arrêter dès que status = "success" ou "error".
    """
    events = get_run_events(run_id)
    if not events:
        abort(404, f"Run '{run_id}' not found")

    flow_name = events[0].get("flow")
    has_error = any(e["status"] == "error" for e in events)
    n_done    = sum(1 for e in events if e["status"] == "success")

    # Compter les steps du flow pour savoir si le run est terminé
    n_steps = 0
    if flow_name:
        try:
            n_steps = len(list_steps(flow_name))
        except FileNotFoundError:
            pass

    if has_error:
        run_status = "error"
    elif n_steps > 0 and n_done >= n_steps:
        run_status = "success"
    else:
        run_status = "running"

    return jsonify({
        "run_id": run_id,
        "flow": flow_name,
        "status": run_status,
        "n_steps_total": n_steps,
        "n_steps_done": n_done,
        "events": events,
    })


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Pyperun API server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    app.run(host=args.host, port=args.port, debug=args.debug)

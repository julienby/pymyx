# Projet : PyMyx

## Philosophie
Outil minimaliste de pipeline de traitement de données IoT (séries temporelles).
Inspiré de HTMX : simplicité, stabilité, pas d'abstraction inutile.
Pensé AI-first : chaque composant est lisible et manipulable par un LLM sans documentation supplémentaire.
Règle 80/20 : ne pas implémenter ce qui sera peu utilisé.

## Modèle mental
- 1 traitement = run.py (code pur) + treatment.json (métadonnées + schéma + params)
- 1 flow = composition ordonnée de traitements dans flow.json (DAG acyclique)
- Le filesystem est l'interface : l'état du pipeline se lit dans les dossiers
- Un traitement peut tourner seul ou dans un flow, sans rien changer à son code

## Structure du projet
pymyx/
├── core/
│   ├── runner.py        ← exécute un traitement seul
│   ├── flow.py          ← exécute un flow complet
│   ├── validator.py     ← valide treatment.json et les params
│   └── logger.py        ← events en jsonlines
├── treatments/
│   └── parse/
│       ├── run.py
│       └── treatment.json
├── flows/
│   └── valvometry_daily.json
├── data/
│   ├── 00_raw/          ← fichiers CSV source, immuables
│   ├── 10_parsed/
│   ├── 20_clean/
│   ├── 25_transform/
│   ├── 30_resampled/
│   ├── 40_aggregated/
│   └── 90_analysis/
├── tests/
│   └── test_runner.py
├── README.md
└── pyproject.toml

## Contrat treatment.json
Chaque traitement déclare ce fichier. C'est la seule source de vérité : schéma + params + doc.
{
  "name": "string",
  "version": "semver",
  "description": "string — lisible par un humain ou un LLM",
  "input": {
    "format": "csv|parquet",
    "description": "string"
  },
  "output": {
    "format": "parquet",
    "description": "string"
  },
  "params": {
    "<param_name>": {
      "type": "string|integer|float|boolean|object|array",
      "default": <valeur>,
      "description": "string"
    }
  }
}

## Contrat flow.json
{
  "name": "string",
  "version": "semver",
  "description": "string",
  "steps": [
    { "treatment": "<nom>", "input": "<dossier>", "output": "<dossier>" }
  ]
}

## Contrat run.py
Chaque run.py expose une seule fonction publique :
def run(input_dir: str, output_dir: str, params: dict) -> None
- Lit tous les fichiers du input_dir
- Écrit les fichiers transformés dans output_dir
- Ne loggue pas lui-même (c'est le rôle du runner)
- Ne connaît pas le flow dans lequel il s'exécute
- Lève des exceptions standard Python en cas d'erreur

## core/runner.py
- Charge treatment.json depuis treatments/<name>/
- Valide et merge les params (defaults + params fournis)
- Appelle run(input_dir, output_dir, params)
- Loggue un event jsonlines avant et après l'exécution
- CLI : python -m pymyx.core.runner --treatment <name> --input <dir> --output <dir> [--params <json_inline>]

## core/flow.py
- Lit flow.json depuis flows/
- Exécute les steps dans l'ordre via runner.py
- S'arrête proprement sur erreur avec message clair (quel step, quelle erreur)
- Loggue chaque step
- CLI : python -m pymyx.core.flow --flow <name>

## core/logger.py
Format jsonlines, un event par ligne :
{
  "ts": "ISO8601",
  "treatment": "string",
  "status": "start|success|error",
  "input_dir": "string",
  "output_dir": "string",
  "duration_ms": integer,
  "error": "string|null"
}
Fichier de log : pymyx.log à la racine du projet.

## core/validator.py
- Valide que treatment.json respecte le contrat
- Valide que les types des params fournis correspondent aux types déclarés
- Valide que input_dir existe avant d'exécuter
- Lève des erreurs explicites avec message actionnable

## Premier traitement à implémenter : parse
Données source réelles :
- Format CSV, séparateur ";"
- 1 ligne par seconde
- Colonne 0 : timestamp ISO8601 UTC
- Colonnes 1-12 : métriques bio_signal nommées m0 à m11, valeurs entières
- Colonne 13 : outdoor_temp, valeur float, domaine environment

treatment.json params :
{
  "delimiter": { "type": "string", "default": ";" },
  "tz": { "type": "string", "default": "UTC" },
  "timestamp_column": { "type": "string", "default": "ts" },
  "domains": {
    "type": "object",
    "default": {
      "bio_signal": ["m0","m1","m2","m3","m4","m5","m6","m7","m8","m9","m10","m11"],
      "environment": ["outdoor_temp"]
    }
  }
}

run.py doit :
- Lire tous les CSV du input_dir
- Parser le timestamp en datetime UTC
- Typer les colonnes (int pour bio_signal, float pour environment)
- Ajouter une colonne "domain" sur chaque ligne selon le mapping params.domains
- Écrire un fichier parquet par domaine et par jour source
  Naming : <domain>__<YYYY-MM-DD>.parquet
- Ne pas modifier les valeurs (c'est du parsing, pas du clean)

## Flow à implémenter : valvometry_daily
Steps :
1. parse      : 00_raw      → 10_parsed
2. clean      : 10_parsed   → 20_clean
3. transform  : 20_clean    → 25_transform
4. resample   : 25_transform → 30_resampled
5. aggregate  : 30_resampled → 40_aggregated

Seul le step parse doit être implémenté. Les autres steps doivent exister
comme stubs (treatment.json + run.py avec fonction run vide et TODO).

## Dépendances Python (minimalistes)
- pandas
- pyarrow
- pydantic (validation treatment.json)
- jsonlines

Pas de : airflow, prefect, dagster, celery, redis, ou tout autre orchestrateur.

## README.md
Expliquer en 20 lignes maximum :
- Ce qu'est # Projet : pymyx

## Philosophie
Outil minimaliste de pipeline de traitement de données IoT (séries temporelles).
Inspiré de HTMX : simplicité, stabilité, pas d'abstraction inutile.
Pensé AI-first : chaque composant est lisible et manipulable par un LLM sans documentation supplémentaire.
Règle 80/20 : ne pas implémenter ce qui sera peu utilisé.

## Modèle mental
- 1 traitement = run.py (code pur) + treatment.json (métadonnées + schéma + params)
- 1 flow = composition ordonnée de traitements dans flow.json (DAG acyclique)
- Le filesystem est l'interface : l'état du pipeline se lit dans les dossiers
- Un traitement peut tourner seul ou dans un flow, sans rien changer à son code

## Structure du projet
pymyx/
├── core/
│   ├── runner.py        ← exécute un traitement seul
│   ├── flow.py          ← exécute un flow complet
│   ├── validator.py     ← valide treatment.json et les params
│   └── logger.py        ← events en jsonlines
├── treatments/
│   └── parse/
│       ├── run.py
│       └── treatment.json
├── flows/
│   └── valvometry_daily.json
├── data/
│   ├── 00_raw/          ← fichiers CSV source, immuables
│   ├── 10_parsed/
│   ├── 20_clean/
│   ├── 25_transform/
│   ├── 30_resampled/
│   ├── 40_aggregated/
│   └── 90_analysis/
├── tests/
│   └── test_runner.py
├── README.md
└── pyproject.toml

## Contrat treatment.json
Chaque traitement déclare ce fichier. C'est la seule source de vérité : schéma + params + doc.
{
  "name": "string",
  "version": "semver",
  "description": "string — lisible par un humain ou un LLM",
  "input": {
    "format": "csv|parquet",
    "description": "string"
  },
  "output": {
    "format": "parquet",
    "description": "string"
  },
  "params": {
    "<param_name>": {
      "type": "string|integer|float|boolean|object|array",
      "default": <valeur>,
      "description": "string"
    }
  }
}

## Contrat flow.json
{
  "name": "string",
  "version": "semver",
  "description": "string",
  "steps": [
    { "treatment": "<nom>", "input": "<dossier>", "output": "<dossier>" }
  ]
}

## Contrat run.py
Chaque run.py expose une seule fonction publique :
def run(input_dir: str, output_dir: str, params: dict) -> None
- Lit tous les fichiers du input_dir
- Écrit les fichiers transformés dans output_dir
- Ne loggue pas lui-même (c'est le rôle du runner)
- Ne connaît pas le flow dans lequel il s'exécute
- Lève des exceptions standard Python en cas d'erreur

## core/runner.py
- Charge treatment.json depuis treatments/<name>/
- Valide et merge les params (defaults + params fournis)
- Appelle run(input_dir, output_dir, params)
- Loggue un event jsonlines avant et après l'exécution
- CLI : python -m pymyx.core.runner --treatment <name> --input <dir> --output <dir> [--params <json_inline>]

## core/flow.py
- Lit flow.json depuis flows/
- Exécute les steps dans l'ordre via runner.py
- S'arrête proprement sur erreur avec message clair (quel step, quelle erreur)
- Loggue chaque step
- CLI : python -m pymyx.core.flow --flow <name>

## core/logger.py
Format jsonlines, un event par ligne :
{
  "ts": "ISO8601",
  "treatment": "string",
  "status": "start|success|error",
  "input_dir": "string",
  "output_dir": "string",
  "duration_ms": integer,
  "error": "string|null"
}
Fichier de log : pymyx.log à la racine du projet.

## core/validator.py
- Valide que treatment.json respecte le contrat
- Valide que les types des params fournis correspondent aux types déclarés
- Valide que input_dir existe avant d'exécuter
- Lève des erreurs explicites avec message actionnable

## Premier traitement à implémenter : parse
Données source réelles :
- Format CSV, séparateur ";"
- 1 ligne par seconde
- Colonne 0 : timestamp ISO8601 UTC
- Colonnes 1-12 : métriques bio_signal nommées m0 à m11, valeurs entières
- Colonne 13 : outdoor_temp, valeur float, domaine environment

treatment.json params :
{
  "delimiter": { "type": "string", "default": ";" },
  "tz": { "type": "string", "default": "UTC" },
  "timestamp_column": { "type": "string", "default": "ts" },
  "domains": {
    "type": "object",
    "default": {
      "bio_signal": ["m0","m1","m2","m3","m4","m5","m6","m7","m8","m9","m10","m11"],
      "environment": ["outdoor_temp"]
    }
  }
}

run.py doit :
- Lire tous les CSV du input_dir
- Parser le timestamp en datetime UTC
- Typer les colonnes (int pour bio_signal, float pour environment)
- Ajouter une colonne "domain" sur chaque ligne selon le mapping params.domains
- Écrire un fichier parquet par domaine et par jour source
  Naming : <domain>__<YYYY-MM-DD>.parquet
- Ne pas modifier les valeurs (c'est du parsing, pas du clean)

## Flow à implémenter : valvometry_daily
Steps :
1. parse      : 00_raw      → 10_parsed
2. clean      : 10_parsed   → 20_clean
3. transform  : 20_clean    → 25_transform
4. resample   : 25_transform → 30_resampled
5. aggregate  : 30_resampled → 40_aggregated

Seul le step parse doit être implémenté. Les autres steps doivent exister
comme stubs (treatment.json + run.py avec fonction run vide et TODO).

## Dépendances Python (minimalistes)
- pandas
- pyarrow
- pydantic (validation treatment.json)
- jsonlines

Pas de : airflow, prefect, dagster, celery, redis, ou tout autre orchestrateur.

## README.md
Expliquer en 20 lignes maximum :
- Ce qu'est pymyx et ce qu'il n'est pas
- Le modèle mental (traitement / flow / filesystem)
- Comment lancer un traitement seul
- Comment lancer un flow complet
- Comment créer un nouveau traitement

## Tests
Un test pour runner.py :
- Crée un traitement de test temporaire
- Vérifie que run() est appelé avec les bons params mergés avec les defaults
- Vérifie que le log contient bien un event success et ce qu'il n'est pas
- Le modèle mental (traitement / flow / filesystem)
- Comment lancer un traitement seul
- Comment lancer un flow complet
- Comment créer un nouveau traitement

## Tests
Un test pour runner.py :
- Crée un traitement de test temporaire
- Vérifie que run() est appelé avec les bons params mergés avec les defaults
- Vérifie que le log contient bien un event success
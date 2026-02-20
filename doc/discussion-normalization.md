# Discussion : Normalisation des données valvométriques

## Contexte

En valvométrie, les capteurs mesurent l'ouverture/fermeture des valves de mollusques
bivalves (moules, huîtres). Chaque capteur a son propre range d'évolution selon sa
position et les caractéristiques individuelles de l'animal. La normalisation est nécessaire
pour comparer les signaux entre capteurs et entre individus.

---

## Critique de Normalize.py (code initial)

### Ce qui est bien
- L'idée de `MinMaxAjustedNormalize` : utiliser l'histogramme pour ignorer les outliers
  plutôt que le min/max brut. Bon instinct.

### Problèmes identifiés

**1. Pas de persistance des paramètres — défaut critique**
Les min/max sont calculés et jetés. Si tu fites sur le jour 1 et le jour 2
indépendamment, tu obtiens deux normalisations différentes : impossible de comparer
les jours entre eux.

**2. `Correction.norm_correct` buggé**
`last_value = 1.0` est partagé entre toutes les lignes au lieu d'être réinitialisé
par colonne. Le forward-fill se propage incorrectement d'une série à l'autre.

**3. Génération des bins fragile**
`np.arange(0, np.max(data), 0.001)` : si les valeurs montent à 1000, ça génère
1 million de bins. Lent et ne fonctionne pas avec des valeurs négatives.

**4. `SubNormalize` trop spécifique**
Le concept de breakpoint est intéressant mais trop couplé à un cas d'usage particulier
pour être un composant générique de pipeline.

**5. Non intégré au pipeline**
Travaille sur des numpy arrays en mémoire, pas sur des fichiers parquet.
Incompatible avec l'architecture run.py / traitement / flow.

---

## Décisions de design

### Méthode : percentile P2/P98

Plutôt que min/max brut ou seuil d'histogramme, on utilise les percentiles :

```
normalized = clip((x - P2) / (P98 - P2), 0, 1)
```

- **Robuste aux outliers** sans paramètre de seuil à tuner
- **Simple à débugger** : P2 ≈ "état fermé typique", P98 ≈ "état ouvert typique"
- **Clipping** : les valeurs hors-range sont clampées à [0, 1] (pas de forward-fill magique)

### Normalisation par device

Chaque capteur (pil-90, pil-98…) a ses propres paramètres P2/P98. Les animaux
ont des comportements et des positions de capteur différents → normalisation indépendante.

### Pattern fit / apply

La normalisation est séparée en deux phases :

- **fit** (`fit=true`) : parcourt toutes les données disponibles, calcule P2/P98
  par device par colonne, sauve `normalize_params.json`
- **apply** (`fit=false`, défaut) : charge le JSON, applique, clip

Le `normalize_params.json` est versionnable et lisible — source de vérité de la
normalisation. Si params manquants et `fit=false` → erreur explicite (pas de magie).

### Position dans le pipeline

```
parse → clean → resample → transform → normalize → aggregate → to_postgres / exportcsv
                                        30_transform  35_normalized  40_aggregated
```

Après `transform`, avant `aggregate` : la moyenne agrégée = moyenne des valeurs
normalisées → directement comparable entre capteurs.

---

## Structure de normalize_params.json

```json
{
  "_meta": {
    "method": "percentile",
    "percentile_min": 2.0,
    "percentile_max": 98.0,
    "fit_window_days": 14,
    "fitted_at": "2026-02-20T10:00:00Z",
    "n_files": 28,
    "n_devices": 2
  },
  "PREMANIP_GRACE_pil-90": {
    "m0": {"p2": 12.0, "p98": 847.0},
    "m1": {"p2": 5.0,  "p98": 920.0},
    "..."
  },
  "PREMANIP_GRACE_pil-98": {
    "m0": {"p2": 9.0,  "p98": 791.0},
    "..."
  }
}
```

---

## Paramètres du traitement

| Param | Défaut | Description |
|-------|--------|-------------|
| `fit` | `false` | Si true : calcule et sauve les params |
| `method` | `"percentile"` | `"percentile"` ou `"minmax"` |
| `percentile_min` | `2.0` | Percentile bas (P2) |
| `percentile_max` | `98.0` | Percentile haut (P98) |
| `domain` | `"bio_signal"` | Domaine à normaliser |
| `columns` | `[]` | Colonnes à normaliser (vide = toutes numériques) |
| `clip` | `true` | Clamp les valeurs dans [0, 1] |
| `fit_window_days` | `0` | Fenêtre glissante pour le fit (0 = tout l'historique) |
| `min_range_warn` | `0` | Seuil de warning si P98-P2 trop petit (0 = désactivé) |

---

## Workflow opérationnel

### Premier lancement (après période de calibration)
```bash
pymyx flow valvometry_full --output-mode full-replace
# normalize a fit=true → calcule P2/P98 sur toutes les données disponibles
```

### Run quotidien incrémental
```bash
pymyx flow valvometry_daily --last
# normalize a fit=false → charge normalize_params.json, applique
```

### Refit (nouvelles données significatives)
```bash
pymyx flow valvometry_full --output-mode full-replace
# recalcule tout depuis le début
```

---

## Fenêtre de fit et contrainte biologique

### Le problème fondamental

Pour que P2/P98 soient représentatifs, la **fenêtre de fit doit contenir au moins
un cycle complet** ouverture totale / fermeture totale. Si la moule reste fermée
pendant toute la fenêtre (stress, spawning, hypoxie, pollution) :
- P98 ≈ valeur quasi-fermée
- Range P98 - P2 ≈ 0
- Normalisation catastrophique

### Recommandation pratique

**Attendre 3 à 7 jours minimum** avant le premier fit, idéalement 7-14 jours pour
capturer des cycles jour/nuit, de marée, et au moins un événement de fermeture
complète. La croissance de la moule étant lente, une fenêtre de 14 jours reste
valide physiologiquement sur la durée d'une expérience standard.

### fit_window_days

`fit_window_days` limite le fit aux N derniers jours disponibles dans le répertoire
d'entrée. Utile pour les longues expériences où les conditions évoluent (température,
saison, acclimatation).

Exemple dans `valvometry_full.json` :
```json
{
    "treatment": "normalize",
    "params": {
        "fit": true,
        "fit_window_days": 14,
        "min_range_warn": 100
    }
}
```

### min_range_warn

Garde-fou opérationnel : si `P98 - P2 < min_range_warn` pour une colonne, le step
affiche un warning. La valeur seuil dépend du range physique attendu pour ton capteur
(à déterminer empiriquement).

Exemple de sortie :
```
[normalize] WARNING: 1 column(s) have range < 100 — window may not capture full behavioral range:
    PREMANIP_GRACE_pil-90/m3: range=42.00 (p2=210.00, p98=252.00)
```

---

## Idée : méta-analyse pour calibrer fit_window_days

Sur les expériences passées, faire tourner le fit sur des fenêtres croissantes
(1j, 2j, 3j… 30j) et observer la convergence de P2/P98 :

```
Pour chaque expérience historique :
  Pour window in [1, 2, 3, ..., 30] jours :
    Calculer P2/P98 sur les derniers `window` jours
    Comparer à la valeur de référence (fit sur 30 jours)

La "fenêtre minimale fiable" = là où les percentiles convergent à ±5%
```

C'est une étude de **stabilité des estimateurs** — à implémenter comme notebook
d'analyse, pas comme partie du pipeline. Le résultat serait une valeur de
`fit_window_days` justifiée empiriquement plutôt qu'arbitraire.

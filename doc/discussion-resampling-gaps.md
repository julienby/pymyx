# Discussion : Resampling et gestion des gaps temporels

## Contexte

Le step `resample` convertit des données à timestamps irréguliers (capteurs IoT) en une
grille temporelle régulière à 1 seconde. Le paramètre clé est `max_gap_fill_s` : durée
maximale d'un trou comblé par forward-fill.

---

## Paramètres du traitement

| Param | Défaut | Description |
|-------|--------|-------------|
| `freq` | `"1s"` | Fréquence de la grille de sortie |
| `max_gap_fill_s` | `20` | Trous <= N secondes → forward-fill. Trous > N → NaN |
| `agg_method` | `{"bio_signal": "nearest", "environment": "mean"}` | Fusion si plusieurs mesures dans la même seconde |

**`agg_method` : pourquoi nearest vs mean ?**
- `bio_signal` → `nearest` : les valves bougent vite, la moyenne sur 1s n'a pas de sens
- `environment` → `mean` : température extérieure, peu de variation → la moyenne est pertinente

---

## Règle générale : ne pas sur-imputer

Le forward-fill **invente des données**. Plus tu étires, plus tu biaises.

| Durée | Statut |
|-------|--------|
| 2-5s | Légitime — bruit réseau, perte de paquet |
| jusqu'à ~60s | Acceptable si le capteur est stable sur cette durée. À documenter. |
| > 60s | Dangereux — masque un vrai événement (capteur débranché, anomalie) |

---

## Pour des stats classiques

Les gaps longs (> 60s) doivent rester en `NaN` : ils sont informatifs.
Un forward-fill long masque un événement réel (capteur mort, stress de l'animal).

---

## Pour du ML/IA

Encore plus sensible :

- **Le modèle apprend les patterns artificiels** introduits par le forward-fill
  (plateaux plats, transitions abruptes à la reprise)
- **Les gaps sont souvent informatifs** — en valvométrie, une moule immobile 30s
  est un comportement, pas une absence de signal
- **Préférer l'interpolation linéaire** au forward-fill pour les courts trous si le
  signal est continu
- **Garder un masque binaire** `is_interpolated` comme feature — le modèle apprend
  à pondérer différemment les valeurs imputées

### Stratégie recommandée

**1. Imputation minimale + masque**

```python
# Forward-fill seulement les très courts trous (bruit réseau)
max_gap_fill_s = 5

# Ajouter un flag binaire par colonne
df["m0__is_interpolated"] = df["m0"].isna()  # avant ffill
df["m0"] = df["m0"].ffill(limit=5)
```

**2. Ne pas imputer les longs gaps — segmenter**

Un gap de 5 minutes = une coupure de séquence, pas une continuation.

```
[seg_1: 08:00 → 08:32] [gap] [seg_2: 08:37 → 09:10]
```

- Pour un RNN/LSTM : chaque segment est une séquence indépendante
- Pour une fenêtre glissante : les fenêtres qui chevauchent un gap sont exclues de l'entraînement

**3. Encoder le temps écoulé (Time-aware models)**

```python
df["dt"] = df.index.diff().dt.total_seconds()
```

Des architectures comme **GRU-D** ou **mTAND** utilisent `dt` pour pondérer la
décroissance de l'information dans le temps. Au lieu de cacher le gap, on le donne
en feature.

### En pratique pour la valvométrie

```
max_gap_fill_s: 5        # forward-fill réseau uniquement
+ colonne dt             # temps depuis dernière mesure réelle
+ flag is_interpolated   # masque binaire
+ segmentation sur gaps  # exclure les fenêtres qui chevauchent un trou
```

Le `NaN` n'est pas un problème à cacher — c'est de l'information comportementale.

---

## Décision retenue

`max_gap_fill_s: 20` pour le pipeline valvométrie actuel.

Justification : compromis entre qualité du signal et continuité opérationnelle.
Les trous courts (< 20s) sont typiquement du bruit de transmission.
Les trous longs restent en `NaN` et sont donc visibles dans les exports et agrégats.

> Note : `valvometry_full` utilisait `max_gap_fill_s: 86400` (24h) pour ne jamais
> laisser de trous — à éviter pour du ML car cela invente potentiellement des heures
> de données.

# TODO

## Transform

- [ ] Ajouter `cbrt_inv` dans `pymyx/treatments/transform/run.py`
  ```python
  "cbrt_inv": lambda s: 1.0 / np.cbrt(s.where(s > 0, other=np.nan))
  ```
  Remplace `sqrt_inv` (B^(-1/2), monopole) par `cbrt_inv` (B^(-1/3), dipôle).
  Physiquement justifié pour un capteur à effet Hall + aimant permanent (dipôle magnétique : B ∝ 1/r³).
  Mettre à jour les flows (`*__sqrt_inv` → `*__cbrt_inv`) et la normalisation en conséquence.
  Voir `doc/discussion-resampling-gaps.md` et discussion du 2026-02-22.

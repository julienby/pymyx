mes use case : 
* relancer 1 seule journée
* pourvoir vider le répertoire output avant d'écrire à nouveau dedans (quand je veux relancer tout un dataset, j'ai fait des modifications)
* pouvoir lancer un traitement entre 2 périodes de temps précises from yyyy-mm-dd h:m:s to yyyy-mm-dd h:m:s  
* pouvoir lancer uniquement à partir d'un certaine date from yyyy-mm-dd h:m:s s'il n'y a pas de to c'est que l'onveu aller jusqu'au plus récent (?? comportement raisonnable)
* la gestion des dates doit être homogène UTC Z -> yyyy-mm-ddTh:m:sZ? pendant tout le traitement comme ça pas d'erreur. Dans les metas données on peut ajouter le fuseau horaire d'enregistrement des données pour pouvoir reconstruire  l'heure locale.
* --last je veux pouvoir lancer un traitement toute les heures en traitant les données de manière intelligente. on va partir d'une fenetre de temps de 1h minimum. donc s'il y a des données jusquà 13h35 on recalcul à partir de 13h

je veux pouvoir lancer un traitement unitaire comme un flow general avec les mêmes variables.
--from yyyy-mm-ddTh:m:sZ --to yyyy-mm-ddTh:m:sZ
--from yyyy-mm-ddTh:m:sZ (d'une date jusqu'au plus récent)
--to yyyy-mm-ddTh:m:sZ (on prend tout jusqu'à une certaine date)
--output-mode (replace|append)
  replace (on supprime le contenu du répertoire output avant d'écrire) 
  append (on ajoute de données on ne supprime rien, eventuellement on écrase l'existant)
--last on compare la dernière donnée output (timesteamp) et la dernière donnée input et on comble le delta. La fenetre de temps minimale est l'heure.

le fonctionnement doit être simple et clair ... toute l'idée est de pouvoir rejouer des traitements en changeant quelques parametres et en gardant de la souplesse sur la fenêtre de temps (parfois je veux faire des petits flow mais très régulièrement -> pilotage parfois c'est plus gros pour l'analyse)

complète ce fonctionnement si tu vois des cas non couverts


## Mission filename

step are : (raw|parsed|clean|transform|resampled|aggregated|... etc)

00_raw : données brutes
Input : PREMANIP_GRACE_pil-90_2026-01-20.csv
pattern <experience>_<device_id>_<day>
Output : domain=<domain_value>/<experience>__<device_id>__<step>__<day>.parquet

10_parsed : fichier lisible / données avec type attendu (si on attend un integer on a un integer)
Input : domain=<domain_value>/<experience>__<device_id>__<step>__<day>.parquet
Output : domain=<domain_value>/<experience>__<device_id>__<step>__<day>.parquet

20_clean : on vire les données qui n'ont pas de sens (range / spike / ...)
Input : domain=<domain_value>/<experience>__<device_id>__<step>__<day>.parquet
Output : domain=<domain_value>/<experience>__<device_id>__<step>__<day>.parquet

25_transform :
Input : domain=<domain_value>/<experience>__<device_id>__<step>__<day>.parquet
Output : domain=<domain_value>/<experience>__<device_id>__<step>__<day>.parquet

30_resampled
Input : domain=<domain_value>/<experience>__<device_id>__<step>__<day>.parquet
Output : domain=<domain_value>/<experience>__<device_id>__<step>__<day>.parquet

40_aggregated
aggregation : (1s|10s|60s|1h ... etc)
Input : domain=<domain_value>/<experience>__<device_id>__<step>__<day>.parquet
Output : domain=<domain_value>/<experience>__<device_id>__<step>__<aggregation>__<day>.parquet

## étape transformation
Etape : après le clean 

    if transfo_type == "sqrt_inv":
        # sqrt(1/x) pour x > 0, sinon NaN
        return pl.when(col_expr > 0).then((1.0 / col_expr) ** 0.5).otherwise(None)
    if transfo_type == "log":
        # ln(x) pour x > 0, sinon NaN
        return pl.when(col_expr > 0).then(col_expr.log()).otherwise(None)
il peut y en avoir d'autres
input : time;m0;m1;m2
transfo : m0 -> applique une transformation sqrt(1/x) -> genere une colonne en plus m0_sqrt_inv
...
transfo : m1 -> applique une transformation sqrt(1/x) -> genere une colonne en plus m1_sqrt_inv
output : time;m0;m0_sqrt_inv;m1;m0_sqrt_inv
ou alors on remplace : 
m0 est remplacé par m0 après transformation
output : time;m0;m1;m2


## to postgres

Maintenant on va créer un step pour alimenter des tables postgres
Structure de la table:
nom : <experience>_<step>_<aggregation> en majuscule
exemple : PREMANIP_GRACE_RAW_60s

champs :
Colonne	Type	Commentaire
id      primary key (useful ?)
time	timestamptz NULL	
device_id	text NULL	
sensor	text NULL	
value	numeric(18,2) NULL	

- si la table n'existe pas elle est auto créé
- possibilié de faire un truncate avant insertion (mode replace)
- possibilité d'être en mode append pour compléter juste ce qui manque (ex je veux alimenter ma table toutes les heures et je complète les données manquantes). le script regarde la dernière valeur de la table et complète intelligemment. on Reste simple
- les données de connexion sont contenu dans la parametres (hosname, user, pass, table_name basé sur le nom des fichiers pour éviter les erreurs). possibilté de composer le nom de la table à partir des variables <experience> <step> <aggregation> come ça si je veux ajouter une specification dans le nom de la table c'est possible 
ex LIVE_<experience>_<step>_<aggregation>
ou par défaut <experience>_<step>_<aggregation>
ou TEST_<experience>_ABC
- il faut optimiser les index pour avoir un requetage rapide (timestamp / device_id / sensor)

l'insertion dans la base pourra venir après un step 60_ 70_ ou 80_  plus tard

## Export ✅ DONE

61_EXPORTNOUR : export CSV depuis aggregated, un fichier par device.

Format cible : tab-separated, colonne Time en Europe/Paris (YYYY-MM-DD HH:MM:SS), colonnes déclaratives avec mapping source → nom exporté.

Fichier : `<experience>_<device_id>_aggregated_<aggregation>_<from>_<to>.csv`

Implémenté :
- choix des colonnes et renommage déclaratif via `columns` dict
- conversion timezone UTC → Europe/Paris
- filtrage optionnel par `from` / `to` (dates YYYY-MM-DD)
- agrégation 10s mean par défaut
- un fichier CSV par device
- traitement : `pymyx/treatments/exportnour/`
- tests : `tests/test_exportnour.py`

Fix associé : le resample démarre maintenant la grille au premier point de donnée valide (plus de lignes vides en début de journée quand le capteur n'envoie pas encore de bio_signal).
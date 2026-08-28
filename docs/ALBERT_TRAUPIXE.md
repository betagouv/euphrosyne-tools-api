# Visualisation de données — support TRAUPIXE

## Version actuelle

Le service de visualisation est indépendant du format de données. Cette première
version permet de questionner un classeur TRAUPIXE depuis le cahier de laboratoire
et d'afficher les visualisations proposées par Albert.

Le périmètre est volontairement limité :

- fichiers `.xlsx` dont le nom contient `TRAUPIXE` ;
- choix explicite du fichier lorsqu'un run en contient plusieurs ;
- une question indépendante, sans historique de conversation ;
- lecture des feuilles `S_Conc. %` ou `S_Conc. ppm` et, si elle existe,
  `S_Best Det.` ;
- calcul des données par du code Python généré par Albert ;
- une à huit options ECharts autonomes, sans liste fermée de graphiques ;
- message d'erreur invitant l'utilisateur à réessayer.

L'interprétation du nom du fichier, les analyses statistiques métier avancées,
l'export, la persistance des résultats et les conversations multi-tours sont hors
de ce périmètre.

Le contrat V1 repose uniquement sur la structure observée des exports : une feuille
`S_Conc. %` (préférée) ou `S_Conc. ppm`, une ligne d'en-têtes avec les deux premières
cellules vides et au moins trois analytes à partir de la troisième colonne, puis les
analyses identifiées dans les deux premières colonnes. La liste des analytes est
entièrement issue du classeur ; aucun analyte « majeur » n'est codé en dur.

Un identifiant contenant `_STD_` désigne explicitement une référence. Les autres
lignes ont le type prudent `unknown` : elles sont utilisées par défaut dans les
calculs, mais le service ne prétend pas qu'il s'agit nécessairement d'objets. Le
libellé source est toujours conservé. Un champ `group` retire seulement un suffixe
final `ptN` ou `pointN` (séparé par un espace, `_` ou `-`) afin d'aider les demandes
de regroupement ; il reste une heuristique et ne remplace pas le libellé source.

Lorsque `S_Best Det.` existe, ses lignes sont associées aux concentrations par
identifiant et rang d'occurrence de cet identifiant. Cela préserve les détecteurs
distincts des analyses dont l'identifiant apparaît plusieurs fois. Le nom du fichier
sert uniquement à découvrir les fichiers TRAUPIXE et n'alimente aucune donnée métier.

## Flux actuel

```text
Classeur + question
        ↓
Lecture TRAUPIXE minimale et JSON normalisé
        ↓
Albert génère un calcul Python
        ↓
Exécution Python dans Azure Dynamic Sessions
        ↓
Résultat de calcul JSON
        ↓
Albert génère une réponse structurée
        ↓
Options ECharts validées puis affichées par le frontend
```

Le modèle doit affecter son calcul à une variable `result`. Le backend la
sérialise en JSON et peut demander une correction lorsque l'exécution ou la réponse
finale est invalide.

Le contrat d'une visualisation reste minimal :

```json
{
  "title": "Titre du graphique",
  "option": {}
}
```

Le backend vérifie uniquement des garde-fous génériques : JSON fini, taille bornée
et exclusion des champs ECharts susceptibles de charger une ressource externe,
d'injecter du contenu ou de déclencher une navigation. Il ne limite ni les types de
graphiques ni le nombre de séries. Le frontend transmet l'option à ECharts sans
interpréter de chaînes comme du code.

## Exécution Python et journalisation

`AZURE_SESSION_POOL_ENDPOINT` configure le pool Azure. `DefaultAzureCredential`
authentifie Tools API auprès de l'API data-plane ; son identité doit posséder le rôle
`Azure ContainerApps Session Executor` sur le pool. Seul le jeu de données TRAUPIXE
normalisé est placé dans `/mnt/data` ; le classeur original reste dans Tools API. Le
résultat JSON est récupéré puis la session est supprimée.

Chaque question autorisée, accompagnée du projet, du chemin du fichier et du
`request_id`, est envoyée à Sentry sous forme de message de niveau `info` pour
analyser l'usage de la fonctionnalité. Les métadonnées de chaque completion Albert
sont ajoutées comme breadcrumbs et contexte aux éventuelles exceptions, sans le
prompt ni le contenu généré. Pour faciliter la mise au point des prompts,
`DATA_VISUALIZATION_TRACE=1` active en plus une trace des requêtes et réponses LLM
complètes dans un fichier JSONL rotatif hors du dépôt. Sur macOS :

```text
~/Library/Logs/euphrosyne-tools-api/data-visualization-exchanges.jsonl
```

Ces traces contiennent notamment la question, le code Python, les résultats calculés
et les réponses du modèle. Elles doivent être traitées comme des données métier
sensibles et sont désactivées par défaut, quel que soit l'environnement d'exécution.

## Endpoint

```http
POST /data/{project_slug}/visualizations
Content-Type: application/json

{
  "path": "projects/project-01/runs/run-01/processed_data/TRAUPIXE-example.xlsx",
  "question": "Compare les concentrations en fer, cuivre et plomb."
}
```

`ALBERT_API_KEY`, `ALBERT_MODEL` et `AZURE_SESSION_POOL_ENDPOINT` sont requis. La
réponse contient `request_id`, `answer` et `visualizations`. Les erreurs d'entrée
utilisent un code stable parmi `INVALID_FILE_PATH`, `UNSUPPORTED_FILE_TYPE`,
`FILE_TOO_LARGE` et `INVALID_DATA_FILE`, accompagné du `request_id`.

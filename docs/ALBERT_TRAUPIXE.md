# Visualisation Albert / TRAUPIXE

## Version actuelle

Cette première version permet de questionner un classeur TRAUPIXE depuis le cahier
de laboratoire et d'afficher les visualisations proposées par Albert.

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

Dans les lignes du classeur, un identifiant contenant `_STD_` ou le libellé
`MesureCharge` désigne une référence ; les autres lignes sont considérées comme des
analyses d'objet. Cette règle minimale devra être réévaluée si de nouvelles variantes
TRAUPIXE l'invalident.

## Flux actuel

```text
Classeur + question
        ↓
Lecture TRAUPIXE minimale et JSON normalisé
        ↓
Albert génère un calcul Python
        ↓
Exécution Python locale
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

Le backend vérifie uniquement des garde-fous génériques : JSON fini, taille et
nombre de séries bornés, présence d'au moins une série et absence de ressources
externes. Le frontend ajoute seulement des valeurs de présentation par défaut
(marges, lisibilité des axes et infobulles) avant de transmettre l'option à ECharts.

## Exécution locale et journalisation

L'exécution Python locale est un outil de développement non isolé. Le sous-processus
ne reçoit qu'une liste limitée de variables nécessaires à Python et ne doit pas être
activé en production.

Les échanges complets sont enregistrés hors du dépôt dans un fichier JSONL rotatif.
Sur macOS :

```text
~/Library/Logs/euphrosyne-tools-api/albert-exchanges.jsonl
```

Ils contiennent la question, le code Python et les réponses du modèle et doivent
être traités comme des données métier sensibles. La sortie standard ne contient que
l'identifiant de requête, le type d'événement et les informations de synthèse.

## Jalons suivants

### Exécution Python isolée sur Azure

Ce jalon remplacera l'exécuteur local par Azure Dynamic Sessions sans modifier le
contrat fonctionnel. Il sera engagé lorsque le Session Pool sera disponible.

Critères de sortie :

- transfert du classeur et récupération du résultat JSON ;
- interface et tests de contrat communs aux exécuteurs ;
- limites de temps et de ressources, nettoyage des sessions et erreurs maîtrisées ;
- corrélation des journaux sans exposition de données sensibles ;
- suppression de l'exécution locale dans les environnements déployés.

### Analyses scientifiques AGLAE

Ce jalon ajoutera des primitives Python déterministes inspirées du rapport
`20260730_TaCT_rapport AGLAE_pates.docx` : normalisation à 100 %, traitement des
valeurs nulles, transformation CLR, distance d'Aitchison, classification de Ward,
ACP et cercle des corrélations.

Les premières visualisations cibles seront les barres groupées des oxydes majeurs,
le dendrogramme CLR–Aitchison–Ward, le plan factoriel ACP PC1–PC2 et le cercle des
corrélations. Les calculs seront spécialisés côté backend, mais leur restitution
restera constituée d'options ECharts génériques afin de ne pas ajouter de types de
graphiques au frontend.

Avant ce jalon, il faudra définir la source des groupes stylistiques (`A`, `B`, `C`,
`D`, `LM`), les règles d'agrégation des analyses d'un même objet, les exclusions et
le remplacement des valeurs nulles.

## Endpoint

```http
POST /aglae/{project_slug}/visualizations
Content-Type: application/json

{
  "path": "projects/project-01/runs/run-01/processed_data/TRAUPIXE-example.xlsx",
  "question": "Compare les concentrations en fer, cuivre et plomb."
}
```

`ALBERT_API_KEY` et `ALBERT_MODEL` sont requis. Le timeout est de 300 secondes par
appel. La réponse contient `request_id`, `answer` et `visualizations`.

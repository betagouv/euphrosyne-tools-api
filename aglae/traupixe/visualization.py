from __future__ import annotations

import json

from data_visualization.models import PreparedDataVisualization

from .dataset import load_traupixe_dataset, serialize_traupixe_for_model
from .format import MAX_SOURCE_SIZE_BYTES

DATASET_FILENAME = "traupixe_data.json"

CALCULATION_INSTRUCTIONS = """
Lis le JSON normalisé, dont le contrat exact est :

- data["analytes"] est une liste d'objets ;
- names = [item["name"] for item in data["analytes"]] donne l'ordre des analytes ;
- chaque objet de data["analyses"] possède identifier, label, group, kind, puis les
  trois LISTES parallèles values, detected et detectors, dans l'ordre de names ;
- accède donc à un analyte avec index = names.index("Fe2O3"), puis
  analysis["values"][index], jamais analysis["values"]["Fe2O3"] ;
- detected[index] est un booléen : false désigne une limite de détection, pas une
  mesure quantitative.

kind vaut "reference" uniquement lorsqu'un marqueur explicite a été reconnu ;
"unknown" signifie que la ligne n'est pas classée, pas qu'il s'agit nécessairement
d'un objet. Sauf demande explicite contraire, sélectionne les analyses ainsi :
selected = [a for a in data["analyses"] if a["kind"] != "reference"] or data["analyses"]
Le repli sur toutes les analyses permet de visualiser un classeur qui ne contient
que des références.
group est une aide de regroupement dérivée d'un suffixe final pt/point ; label reste
le libellé source faisant autorité.

Minimise la taille de result sans omettre de données :
- pour des observations tabulaires, déclare dimensions une seule fois et utilise
  rows comme liste de tableaux alignés sur dimensions ;
- pour une matrice ou une heatmap, fournis rows, columns et une matrice values au
  format values[row_index][column_index] ; detected et detectors sont, si utiles,
  des matrices parallèles de mêmes dimensions ;
- plusieurs jeux de données peuvent être placés dans un objet datasets nommé.

Ne produis jamais une liste plate cells qui répète pour chaque cellule ses
coordonnées, son analyte ou les libellés déjà présents dans rows et columns. Pour une
matrice de détecteurs, conserve néanmoins une valeur, éventuellement null, par couple
analyse retenue × analyte.
""".strip()

VISUALIZATION_INSTRUCTIONS = """
Pour une matrice de détecteurs TRAUPIXE, les deux axes sont les analyses retenues et
les analytes ; le détecteur est la valeur de chaque cellule, jamais un axe. Conserve
une cellule par couple analyse × analyte, y compris les détecteurs absents. Place les
analytes, dont les libellés sont courts, sur l'axe horizontal et les analyses sur
l'axe vertical. Conserve les libellés complets dans les données et dans les noms des
points pour qu'ils restent accessibles dans les infobulles. Encode les détecteurs
comme valeurs numériques et restitue leurs libellés avec visualMap.
""".strip()


class TraupixeVisualizationHandler:
    max_source_size_bytes = MAX_SOURCE_SIZE_BYTES

    def prepare(self, workbook: bytes) -> PreparedDataVisualization:
        dataset = load_traupixe_dataset(workbook)
        content = json.dumps(
            serialize_traupixe_for_model(dataset),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return PreparedDataVisualization(
            filename=DATASET_FILENAME,
            content=content,
            descriptor=dataset.descriptor(),
            calculation_instructions=CALCULATION_INSTRUCTIONS,
            visualization_instructions=VISUALIZATION_INSTRUCTIONS,
        )

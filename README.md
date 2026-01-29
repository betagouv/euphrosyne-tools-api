# Euphrosyne Tools API

Un micro service dédié à la gestion de Machines Virtuelles sur Azure et des connections (RDP) via [Apache Guacamole](https://guacamole.apache.org/).

## Développement

Ce projet utilise [FastAPI](https://fastapi.tiangolo.com/).

`uvicorn main:app --reload`

## Configuration des variables d'environnement

| Nom de la variable                     | Description                                                                                                                                                                                        | Requis |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| PROJECT_STORAGE_BACKEND                | Optionnel. Backend de stockage des données projets. Valeurs : `azure_fileshare` (défaut) ou `azure_blob`.                                                                                           |
| AZURE_SUBSCRIPTION_ID                  | ID de la souscription. Azure                                                                                                                                                                       |
| AZURE_CLIENT_ID                        | ID de l'application Azure (voir section _Générer les clés pour s'authentifier auprès d'Azure_).                                                                                                    |
| AZURE_CLIENT_SECRET                    | Secret de l'application Azure (voir section _Générer les clés pour s'authentifier auprès d'Azure_).                                                                                                |
| AZURE_TENANT_ID                        | Tenant de l'application Azure (voir section _Générer les clés pour s'authentifier auprès d'Azure_).                                                                                                |
| AZURE_RESOURCE_GROUP_NAME              | Nom du _Resource group_ où sont regroupées les ressources sur le compte Azure.                                                                                                                     |
| AZURE_TEMPLATE_SPECS_NAME              | Nom du _Template specs_ utilisé pour déployer les machines virtuelles. Voir le projet `euphrosyne-tools-infra`.                                                                                    |
| AZURE_RESOURCE_PREFIX                  | Optionnel. Préfixe utilisé pour éviter les collisions de nom lors de la création de ressources sur Azure. Doit être le même que dans la configuration Terraform (projet `euphrosyne-tools-infra`). |
| AZURE_STORAGE_ACCOUNT                  | Nom du _Storage account_ Azure.                                                                                                                                                                    |
| AZURE_STORAGE_FILESHARE                | Nom du _Fileshare_ contenant les fichiers de données sur le _Storage account_ Azure. Requis si `PROJECT_STORAGE_BACKEND=azure_fileshare` (valeur par défaut).                                        |
| AZURE_STORAGE_PROJECTS_LOCATION_PREFIX | Optionnel. Prefixe lorsque le dossier contenant les fichiers de données sur le _Fileshare_ Azure n'est pas à la racine.                                                                            |
| AZURE_STORAGE_DATA_CONTAINER           | Nom du container Blob utilisé pour les données projets (requis si `PROJECT_STORAGE_BACKEND=azure_blob`).                                                                                           |
| AZURE_IMAGE_GALLERY                    | Nom de la _Azure compute gallery_ qui stock les différentes images                                                                                                                                 |
| AZURE_IMAGE_DEFINITION                 | Nom de la _VM image definition_ qui est l'image pré-configurée pour les VM Euphrosyne                                                                                                              |
| CORS_ALLOWED_ORIGIN                    | Origines des frontends autorisées à utiliser l'API. Séparer les origines par des espaces.                                                                                                          |
| GUACAMOLE_ROOT_URL                     | URL du service guacamole. Ajouter `/guacamole` à la fin si besoin.                                                                                                                                 |
| GUACAMOLE_ADMIN_USERNAME               | Nom d'un utilisateur qui peut gérer les connections sur le service Guacamole.                                                                                                                      |
| GUACAMOLE_ADMIN_PASSWORD               | Mot de passe de l'utilisateur Guacamole.                                                                                                                                                           |
| GUACAMOLE_SECRET_KEY                   | Clé secrète utilisée pour encrypter les mots de passe des utilisateurs créés à la volée.                                                                                                           |
| JWT_SECRET_KEY                         | Clé secrète utilisée pour lire les tokens JWT reçus depuis le backend `euphrosyne`. Doit être la même que l'application Django `Euphrosyne`.                                                       |
| VM_LOGIN                               | Nom d'utilisateur utilisé pour se connecter aux machines virtuelles.                                                                                                                               |
| VM_PASSWORD                            | Mot de passe utilisé pour se connecter aux machines virtuelles.                                                                                                                                    |

## Stockage des données projets

Les données projets peuvent être stockées soit dans un Fileshare Azure, soit dans un container Blob.

- **Fileshare (par défaut)** : `PROJECT_STORAGE_BACKEND=azure_fileshare` et `AZURE_STORAGE_FILESHARE` doit être renseigné.
- **Blob** : définir `PROJECT_STORAGE_BACKEND=azure_blob` et renseigner `AZURE_STORAGE_DATA_CONTAINER`.

Le préfixe `AZURE_STORAGE_PROJECTS_LOCATION_PREFIX` continue de s'appliquer (chemin de base des projets) pour les deux backends.

## Configurer le CORS (Blob / Fileshare)

Pour autoriser les frontends à accéder directement au stockage, utiliser les scripts suivants :

- **Blob** :
  ```bash
  python scripts/set_blob_cors.py "<origins>" <container_name>
  ```
  Exemple :
  ```bash
  python scripts/set_blob_cors.py "https://app.example.com,https://admin.example.com" project-myproject
  ```

- **Fileshare** :
  ```bash
  python scripts/set_file_share_cors.py "<origins>"
  ```
  Exemple :
  ```bash
  python scripts/set_file_share_cors.py "https://app.example.com,https://admin.example.com"
  ```

## Générer les clés pour s'authentifier auprès d'Azure

https://docs.microsoft.com/en-us/azure/developer/python/sdk/authentication-local-development-service-principal?tabs=azure-portal

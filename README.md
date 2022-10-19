# Euphrosyne Tools API

Un micro service dédié à la gestion de Machines Virtuelles sur Azure et des connections (RDP) via [Apache Guacamole](https://guacamole.apache.org/).

## Développement

Ce projet utilise [FastAPI](https://fastapi.tiangolo.com/).

`uvicorn main:app --reload`

## Configuration des variables d'environnement

| Nom de la variable                     | Description                                                                                                                                                                                        | Requis |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| AZURE_SUBSCRIPTION_ID                  | ID de la souscription. Azure                                                                                                                                                                       |
| AZURE_CLIENT_ID                        | ID de l'application Azure (voir section _Générer les clés pour s'authentifier auprès d'Azure_).                                                                                                    |
| AZURE_CLIENT_SECRET                    | Secret de l'application Azure (voir section _Générer les clés pour s'authentifier auprès d'Azure_).                                                                                                |
| AZURE_TENANT_ID                        | Tenant de l'application Azure (voir section _Générer les clés pour s'authentifier auprès d'Azure_).                                                                                                |
| AZURE_RESOURCE_GROUP_NAME              | Nom du _Resource group_ où sont regroupées les ressources sur le compte Azure.                                                                                                                     |
| AZURE_TEMPLATE_SPECS_NAME              | Nom du _Template specs_ utilisé pour déployer les machines virtuelles. Voir le projet `euphrosyne-tools-infra`.                                                                                    |
| AZURE_RESOURCE_PREFIX                  | Optionnel. Préfixe utilisé pour éviter les collisions de nom lors de la création de ressources sur Azure. Doit être le même que dans la configuration Terraform (projet `euphrosyne-tools-infra`). |
| AZURE_STORAGE_ACCOUNT                  | Nom du _Storage account_ Azure.                                                                                                                                                                    |
| AZURE_STORAGE_FILESHARE                | Nom du _Fileshare_ contenant les fichiers de données sur le _Storage account_ Azure.                                                                                                               |
| AZURE_STORAGE_PROJECTS_LOCATION_PREFIX | Optionnel. Prefixe lorsque le dossier contenant les fichiers de données sur le _Fileshare_ Azure n'est pas à la racine.                                                                            |
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

## Générer les clés pour s'authentifier auprès d'Azure

https://docs.microsoft.com/en-us/azure/developer/python/sdk/authentication-local-development-service-principal?tabs=azure-portal

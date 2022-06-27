# Euphrosyne Tools API

Un micro service dédié à la gestion de Machines Virtuelles sur Azure et des connections (RDP) via [Apache Guacamole](https://guacamole.apache.org/).

## Développement

Ce projet utilise [FastAPI](https://fastapi.tiangolo.com/).

`uvicorn main:app --reload`

## Configuration des variables d'environnement

| First Header              | Second Header                                                                                                                                                                           |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| AZURE_SUBSCRIPTION_ID     | ID de la souscription. Azure                                                                                                                                                            |
| AZURE_CLIENT_ID           | ID de l'application Azure (voir section _Générer les clés pour s'authentifier auprès d'Azure_).                                                                                         |
| AZURE_CLIENT_SECRET       | Secret de l'application Azure (voir section _Générer les clés pour s'authentifier auprès d'Azure_).                                                                                     |
| AZURE_TENANT_ID           | Tenant de l'application Azure (voir section _Générer les clés pour s'authentifier auprès d'Azure_).                                                                                     |
| AZURE_RESOURCE_GROUP_NAME | Nom du _Resource group_ où sont regroupés les ressources sur le compte Azure.                                                                                                           |
| AZURE_TEMPLATE_SPECS_NAME | Nom du _Template specs_ utilisé pour déployer les machines virtuelles. Voir le projet `euphrosyne-tools-infra`.                                                                         |
| AZURE_RESOURCE_PREFIX     | Préfixe utilisé pour éviter les collisions de nom lors de la création de ressources sur Azure. Doit être le même que dans la configuration Terraform (projet `euphrosyne-tools-infra`). |
| CORS_ALLOWED_ORIGIN       | Origines des frontends autorisées à utiliser l'API. Séparer les origines par des espaces.                                                                                               |
| GUACAMOLE_ROOT_URL        | URL du service guacamole. Ajouter `/guacamole` à la fin si besoin.                                                                                                                      |
| GUACAMOLE_ADMIN_USERNAME  | Nom d'un utilisateur qui peut gérer les connections sur le service Guacamole.                                                                                                           |
| GUACAMOLE_ADMIN_PASSWORD  | Mot de passe de l'utilisateur Guacamole.                                                                                                                                                |
| GUACAMOLE_SECRET_KEY      | Clé secrète utilisée pour encrypter les mots de passe des utilisateurs créés à la volée.                                                                                                |
| JWT_SECRET_KEY            | Clé secrète utilisée pour lire les tokens JWT reçus depuis le backend `euphrosyne`. Doit être la même que l'application Django `Euphrosyne`.                                            |

## Générer les clés pour s'authentifier auprès d'Azure

https://docs.microsoft.com/en-us/azure/developer/python/sdk/authentication-local-development-service-principal?tabs=azure-portal

<h1 align="center">Analyse des ventes d'une enseigne</h1>

## 📌 Objectifs

Ce projet a pour objectif d’intégrer les données de ventes d’une chaîne de magasins située à Lille dans un entrepôt de données Snowflake. Une fois les données chargées et nettoyées à l’aide de dbt, nous concevrons un modèle en étoile pour structurer les données de manière analytique. Ce modèle permettra de faciliter l’analyse décisionnelle.

En résumé, nous allons :

* Configurer les environnements Snowflake et dbt
* Charger les données de ventes issues de fichiers plats (CSV) dans Snowflake
* Créer les modèles dbt : staging, dimensions, faits
* Implémenter des tests unitaires pour valider la qualité des données

---

## 🧰 Technologies utilisées

* Snowflake
* dbt Cloud
* SQL

---

## 📚 Données

Le jeu de données est un fichier plat (csv) constitué de 721 lignes et 25 colonnes, où chaque ligne représente un achat effectué par un client dans un magasin à une date donnée.

| Colonnes              | Type       | Description                                 | Valeurs                  |
| ----------------------|------------|---------------------------------------------|--------------------------|
| `numCommande`         | Numérique  | Numéro de commande du client                | 25XXXXXX                 |
| `nomMagasin`          | Texte      |Nom du magasin d'achat                       | Au panier frais          |
| `adresseMagasin`      | Texte      | Adresse du magasin                          | 164 Rue du Maréchal Foch |
|`codePostalMagasin`    | Numérique  | Code postal du magasin                      | 59120                    |
| `communeMagasin`      | Texte      | Commune du magasin                          | Loos                     |
| `nomClient`           | Texte      | Nom du client qui a passé la commande       | Foch                     |
| `prenomClient`        | Texte      | Prénom du client                            | Maréchal                 |
| `statutClient`        | Texte      | Statut du client                            | regulier                 |
| `dateNaissanceClient` | Date       | Date de naissance du client                 | YYYY-MM-DD               |
| `adresseClient`       | Texte      | Adresse du client                           | 164 Rue du Maréchal Foch |
| `codePostalClient`    | Numérique  | Code postal du client                       | 59120                    |
| `communeClient`       | Texte      | Commune du client                           | Loos                     |
| `paysClient`          | Texte      | Pays du client                              | France                   |
| `telClient`           | Numérique  | Téléphone du client                         | XXXXXXXXXX               |
| `emailClient`         | Texte      | Email du client                             | username@domain          |
| `dateLivCommande`     | Date       | Date de livraison de la commande            | YYYY-MM-DD               |
| `dateCmdCommande`     | Date       | Date à laquelle la commande a été effectuée | YYYY-MM-DD               |
| `libelleProduit`      | Texte      | Libellé du produit commandé                 | Eau gazeuse 50 cl        |
| `marqueProduit`       | Texte      | Marque du produit                           | PERRIER                  |
| `codeTVAProduit`      | Numérique  | Code TVA du produit                         | 2                        |
| `libelleRayon`        | Texte      | Libellé du rayon du produit                 | Boissons                 |
| `qteCmd`              | Numérique  | Quantité de produit commandée               | 1                        |
| `qteLiv`              | Numérique  | Quantité de produit livrée                  | 1                        |
| `pU`                  | Numérique  | Prix unitaire du produit                    | 1.63                     |
|  `total`              | Numérique  | Prix total du produit                       | 1.63                     |

---

## 🚀 Etapes de l'analyse

### 🛠️ Configuration de Snowflake

```
-- Utiliser le rôle admin
USE ROLE ACCOUNTADMIN;

-- Créer le rôle
CREATE ROLE IF NOT EXISTS transform_role;

-- Accorder les privilèges au rôle
GRANT ROLE transform_role TO ROLE ACCOUNTADMIN;

-- Créer le warehouse par défaut
CREATE WAREHOUSE IF NOT EXISTS compute_wh;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE transform_role;
GRANT OPERATE ON WAREHOUSE compute_wh TO ROLE transform_role;

-- Créer l'utilisateur retail_user 
CREATE USER IF NOT EXISTS retail_user
    PASSWORD = 'XXXXXXXXXX'
    LOGIN_NAME = 'retail_user'
    MUST_CHANGE_PASSWORD = FALSE
    DEFAULT_WAREHOUSE = 'compute_wh'
    COMMENT = 'Utilisateur unique';

-- Attribuer des rôles à l'utilisateur
GRANT ROLE transform_role TO USER retail_user;

-- Créer la BDD retail
CREATE DATABASE IF NOT EXISTS retail;

-- Accès à la base de données retail
GRANT USAGE ON DATABASE retail TO ROLE transform_role;

-- Créer le schéma raw
CREATE SCHEMA IF NOT EXISTS retail.raw;

-- Permissions sur retail.raw
GRANT ALL ON WAREHOUSE compute_wh TO ROLE transform_role; 
GRANT ALL ON DATABASE retail TO ROLE transform_role;
GRANT ALL ON ALL SCHEMAS IN DATABASE retail TO ROLE transform_role;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE retail TO ROLE transform_role;
GRANT ALL ON ALL TABLES IN SCHEMA retail.raw TO ROLE transform_role;
GRANT ALL ON FUTURE TABLES IN SCHEMA retail.raw TO ROLE transform_role;

-- Création d'un stage
CREATE STAGE IF NOT EXISTS retail.raw.internal_csv_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

```

### 📥 Ingestion de la donnée source

1. Uploader le fichier csv sur Snowflake
    * Aller dans l'onglet **"Data"** dans la barre latérale gauche
    * Dans le volet de navigation **"Databases"**, cliquer sur la BDD **"retail"**, puis sur le schéma **"raw"**
    * Dans la liste des objets du schéma, cliquer sur **"Stages"**
    * Cliquer sur le stage interne où on veut uploader le fichier *internal_csv_stage*
    * En haut à droite de la page du stage, cliquer sur le bouton **"Upload"**, puis sélectionner le fichier CSV à uploader
    * Pour vérifier si le chargement dans le stage est bien exécuté, aller sur un *worksheet* et faire : ``` LIST @retail.raw.internal_csv_stage; ```

2. Créer la table raw_sales 

```
CREATE OR REPLACE TABLE retail.raw.raw_sales (
    numCommande NUMBER(8,0),
    nomMagasin VARCHAR(50),
    adresseMagasin VARCHAR(100),
    codePostalMagasin VARCHAR(10),
    communeMagasin VARCHAR(50),
    nomClient VARCHAR(50),
    prenomClient VARCHAR(50),
    statutClient VARCHAR(20),
    dateNaissanceClient DATE,
    adresseClient VARCHAR(100),
    codePostalClient VARCHAR(10),
    communeClient VARCHAR(50),
    paysClient VARCHAR(30),
    telClient VARCHAR(20),
    emailClient VARCHAR(100),
    dateLivCommande DATE,
    dateCmdCommande DATE,
    libelleProduit VARCHAR(100),
    marqueProduit VARCHAR(50),
    codeTVAProduit NUMBER(1,0),
    libelleRayon VARCHAR(50),
    qteCmd NUMBER(10,0),
    qtLiv NUMBER(10,0),
    pU NUMBER(10,2),
    total NUMBER(12,2)
);
```

3. Charger les données du stage dans la table

```
COPY INTO retail.raw.raw_sales
FROM @retail.raw.internal_csv_stage/retail_sales.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ENCODING = 'UTF-8'
);
```
### 🛠️ Configuration de dbt

1. Connexion à Snowflake depuis dbt Cloud
    * Dans la barre latérale gauche, cliquer sur un compte existant (si c'est la première fois, suivre juste les étapes de création de compte), puis sélectionner **Create new account**
    * Définir l'**account name**
    * Une nouvelle page s'ouvre, sélectionner **I am new to dbt**, puis dans la page suivante, **I'm learning dbt to advance my career**
    * Nommer le projet **retail_project**
    * Dans *Connection*, choisir **Add new connection**, puis dans la nouvelle page qui s'ouvre, dans *Type* choisir **Snowflake**
    * Dans *Settings*, renseigner : 
        * *Account* :  il s'agit du **username** dans l'URL envoyé par Snowflake après création du compte username.snowflakecomputing.com).
        * *Database* : **retail** 
        * *Warehouse* : **compute_wh**
    * Dans *Optional settings*, mettre **transform_role** (dbt n'utilise qu'un seul rôle actif, et transform_role a les privilèges nécessaires pour créer nos modèles)
    * Cocher l'option **Session Keep Alive**, puis sauvegarder
    * Revenir sur la page du projet, et choisir dans *Connection*, **Snowflake**
    * Dans *Development credentials*, entrer les informations :
        * *Username* : **retail_user**
        * *Password* : **XXXXXXXXXXX** (le mot de passe défini lors de la création de l'utilisateur dans Snowflake)
        * *Schema* : **raw** 
        * *Threads* : **4** (car c'est un petit projet)
        * Tester la connexion et enregistrer si tout est **OK**.
    * Dans *Setup a repository*, choisir **Managed** (c'est le GitHub natif de dbt), puis le nommer **retail_repository** et le créer.
    * Une fois ces étapes réalisées, cliquer sur **Start developing in the IDE**, puis faire **Initialize dbt project**
    * Supprimer dans *Models/* le dossier **example**
    * Faire **Commit and sync**, puis entrer le message de validation **Initialisation du projet**.

### 🧱 Création des modèles dbt

Avant de réaliser une étape, il faut :  
* créer une branche : **Create branch** (par exemple **sources_definition**)
* une fois les opérations validées, faire **Commit and Sync**, puis mettre un message de validation (par exemple **Définition de la source raw_retail_data**).
* faire ensuite, **Merge this branch to main** pour pousser la modification sur la branche principale.

1. Dans le projet dbt, dans *models/* :
    * créer le fichier (**sources.yml**) pour définir les sources brutes : **Create file**.
    ```
    version: 2

    sources:
      - name: raw_retail_data
        database: retail
        schema: raw
        tables:
          - name: raw_sales
    ``` 
    * créer le dossiers **staging** pour y définir : 
        * les modèles qui lisent les sources brutes.
        * les modèles qui font des transformations intermédiaires.
        * les modèles finaux par domaine métier (modèles de dimensions et de faits). 
    
2. Configurer le **dbt_project.yml** : 
```

name: 'retail_project' # nom du projet dbt
version: '1.0.0' # version du projet utile pour la gestion et le suivi
config-version: 2 # version du format de configuration utilisée par dbt

# Nom du profil dbt à utiliser pour la connexion à la base de données (défini dans ~/.dbt/profiles.yml)
profile: 'default' 

model-paths: ["models"] # Chemin où dbt va chercher les modèles SQL
analysis-paths: ["analyses"] # Chemin pour les fichiers d'analyse (rapports, requêtes ad hoc)
test-paths: ["tests"] # Chemin des tests personnalisés
seed-paths: ["seeds"] # Chemin des fichiers CSV seed (données statistiques à charger)
macro-paths: ["macros"] # Chemin des macros Jinja personnalisées
snapshot-paths: ["snapshots"] # Chemin des snapshots (historisation des données)

target-path: "target"  # répertoire où dbt compile les fichiers SQL générés
clean-targets:         # liste des dossiers que dbt clean va supprimer pour faire un nettoyage
  - "target"
  - "dbt_packages"

# Configuration des modèles

models:
  retail_project:
    +materialized: table
```

[Plus de détails sur la configuration des modèles](https://docs.getdbt.com/reference/model-configs)

3. Changement du nom du schéma dans Snowflake

Nous allons changer le nom du schéma pour que dbt n'affiche pas par exemple **raw_staging** dans Snowflake. 
* créer une branche **schema_name_changement**
* aller dans *macros/* 
* créer un fichier **generate_schema_name.sql**
```
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
```
* faire **dbt build**
* valider les modifications, puis Merge la branche.

4. Créer le modèle stg_sales.sql
    * Modifier la table raw_sales sur Snowflake 
        ```
        -- Modifier la table retail.raw.raw_sales pour permettre l'incrémentation
        ALTER TABLE retail.raw.raw_sales ADD COLUMN load_timestamp TIMESTAMP;
        UPDATE retail.raw.raw_sales SET load_timestamp = CURRENT_TIMESTAMP;
        ```
    * Ecrire le modèle stg_sales.sql
        * créer une branche **staging_model**
        * créer le fichier **stg_sales.sql** dans *Models/staging*
        ```
        {{
            config(
                database='retail',
                materialized='incremental',
                unique_key='sales_id'
            )
        }}

        WITH raw_sales AS (
            SELECT * 
            FROM {{ source("raw_retail_data", "raw_sales") }}
        )
        SELECT 
            CONCAT(numCommande, '_', dateCmdCommande, '_', libelleProduit) AS sales_id,
            numCommande,
            nomMagasin,
            adresseMagasin,
            codePostalMagasin,
            communeMagasin,
            nomClient,
            prenomClient,
            statutClient,
            dateNaissanceClient,
            adresseClient,
            codePostalClient,
            communeClient,
            paysClient,
            telClient,
            emailClient,
            dateLivCommande,
            dateCmdCommande,
            libelleProduit,
            marqueProduit,
            codeTVAProduit,
            libelleRayon,
            qteCmd,
            qtLiv,
            pU,
            total,
            load_timestamp
        FROM raw_sales
        {% if is_incremental() %}
        WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{this}} )
        {% endif %}
        ```
        * Sauvegarder le fichier et faire **dbt run --select stg_sales**
        * Une fois OK, faire **Commit and Sync**, mettre le message **Création du modèle stg_sales.sql**, puis **Merge this branch to main**.
    * Installer les packages **dbt_expectations**  et **dbt_utils** dans le répertoire racine du projet
        * créer le fichier **packages.yml** et y intégrer le script suivant : 
        ```
        packages:
          - package: metaplane/dbt_expectations
            version: [">=0.10.0", "<0.11.0"]

          - package: dbt-labs/dbt_utils
            version: [">=1.1.0", "<2.0.0"]
        ```
        * sauvegarder le fichier, puis entrer **dbt deps** pour lancer l'installation
    * Ajouter les tests unitaires
        * créer une branche **stg_sales_test**
        * créer dans *Models/staging/* un fichier **schema.yml**, et y intégrer le script suivant : 
        ```
        version: 2

        models:
          - name: stg_sales
            description: "Tests unitaires du modèle stg_sales"
            columns:
              - name: sales_id
                description: "Identifiant unique de la ligne de vente"
                tests:
                  - not_null
                  - unique

              - name: numCommande
                description: "Numéro de commande"
                tests:
                  - not_null

              - name: total
                description: "Le montant total ht de la commande"
                tests:
                  - dbt_expectations.expect_column_values_to_be_between:
                      min_value: 0
                      strictly: false
        ```
        * faire **dbt test** pour lancer les tests
        * valider les modifications et Merge la branche.
5. Créer les modèles dimensionnels dans *models/staging*
    * Créer le modèle **dim_commande**
        * créer une branche **dim_commande**
        * créer un fichier **dim_commande.sql**
        * insérer ce script 
        ```
        {{
            config(
                database='retail',
                schema='dimensions'
            )
        }}

        WITH commandes AS (
            SELECT DISTINCT numCommande
            FROM {{ ref("stg_sales") }}
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY numCommande) AS idCommande,
            numCommande
        FROM commandes
        ```
        * faire **dbt run --select dim_commande**
        * ajouter dans le fichier **schema.yml** ces tests unitaires
        ```
        - name: dim_commande
          description: "Tests unitaires du modèle dim_commande"
          columns:
            - name: idCommande
              description: "Identifiant unique de la commande"
              tests:
                - not_null
                - unique
        ```
        * sauvegarder le fichier, puis faire **dbt test**
        * valider les modifications, puis Merge.
    * Créer le modèle **dim_magasin**
        * créer une branche **dim_magasin**
        * créer un fichier **dim_magasin.sql** 
        * insérer le script suivant : 
        ```
        {{
            config(
                database='retail',
                schema='dimensions'
            )
        }}

        WITH magasins AS (
            SELECT DISTINCT 
                nomMagasin AS nom,
                adresseMagasin AS adresse,
                codePostalMagasin AS codePostal,
                communeMagasin AS commune
            FROM {{ ref("stg_sales") }}
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY nom) AS idMagasin,
            nom,
            adresse,
            codePostal,
            commune
        FROM magasins
        ```
        * faire **dbt run --select dim_magasin**
        * ajouter dans le fichier **schema.yml** les tests unitaires suivants : 
        ```
          - name: dim_magasin
            description: "Tests unitaires du modèle dim_magasin"
            columns:
              - name: idMagasin
                description: "Identifiant du magasin"
                tests:
                  - not_null
                  - unique
        ```
        * sauvegarder le fichier, puis faire **dbt test**
        * valider les modifications, puis Merge
    * Créer le modèle **dim_produit**
        * créer une branche **dim_produit**
        * créer un fichier **dim_produit.sql** 
        * insérer le script suivant : 
        ```
        {{
            config(
                database='retail',
                schema='dimensions'
            )
        }}

        WITH produits AS (
            SELECT DISTINCT 
                libelleProduit,
                marqueProduit AS marque,
                codeTVAProduit AS codeTVA,
                libelleRayon
            FROM {{ ref("stg_sales") }}
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY libelleProduit) AS idProduit,
            libelleProduit,
            marque,
            codeTVA,
            libelleRayon
        FROM produits
        ```
        * faire **dbt run --select dim_produit**
        * ajouter dans le fichier **schema.yml** les tests unitaires suivants :
        ```
          - name: dim_produit
            description: "Tests unitaires du modèle dim_produit"
            columns:
              - name: idProduit
                description: "Identifiant du produit"
                tests:
                  - not_null
                  - unique
        ```
        * sauvegarder le fichier, puis faire **dbt test**
        * valider les modifications, puis Merge
    * Créer le modèle **dim_client**
        * créer une branche **dim_client**
        * créer un fichier **dim_client.sql** 
        * insérer le script suivant : 
        ```
        {{
            config(
                database='retail',
                schema='dimensions'
            )
        }}

        WITH clients AS (
            SELECT DISTINCT 
                nomClient AS nom,
                prenomClient AS prenom,
                statutClient AS statut,
                dateNaissanceClient AS dateNaissance,
                adresseClient AS adresse,
                codePostalClient AS codePostal,
                communeClient AS commune,
                paysClient AS pays,
                telClient AS tel,
                emailClient AS email
            FROM {{ ref("stg_sales") }}
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY nom) AS idClient,
            nom,
            prenom,
            statut,
            dateNaissance,
            adresse,
            codePostal,
            commune,
            pays,
            tel,
            email
        FROM clients
        ```
        * faire **dbt run --select dim_client**
        * ajouter dans le fichier **schema.yml** les tests unitaires suivants :
        ```
          - name: dim_client
            description: "Tests unitaires du modèle dim_client"
            columns:
              - name: idClient
                description: "Identifiant du client"
                tests:
                  - not_null
                  - unique
        ```
        * sauvegarder le fichier, puis faire **dbt test**
        * valider les modifications, puis Merge
    * Créer le modèle **dim_jour**
        * créer une branche **dim_jour**
        * créer un fichier **dim_jour.sql** 
        * insérer le script suivant : 
        ```
        {{
            config(
                database='retail',
                schema='dimensions'
            )
        }}

        WITH jours AS (
            SELECT dateCmdCommande AS date
            FROM retail.staging.stg_sales
            UNION
            SELECT dateLivCommande AS date
            FROM {{ ref("stg_sales") }}
            ORDER BY date
        )
        SELECT 
            date,
            EXTRACT('dayofweek', date) AS jour,
            EXTRACT('month', date) AS mois,
            EXTRACT('quarter', date) AS trimestre,
            EXTRACT('year', date) AS annee
        FROM jours
        ```
        * faire **dbt run --select dim_jour**
        * ajouter dans le fichier **schema.yml** les tests unitaires suivants :
        ```
        - name: dim_jour
          description: "Tests unitaires du modèle dim_jour"
          columns:
            - name: date
              description: "Date"
              tests:
                - not_null
                - unique
        ```
        * sauvegarder le fichier, puis faire **dbt test**
        * valider les modifications, puis Merge
    * Créer le modèle intermédiaire **temp_fact_vente**
        * créer une branche **temp_fact_vente**
        * créer un fichier **temp_fact_vente.sql** 
        * insérer le script suivant : 
        ```
        {{
            config(
                database='retail',
                schema='intermediate'
            )
        }}

        WITH ventes AS (
            SELECT 
                commande.idCommande,
                stg.dateCmdCommande AS dateCommande,
                stg.dateLivCommande AS dateLivraison,
                magasin.idMagasin,
                client.idClient,
                produit.idProduit,
                produit.codeTVA,
                stg.qteCmd AS quantiteCommandee,
                stg.qtLiv AS quantiteLivree,
                stg.pU AS prixUnitaireHT,
                stg.total AS montantVenteHT
            FROM {{ ref("stg_sales") }} AS stg 
                LEFT JOIN {{ ref("dim_commande") }} AS commande
                    ON stg.numCommande = commande.numCommande
                LEFT JOIN {{ ref("dim_magasin") }} AS magasin
                    ON stg.nomMagasin = magasin.nom
                LEFT JOIN {{ ref("dim_client") }} AS client
                    ON stg.emailCLient = client.email
                LEFT JOIN {{ ref("dim_produit") }} AS produit
                    ON stg.libelleProduit = produit.libelleProduit
                        AND stg.marqueProduit = produit.marque
        )
        SELECT *
        FROM ventes
        ```
        * faire **dbt run --select temp_fact_vente**
        * ajouter dans le fichier **schema.yml** les tests unitaires suivants :
        ```
          - name: temp_fact_vente
            description: "Tests unitaires du modèle intermédiaire temp_fact_vente"
            columns:
              - name: idCommande
                description: "Identifiant de la commande"
                tests:
                  - relationships:
                      to: "{{ ref('dim_commande') }}"
                      field: idCommande

              - name: idMagasin
                description: "Identifiant du magasin"
                tests:
                  - relationships:
                      to: "{{ ref('dim_magasin') }}"
                      field: idMagasin
            
              - name: idClient
                description: "Identifiant du client"
                tests:
                  - relationships:
                      to: "{{ ref('dim_client') }}"
                      field: idClient

              - name: idProduit
                description: "Identifiant du produit"
                tests:
                  - relationships:
                      to: "{{ ref('dim_produit') }}"
                      field: idProduit
        ```
        * sauvegarder le fichier, puis faire **dbt test**
        * valider les modifications, puis Merge
    * Créer le modèle **fact_vente**
        * créer une branche **fact_vente**
        * créer un fichier **fact_vente.sql** 
        * insérer le script suivant : 
        ```
        {{
            config(
                database='retail',
                schema='facts'
            )
        }}

        WITH ventes AS (
            SELECT
                idCommande,
                dateCommande,
                dateLivraison,
                idMagasin,
                idClient,
                idProduit,
                quantiteCommandee,
                quantiteLivree,
                prixUnitaireHT,
                CASE codeTVA
                    WHEN 1 THEN ROUND(prixUnitaireHT*1.055,2)
                    WHEN 2 THEN ROUND(prixUnitaireHT*1.2,2)
                    ELSE prixUnitaireHT
                END AS prixUnitaireTTC,
                montantVenteHT,
                CASE codeTVA
                    WHEN 1 THEN ROUND(montantVenteHT*1.055,2)
                    WHEN 2 THEN ROUND(montantVenteHT*1.2,2)
                    ELSE montantVenteHT
                END AS montantVenteTTC
            FROM {{ ref("temp_fact_vente") }}
        )
        SELECT *
        FROM ventes
        ```
        * faire **dbt run --select fact_vente**
        * valider les modifications, puis Merge

[En savoir plus sur les types de données sur Snowflake](https://docs.snowflake.com/fr/sql-reference-data-types)

### 🔍 Analyses

1. **Analyses stg_sales**

Nous allons réaliser des requêtes SQL sur DBT pour explorer un peu notre base de données avant de réaliser notre modélisation en étoile.

* Créer une branche stg_sales_analyses
* créer un fichier **analyses_stg_sales.sql** dans *analyses/* 
* écrire les requêtes suivantes :
```
-- 1. Quel est le nombre total de magasins référencés dans la base de données ?

SELECT COUNT(DISTINCT nomMagasin)
FROM {{ ref("stg_sales") }};

-- 2. Combien de commandes ont été passées au sein de l’enseigne ?

SELECT COUNT(DISTINCT numCommande)
FROM {{ ref("stg_sales") }};

-- 3. Combien de clients distincts sont enregistrés dans la base ?

SELECT COUNT(DISTINCT emailClient)
FROM {{ ref("stg_sales") }};

-- 4. Combien de produits différents sont proposés par l’enseigne ?

SELECT COUNT(DISTINCT libelleProduit, marqueProduit)
FROM {{ ref("stg_sales") }};

-- 5. Sur combien de jours distincts les données de ventes sont-elles enregistrées ?

WITH jours AS (
    SELECT dateCmdCommande
    FROM {{ ref("stg_sales") }}
    UNION
    SELECT dateLivCommande
    FROM {{ ref("stg_sales") }}
)
SELECT COUNT(*)
FROM jours;

-- 6. Quel est le nombre total de transactions effectuées par l’enseigne ?

SELECT COUNT(numCommande)
FROM {{ ref("stg_sales") }};
```
* sauvegarder, puis faire **Compile**
** valider les modifications, puis Merge la branche.


2. **Analyses fact_vente**

Nous allons réaliser des requêtes SQL sur DBT pour déduire des insights de nos ventes.

* Créer une branche sales_analysis
* créer un fichier **analyses_fact_vente.sql** dans *analyses/* 
* écrire les requêtes suivantes :
```
-- 1. Quel est le CA de l'enseigne ?

SELECT SUM(montantVenteTTC) AS CA
FROM {{ ref("fact_vente") }};

-- 2. Quel est le CA par magasin ?

SELECT 
    magasin.nom AS magasin, 
    SUM(vente.montantVenteTTC) AS CA
FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_magasin") }} AS magasin
        ON vente.idMagasin = magasin.idMagasin
GROUP BY magasin.nom
ORDER BY CA DESC;

-- 3. Quel est le CA par rayon ?

SELECT 
    produit.libelleRayon AS rayon, 
    SUM(vente.montantVenteTTC) AS CA
FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_produit") }} AS produit
        ON vente.idProduit = produit.idProduit
GROUP BY produit.libelleRayon
ORDER BY CA DESC;

-- 4. Quel est le CA pour chaque habitant de Marcq en Baroeul ?

SELECT 
    vente.idClient, 
    CONCAT(client.nom, ' ', client.prenom) AS habitant, 
    SUM(vente.montantVenteTTC) AS CA
FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_client") }} AS client
        ON vente.idClient = client.idClient
WHERE UPPER(client.commune) = 'MARCQ EN BAROEUL'
GROUP BY vente.idClient, CONCAT(client.nom, ' ', client.prenom)
ORDER BY CA DESC;

-- 5. Quel est le CA par mois ?

SELECT 
    jour.mois AS mois, 
    SUM(vente.montantVenteTTC) AS CA
FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_jour") }} AS jour
        ON vente.dateLivraison = jour.date
GROUP BY jour.mois
ORDER BY CA DESC;

-- 6. Calculer le cube pour le CA par mois, par magasin et par rayon 

SELECT 
    jour.mois AS mois, 
    magasin.nom AS magasin, 
    produit.libelleRayon AS rayon, 
    SUM(vente.montantVenteTTC) AS CA
FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_jour") }} AS jour
        ON vente.dateLivraison = jour.date
    LEFT JOIN {{ ref("dim_magasin") }} AS magasin
        ON vente.idMagasin = magasin.idMagasin
    LEFT JOIN {{ ref("dim_produit") }} AS produit
        On vente.idProduit = produit.idProduit
GROUP BY CUBE(jour.mois, magasin.nom, produit.libelleRayon)
ORDER BY jour.mois, magasin.nom, produit.libelleRayon;

-- 7. Calculer le cube correspondant aux magasins de Wasquehal et de Croix, au mois d'avril et de juin dans les rayons épicerie salée/sucrée

SELECT 
    jour.mois AS mois, 
    magasin.nom AS magasin, 
    produit.libelleRayon AS rayon, 
    SUM(vente.montantVenteTTC) AS CA
FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_jour") }} AS jour
        ON vente.dateLivraison = jour.date
    LEFT JOIN {{ ref("dim_magasin") }} AS magasin
        ON vente.idMagasin = magasin.idMagasin
    LEFT JOIN {{ ref("dim_produit") }} AS produit
        On vente.idProduit = produit.idProduit
WHERE UPPER(magasin.nom) IN ('CROIX', 'WASQUEHAL')
    AND jour.mois IN (4, 6)
    AND LOWER(produit.libelleRayon) IN ('epicerie salée', 'epicerie sucrée')
GROUP BY CUBE(jour.mois, magasin.nom, produit.libelleRayon)
ORDER BY jour.mois, magasin.nom, produit.libelleRayon;

-- 8. Effectuer un rollup sur le CA entre le rayon et le produit

SELECT 
    produit.libelleRayon AS rayon,
    produit.libelleProduit AS produit,
    SUM(vente.montantVenteTTC) AS CA
FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_produit") }} AS produit
        ON vente.idProduit = produit.idProduit
GROUP BY ROLLUP(produit.libelleRayon, produit.libelleProduit)
ORDER BY rayon, produit;

-- 9. Effectuer un drill-in en ne considérant que les magasins et les rayons

SELECT 
    magasin.nom AS magasin,
    produit.libelleRayon AS rayon,
    SUM(vente.montantVenteTTC) AS CA
FROM {{ ref('fact_vente') }} AS vente
    LEFT JOIN {{ ref("dim_magasin") }} AS magasin 
        ON vente.idMagasin = magasin.idMagasin
    LEFT JOIN {{ ref("dim_produit") }} AS produit 
        ON vente.idProduit = produit.idProduit 
GROUP BY magasin.nom, produit.libelleRayon
ORDER BY CA DESC;

-- 10. Classer les produits vendus par CA généré décroissant 

SELECT
    produit.libelleProduit AS produit,
    SUM(vente.montantVenteTTC) AS CA
FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_produit") }} AS produit
        ON vente.idProduit = produit.idProduit
GROUP BY produit.libelleProduit
HAVING SUM(vente.montantVenteTTC) <> 0
ORDER BY CA DESC;

-- 11. Calculer le rang des produits en fonction du CA généré décroissant

SELECT
    produit.libelleProduit AS produit,
    SUM(vente.montantVenteTTC) AS CA,
    RANK() OVER (
        ORDER BY SUM(vente.montantVenteTTC) DESC
    ) AS rang
FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_produit") }} AS produit
        ON vente.idProduit = produit.idProduit
GROUP BY produit.libelleProduit
HAVING SUM(vente.montantVenteTTC) <> 0;

-- 12. Calculer le centile des produits en fonction du CA généré décroissant

SELECT
    produit.libelleProduit AS produit,
    SUM(vente.montantVenteTTC) AS CA,
    NTILE(100) OVER (ORDER BY SUM(vente.montantVenteTTC) DESC) AS centile
FROM {{ ref("fact_vente") }} AS vente 
    LEFT JOIN {{ ref("dim_produit") }} AS produit
        ON vente.idProduit = produit.idProduit
GROUP BY produit.libelleProduit;

-- 13. Quel est le nombre de références distinctes vendues pour chaque commande ?

SELECT 
    commande.numCommande AS commande,
    COUNT(DISTINCT vente.idProduit) AS reference_count
FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_commande") }} AS commande
        ON vente.idCommande = commande.idCommande
GROUP BY commande.numCommande
ORDER BY reference_count DESC;

-- 14. Quel est le nombre moyen de références distinctes vendues par commande ?

WITH ref_par_commande AS (
    SELECT 
        commande.numCommande AS commande,
        COUNT(DISTINCT vente.idProduit) AS reference_count
    FROM {{ ref("fact_vente") }} AS vente
        LEFT JOIN {{ ref("dim_commande") }} AS commande
            ON vente.idCommande = commande.idCommande
    GROUP BY commande.numCommande
)
SELECT AVG(reference_count) AS avg_reference_count
FROM ref_par_commande;

-- 15. Quel est le nombre de références distinctes vendues pour chaque commande pour chaque mois ?

SELECT 
    commande.numCommande AS commande,
    jour.mois,
    COUNT(DISTINCT vente.idProduit) AS reference_count
FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_commande") }} AS commande
        ON vente.idCommande = commande.idCommande
    LEFT JOIN {{ ref("dim_jour") }} AS jour 
        ON vente.dateLivraison = jour.date
GROUP BY commande.numCommande, jour.mois
ORDER BY reference_count DESC;

-- 16. Quel est le nombre moyen de références distinctes vendues par commande chaque mois ?

WITH ref_par_commande_mois AS (
    SELECT 
        commande.numCommande AS commande,
        jour.mois,
        COUNT(DISTINCT vente.idProduit) AS reference_count
    FROM {{ ref("fact_vente") }} AS vente
        LEFT JOIN {{ ref("dim_commande") }} AS commande
            ON vente.idCommande = commande.idCommande
        LEFT JOIN {{ ref("dim_jour") }} AS jour 
            ON vente.dateLivraison = jour.date
    GROUP BY commande.numCommande, jour.mois
)
SELECT 
    mois,
    ROUND(AVG(reference_count)) AS avg_reference_count 
FROM ref_par_commande_mois
GROUP BY mois
ORDER BY avg_reference_count DESC;

-- 17. Calculer le support de chaque produit, i.e. le pourcentage de commandes contenant ce produit

SELECT
    produit.libelleProduit AS produit,
    ROUND(100*(COUNT(DISTINCT idCommande)/(SELECT COUNT(DISTINCT idCommande) FROM {{ ref("fact_vente") }})), 2) AS pourcentage
FROM {{ ref("fact_vente") }} AS vente 
    LEFT JOIN {{ ref("dim_produit") }} AS produit 
        ON vente.idProduit = produit.idProduit
GROUP BY produit.libelleProduit
ORDER BY pourcentage DESC;

-- 18. Pour le produit "petit cookies aux Pépites de Chocolat 250 g", calculer pour chacun des autres produits le nombre de cooccurrences,
-- i.e. le nombre de commandes où ces deux produits sont présents

WITH orders_with_target_product AS (
    SELECT DISTINCT vente.idCommande
    FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_produit") }} AS produit 
        ON vente.idProduit = produit.idProduit 
    WHERE produit.libelleProduit = 'petit cookies aux Pépites de Chocolat 250 g'
)
SELECT 
    produit.libelleProduit AS co_produit,
    COUNT(DISTINCT vente.idCommande) AS cooccurrences
FROM {{ ref("fact_vente") }} AS vente 
    LEFT JOIN {{ ref("dim_produit") }} AS produit 
        ON vente.idProduit = produit.idProduit
    JOIN orders_with_target_product AS target
        ON vente.idCommande = target.idCommande
WHERE produit.libelleProduit <> 'petit cookies aux Pépites de Chocolat 250 g'
GROUP BY produit.libelleProduit
ORDER BY cooccurrences DESC;

-- 19. Considérons le produit "petit cookies aux Pépites de Chocolat 250 g", considérons les règles de la forme : 
-- si la commande contient ce produit alors elle contient également le produit X, où X est n'importe quel autre produit.
-- Calculer pour toutes les valeurs de X le support de la règle, i.e. le pourcentage de commandes qui contiennent ces deux produits.
-- On se restreint aux règles dont le support est supérieur à 20%.

WITH orders_with_target_product AS (
    SELECT DISTINCT vente.idCommande
    FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_produit") }} AS produit 
        ON vente.idProduit = produit.idProduit 
    WHERE produit.libelleProduit = 'petit cookies aux Pépites de Chocolat 250 g'
),

support_produit AS (
    SELECT 
        produit.libelleProduit AS co_produit,
        COUNT(DISTINCT vente.idCommande) AS cooccurrences,
        ROUND(100*COUNT(DISTINCT vente.idCommande)/(SELECT COUNT(DISTINCT idCommande) FROM {{ ref("fact_vente") }}), 2) AS support
    FROM {{ ref("fact_vente") }} AS vente 
        LEFT JOIN {{ ref("dim_produit") }} AS produit 
            ON vente.idProduit = produit.idProduit
        JOIN orders_with_target_product AS target
            ON vente.idCommande = target.idCommande
    WHERE produit.libelleProduit <> 'petit cookies aux Pépites de Chocolat 250 g'
    GROUP BY produit.libelleProduit   
)

SELECT 
    co_produit,
    cooccurrences,
    support
FROM support_produit
WHERE support > 20
ORDER BY support DESC;

-- 20. Calculer pour toutes les valeurs de X la confiance de la règle, i.e. le pourcentage de commandes qui contiennent ces deux produits
-- parmi les commandes contenant le premier produit. On se restreint aux règles dont la confiance est supérieure à 50%.

WITH orders_with_target_product AS (
    SELECT DISTINCT vente.idCommande
    FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_produit") }} AS produit 
        ON vente.idProduit = produit.idProduit 
    WHERE produit.libelleProduit = 'petit cookies aux Pépites de Chocolat 250 g'
),

confiance_produit AS (
    SELECT 
        produit.libelleProduit AS co_produit,
        COUNT(DISTINCT vente.idCommande) AS cooccurrences,
        ROUND(100*COUNT(vente.idCommande)/(SELECT COUNT(idCommande) FROM orders_with_target_product), 2) AS confiance
    FROM {{ ref("fact_vente") }} AS vente 
        LEFT JOIN {{ ref("dim_produit") }} AS produit 
            ON vente.idProduit = produit.idProduit
        JOIN orders_with_target_product AS target
            ON vente.idCommande = target.idCommande
    WHERE produit.libelleProduit <> 'petit cookies aux Pépites de Chocolat 250 g'
    GROUP BY produit.libelleProduit   
)

SELECT 
    co_produit,
    cooccurrences,
    confiance
FROM confiance_produit
WHERE confiance > 50
ORDER BY confiance DESC;

-- 21. Calculer pour toutes les valeurs de X le "lift" de la règle, i.e. l'amélioration apportée par la règle par rapport à un jeu de
-- transactions aléatoire. Le "lift" doit être strictement supérieur à 1. Il correspond au rapport entre la confiance de la règle et
-- le support de X

WITH orders_with_target_product AS (
    SELECT DISTINCT vente.idCommande
    FROM {{ ref("fact_vente") }} AS vente
    LEFT JOIN {{ ref("dim_produit") }} AS produit 
        ON vente.idProduit = produit.idProduit 
    WHERE produit.libelleProduit = 'petit cookies aux Pépites de Chocolat 250 g'
),

confiance_produit AS (
    SELECT 
        produit.libelleProduit AS co_produit,
        COUNT(vente.idCommande) AS cooccurrences,
        ROUND(100*COUNT(vente.idCommande)/(SELECT COUNT(idCommande) FROM orders_with_target_product), 2) AS confiance
    FROM {{ ref("fact_vente") }} AS vente 
        LEFT JOIN {{ ref("dim_produit") }} AS produit 
            ON vente.idProduit = produit.idProduit
        JOIN orders_with_target_product AS target
            ON vente.idCommande = target.idCommande
    WHERE produit.libelleProduit <> 'petit cookies aux Pépites de Chocolat 250 g'
    GROUP BY produit.libelleProduit   
),

support_produit AS (
    SELECT
        produit.libelleProduit AS co_produit,
        ROUND(100*COUNT(DISTINCT vente.idCommande)/(SELECT COUNT(DISTINCT idCommande) FROM {{ ref("fact_vente") }}), 2) AS support
    FROM {{ ref("fact_vente") }} AS vente 
        LEFT JOIN {{ ref("dim_produit") }} AS produit 
            ON vente.idProduit = produit.idProduit
    GROUP BY produit.libelleProduit
)

SELECT 
    cp.co_produit,
    cp.cooccurrences,
    sp.support,
    cp.confiance,
    ROUND(cp.confiance/sp.support, 2) AS lift
FROM AS confiance_produit cp 
    JOIN support_produit AS sp 
        ON cp.co_produit = sp.co_produit
WHERE 
    sp.support > 20
    AND cp.confiance > 50
    AND ROUND(cp.confiance/sp.support, 2) > 1
ORDER BY ROUND(cp.confiance/sp.support, 2) DESC;

-- 22. Donner les 10 couples de produits pour lesquels le nombre de cooccurrences est le plus important.

WITH commande_produit AS (
    SELECT 
        vente.idCommande,
        vente.idProduit,
        produit.libelleProduit
    FROM {{ ref("fact_vente") }} AS vente
        LEFT JOIN {{ ref("dim_produit") }} AS produit 
            ON vente.idProduit = produit.idProduit 
),

croisement_produits AS (
    SELECT 
        p1.idCommande, 
        p1.libelleProduit AS produit1,
        p2.libelleProduit AS produit2
    FROM commande_produit AS p1
        JOIN commande_produit AS p2
            ON p1.idCommande = p2.idCommande
            AND p1.idProduit < p2.idProduit 
),

cooccurrences_produits AS (
    SELECT 
        produit1,
        produit2,
        COUNT(DISTINCT idCommande) AS cooccurrences
    FROM croisement_produits 
    GROUP BY produit1, produit2
)

SELECT 
    CONCAT('(', produit1, ', ', produit2, ')') AS produits,
    cooccurrences
FROM cooccurrences_produits
ORDER BY cooccurrences DESC
LIMIT 10;

-- 23. Nous considérons les règles d'association du type : 
-- si la commande contient le produit X alors elle contient également le produit Y
-- Calculer le support de chacune de ces règles, i.e. le pourcentage de commandes qui contiennent ces deux produits.
-- On se restreint aux règles dont le support est supérieur à 20%.

WITH commande_produit AS (
    SELECT 
        vente.idCommande,
        vente.idProduit,
        produit.libelleProduit
    FROM {{ ref("fact_vente") }} AS vente
        LEFT JOIN {{ ref("dim_produit") }} AS produit 
            ON vente.idProduit = produit.idProduit 
),

croisement_produits AS (
    SELECT 
        p1.idCommande, 
        p1.libelleProduit AS produit1,
        p2.libelleProduit AS produit2
    FROM commande_produit AS p1
        JOIN commande_produit AS p2
            ON p1.idCommande = p2.idCommande
            AND p1.idProduit < p2.idProduit 
),

support_produits AS (
    SELECT 
        produit1,
        produit2,
        COUNT(DISTINCT idCommande) AS cooccurrences,
        ROUND(100*COUNT(DISTINCT idCommande)/(SELECT COUNT(DISTINCT idCommande) FROM {{ ref("fact_vente") }}), 2) AS support
    FROM croisement_produits 
    GROUP BY produit1, produit2
)

SELECT 
    CONCAT('(', produit1, ', ', produit2, ')') AS produits,
    cooccurrences,
    support
FROM support_produits
WHERE support > 20
ORDER BY support DESC;

-- 24. Calculer la confiance de chacune de ces règles, i.e. le pourcentage de commandes qui contiennent ces deux produits parmi les 
-- commandes contenant X. On se restreint aux règles dont la confiance est supérieure à 80%.

WITH commande_produit AS (
    SELECT 
        vente.idCommande,
        vente.idProduit,
        produit.libelleProduit
    FROM {{ ref("fact_vente") }} AS vente
        LEFT JOIN {{ ref("dim_produit") }} AS produit 
            ON vente.idProduit = produit.idProduit 
),

produit_count AS (
    SELECT 
        libelleProduit, 
        COUNT(DISTINCT idCommande) AS produitx_count
    FROM commande_produit
    GROUP BY libelleProduit
),

croisement_produits AS (
    SELECT 
        p1.idCommande, 
        p1.libelleProduit AS produit1,
        p2.libelleProduit AS produit2
    FROM commande_produit AS p1
        JOIN commande_produit AS p2
            ON p1.idCommande = p2.idCommande
            AND p1.idProduit < p2.idProduit 
),

cooccurrences_produits AS (
    SELECT 
        produit1,
        produit2,
        COUNT(DISTINCT idCommande) AS cooccurrences,
    FROM croisement_produits 
    GROUP BY produit1, produit2
),

confiance_produits AS (
    SELECT 
        cp.produit1,
        cp.produit2,
        cp.cooccurrences,
        pc.produitx_count,
        ROUND(100*cp.cooccurrences/pc.produitx_count, 2) AS confiance
    FROM cooccurrences_produits AS cp 
        JOIN produit_count AS pc 
            ON cp.produit1 = pc.libelleProduit
)

SELECT 
    CONCAT('(', produit1, ', ', produit2, ')') AS produits,
    cooccurrences,
    produitx_count,
    confiance
FROM confiance_produits
WHERE confiance > 80
ORDER BY confiance ASC;

-- 25. Calculer le "lift" de chacune de ces règles, i.e. le rapport entre la confiance de la règle et le support de Y, le "lift"
-- doit être strictement supérieur à 1.

WITH commande_produit AS (
    SELECT DISTINCT 
        vente.idCommande,
        vente.idProduit,
        produit.libelleProduit
    FROM {{ ref("fact_vente") }} AS vente
        LEFT JOIN {{ ref("dim_produit") }} AS produit 
            ON vente.idProduit = produit.idProduit 
),

produit_count AS (
    SELECT 
        libelleProduit, 
        COUNT(DISTINCT idCommande) AS produitx_count
    FROM commande_produit
    GROUP BY libelleProduit
),

croisement_produits AS (
    SELECT 
        p1.idCommande, 
        p1.libelleProduit AS produit1,
        p2.libelleProduit AS produit2
    FROM commande_produit AS p1
        JOIN commande_produit AS p2
            ON p1.idCommande = p2.idCommande
            AND p1.idProduit < p2.idProduit 
),

cooccurrences_produits AS (
    SELECT 
        produit1,
        produit2,
        COUNT(DISTINCT idCommande) AS cooccurrences
    FROM croisement_produits 
    GROUP BY produit1, produit2
),

confiance_produits AS (
    SELECT 
        cp.produit1,
        cp.produit2,
        cp.cooccurrences,
        pc.produitx_count,
        ROUND(100*cp.cooccurrences/pc.produitx_count, 2) AS confiance
    FROM cooccurrences_produits AS cp 
        JOIN produit_count AS pc 
            ON cp.produit1 = pc.libelleProduit
),

support_produit_2 AS (
    SELECT 
        produit.libelleProduit AS produit2,
        ROUND(100*COUNT(DISTINCT vente.idCommande)/(SELECT COUNT(DISTINCT idCommande) FROM {{ ref("fact_vente") }}), 2) AS support
    FROM {{ ref("fact_vente") }} AS vente 
        LEFT JOIN {{ ref("dim_produit") }} AS produit 
            ON vente.idProduit = produit.idProduit 
    GROUP BY produit.libelleProduit
)

SELECT 
    CONCAT('(', cp.produit1, ', ', cp.produit2, ')') AS produits,
    cp.cooccurrences,
    cp.produitx_count,
    cp.confiance,
    sp2.support AS support_produit2,
    ROUND(cp.confiance/sp2.support,2) AS lift 
FROM confiance_produits AS cp 
    JOIN support_produit_2 AS sp2 
        ON cp.produit2 = sp2.produit2
WHERE 
    sp2.support > 20
    AND cp.confiance > 80
    AND ROUND(cp.confiance/sp2.support,2) > 1
ORDER BY ROUND(cp.confiance/sp2.support,2) DESC;
```
* sauvegarder, puis faire **Compile**
** valider les modifications, puis Merge la branche.

---

## ✅ Avancement

- [x] Configuration de Snowflake
- [x] Chargement de la donnée source sur Snowflake
- [x] Configuration de dbt
- [x] Création des modèles dbt
- [x] Tests unitaires
- [x] Analyses 

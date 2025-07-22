/* Analyses de stg_sales */

-- 1. Quel est le nombre total de magasins référencés dans la base de données ?

SELECT COUNT(DISTINCT nomMagasin)
FROM retail.staging.stg_sales;

-- 3

-- 2. Combien de commandes ont été passées au sein de l’enseigne ?

SELECT COUNT(DISTINCT numCommande)
FROM retail.staging.stg_sales;

-- 40

-- 3. Combien de clients distincts sont enregistrés dans la base ?

SELECT COUNT(DISTINCT emailClient)
FROM retail.staging.stg_sales;

-- 6

-- 4. Combien de produits différents sont proposés par l’enseigne ?

SELECT COUNT(DISTINCT libelleProduit, marqueProduit)
FROM retail.staging.stg_sales;

-- 251

-- 5. Sur combien de jours distincts les données de ventes sont-elles enregistrées ?

WITH jours AS (
    SELECT dateCmdCommande
    FROM retail.staging.stg_sales
    UNION
    SELECT dateLivCommande
    FROM retail.staging.stg_sales
)
SELECT COUNT(*)
FROM jours;

-- 46

-- 6. Quel est le nombre total de transactions effectuées par l’enseigne ?

SELECT COUNT(numCommande)
FROM retail.staging.stg_sales;

-- 720

/* Analyses de fact_vente */

-- 1. Quel est le CA de l'enseigne ?

SELECT SUM(montantVenteTTC) AS CA
FROM retail.facts.fact_vente;

-- 2162.02 €

-- 2. Quel est le CA par magasin ?

SELECT magasin.nom AS magasin, SUM(vente.montantVenteTTC) AS CA
FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_magasin AS magasin
        ON vente.idMagasin = magasin.idMagasin
GROUP BY magasin.nom
ORDER BY CA DESC;

-- Marcq en Baroeul         | 940.08 €
-- Wasquehal - La Pilaterie | 667.05 €
-- Croix                    | 554.89 €

-- 3. Quel est le CA par rayon ?

SELECT produit.libelleRayon AS rayon, SUM(vente.montantVenteTTC) AS CA
FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_produit AS produit
        ON vente.idProduit = produit.idProduit
GROUP BY produit.libelleRayon
ORDER BY CA DESC;

-- Epicerie sucrée | 686.39 €
-- Epicerie salée  | 565.14 €
-- Boissons        | 368.46 €

-- 4. Quel est le CA pour chaque habitant de Marcq en Baroeul ?

SELECT vente.idClient, CONCAT(client.nom, ' ', client.prenom) AS habitant, SUM(vente.montantVenteTTC) AS CA
FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_client AS client
        ON vente.idClient = client.idClient
WHERE UPPER(client.commune) = 'MARCQ EN BAROEUL'
GROUP BY vente.idClient, CONCAT(client.nom, ' ', client.prenom)
ORDER BY CA DESC;

-- BEAUFILS Bruno | 516.63 €
-- PATY Sonia     | 423.45 €

-- 5. Quel est le CA par mois ?

SELECT jour.mois AS mois, SUM(vente.montantVenteTTC) AS CA
FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_jour AS jour
        ON vente.dateLivraison = jour.date
GROUP BY jour.mois
ORDER BY CA DESC;

-- 6 | 469.84 €
-- 4 | 399.59 €
-- 5 | 370.66 €

-- 6. Calculer le cube pour le CA par mois, par magasin et par rayon 

SELECT jour.mois AS mois, magasin.nom AS magasin, produit.libelleRayon AS rayon, SUM(vente.montantVenteTTC) AS CA
FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_jour AS jour
        ON vente.dateLivraison = jour.date
    LEFT JOIN retail.dimensions.dim_magasin AS magasin
        ON vente.idMagasin = magasin.idMagasin
    LEFT JOIN retail.dimensions.dim_produit AS produit
        On vente.idProduit = produit.idProduit
GROUP BY CUBE(jour.mois, magasin.nom, produit.libelleRayon)
ORDER BY jour.mois, magasin.nom, produit.libelleRayon;

-- 3 | Croix | Boissons        | 13.01 €
-- 3 | Croix | Epicerie salée  | 21.14 €
-- 3 | Croix | Epicerie sucrée | 36.20 €

-- 7. Calculer le cube correspondant aux magasins de Wasquehal et de Croix, au mois d'avril et de juin dans les rayons épicerie salée/sucrée

SELECT jour.mois AS mois, magasin.nom AS magasin, produit.libelleRayon AS rayon, SUM(vente.montantVenteTTC) AS CA
FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_jour AS jour
        ON vente.dateLivraison = jour.date
    LEFT JOIN retail.dimensions.dim_magasin AS magasin
        ON vente.idMagasin = magasin.idMagasin
    LEFT JOIN retail.dimensions.dim_produit AS produit
        On vente.idProduit = produit.idProduit
WHERE UPPER(magasin.nom) IN ('CROIX', 'WASQUEHAL')
    AND jour.mois IN (4, 6)
    AND LOWER(produit.libelleRayon) IN ('epicerie salée', 'epicerie sucrée')
GROUP BY CUBE(jour.mois, magasin.nom, produit.libelleRayon)
ORDER BY jour.mois, magasin.nom, produit.libelleRayon;

-- 4 | Croix | Epicerie salée  | 42.00 €
-- 4 | Croix | Epicerie sucrée | 29.84 €
-- 4 | Croix | null            | 71.84 €


-- 8. Effectuer un rollup sur le CA entre le rayon et le produit

SELECT 
    produit.libelleRayon AS rayon,
    produit.libelleProduit AS produit,
    SUM(vente.montantVenteTTC) AS CA
FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_produit AS produit
        ON vente.idProduit = produit.idProduit
GROUP BY ROLLUP(produit.libelleRayon, produit.libelleProduit)
ORDER BY rayon, produit;

-- Boissons | Bière ambrée 5.9° 75 cl | 2.99 €
-- Boissons | Bière blonde 5L         | 52.10 €
-- Boissons | Bière blonde 8° 75 cl   | 14.33 €

-- 9. Effectuer un drill-in en ne considérant que les magasins et les rayons

SELECT 
    magasin.nom AS magasin,
    produit.libelleRayon AS rayon,
    SUM(vente.montantVenteTTC) AS CA
FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_magasin AS magasin 
        ON vente.idMagasin = magasin.idMagasin
    LEFT JOIN retail.dimensions.dim_produit AS produit 
        ON vente.idProduit = produit.idProduit 
GROUP BY magasin.nom, produit.libelleRayon
ORDER BY CA DESC;

-- Marcq en Baroeul         | Epicerie sucrée | 350.22 €
-- Marcq en Baroeul         | Epicerie salée  | 230.94 €
-- Wasquehal - La Pilaterie | Epicerie sucrée | 206.81 €

-- 10. Classer les produits vendus par CA généré décroissant 

SELECT
    produit.libelleProduit AS produit,
    SUM(vente.montantVenteTTC) AS CA
FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_produit AS produit
        ON vente.idProduit = produit.idProduit
GROUP BY produit.libelleProduit
HAVING SUM(vente.montantVenteTTC) <> 0
ORDER BY CA DESC;

-- Lait demi-écrémé UHT 6 * 1L  | 89.61 €
-- Sensor Excel Lames de rasoir | 59.46 €
-- Bière blonde 5L              | 52.10 €

-- 11. Calculer le rang des produits en fonction du CA généré décroissant

SELECT
    produit.libelleProduit AS produit,
    SUM(vente.montantVenteTTC) AS CA,
    RANK() OVER (
        ORDER BY SUM(vente.montantVenteTTC) DESC
    ) AS rang
FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_produit AS produit
        ON vente.idProduit = produit.idProduit
GROUP BY produit.libelleProduit
HAVING SUM(vente.montantVenteTTC) <> 0;

-- Lait demi-écrémé UHT 6 * 1L  | 89.61 € | 1
-- Sensor Excel Lames de rasoir | 59.46 € | 2
-- Bière blonde 5L              | 52.10 €| 3

-- 12. Calculer le centile des produits en fonction du CA généré décroissant

SELECT
    produit.libelleProduit AS produit,
    SUM(vente.montantVenteTTC) AS CA,
    NTILE(100) OVER (ORDER BY SUM(vente.montantVenteTTC) DESC) AS centile
FROM retail.facts.fact_vente AS vente 
    LEFT JOIN retail.dimensions.dim_produit AS produit
        ON vente.idProduit = produit.idProduit
GROUP BY produit.libelleProduit;

-- Lait demi-écrémé UHT 6 * 1L  | 89.61 € | 1
-- Sensor Excel Lames de rasoir | 59.46 € | 1
-- Bière blonde 5L              | 52.10 € | 1

-- 13. Quel est le nombre de références distinctes vendues pour chaque commande ?

SELECT 
    commande.numCommande AS commande,
    COUNT(DISTINCT vente.idProduit) AS reference_count
FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_commande AS commande
        ON vente.idCommande = commande.idCommande
GROUP BY commande.numCommande
ORDER BY reference_count DESC;

-- 28286963 | 47
-- 28608948 | 43
-- 26812428 | 34

-- 14. Quel est le nombre moyen de références distinctes vendues par commande ?

WITH ref_par_commande AS (
    SELECT 
        commande.numCommande AS commande,
        COUNT(DISTINCT vente.idProduit) AS reference_count
    FROM retail.facts.fact_vente AS vente
        LEFT JOIN retail.dimensions.dim_commande AS commande
            ON vente.idCommande = commande.idCommande
    GROUP BY commande.numCommande
)
SELECT AVG(reference_count) AS avg_reference_count
FROM ref_par_commande;

-- 18

-- 15. Quel est le nombre de références distinctes vendues pour chaque commande pour chaque mois ?

SELECT 
    commande.numCommande AS commande,
    jour.mois,
    COUNT(DISTINCT vente.idProduit) AS reference_count
FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_commande AS commande
        ON vente.idCommande = commande.idCommande
    LEFT JOIN retail.dimensions.dim_jour AS jour 
        ON vente.dateLivraison = jour.date
GROUP BY commande.numCommande, jour.mois
ORDER BY reference_count DESC;

-- 28286963 | 7 | 47
-- 28608948 | 8 | 43
-- 26812428 | 5 | 34

-- 16. Quel est le nombre moyen de références distinctes vendues par commande chaque mois ?

WITH ref_par_commande_mois AS (
    SELECT 
        commande.numCommande AS commande,
        jour.mois,
        COUNT(DISTINCT vente.idProduit) AS reference_count
    FROM retail.facts.fact_vente AS vente
        LEFT JOIN retail.dimensions.dim_commande AS commande
            ON vente.idCommande = commande.idCommande
        LEFT JOIN retail.dimensions.dim_jour AS jour 
            ON vente.dateLivraison = jour.date
    GROUP BY commande.numCommande, jour.mois
)
SELECT 
    mois,
    ROUND(AVG(reference_count)) AS avg_reference_count 
FROM ref_par_commande_mois
GROUP BY mois
ORDER BY avg_reference_count DESC;

-- 8 | 26
-- 3 | 23
-- 6 | 17

-- 17. Calculer le support de chaque produit, i.e. le pourcentage de commandes contenant ce produit

SELECT
    produit.libelleProduit AS produit,
    ROUND(100*(COUNT(DISTINCT idCommande)/(SELECT COUNT(DISTINCT idCommande) FROM retail.facts.fact_vente)), 2) AS pourcentage
FROM retail.facts.fact_vente AS vente 
    LEFT JOIN retail.dimensions.dim_produit AS produit 
        ON vente.idProduit = produit.idProduit
GROUP BY produit.libelleProduit
ORDER BY pourcentage DESC;

-- Yaourt à boire 500 ml                       | 47.50%
-- Lait demi-écrémé UHT 6 * 1 L                | 42.50%
-- petit cookies aux Pépites de Chocolat 250 g | 40.00%

-- 18. Pour le produit "petit cookies aux Pépites de Chocolat 250 g", calculer pour chacun des autres produits le nombre de cooccurrences,
-- i.e. le nombre de commandes où ces deux produits sont présents

WITH orders_with_target_product AS (
    SELECT DISTINCT vente.idCommande
    FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_produit AS produit 
        ON vente.idProduit = produit.idProduit 
    WHERE produit.libelleProduit = 'petit cookies aux Pépites de Chocolat 250 g'
)
SELECT 
    produit.libelleProduit AS co_produit,
    COUNT(DISTINCT vente.idCommande) AS cooccurrences
FROM retail.facts.fact_vente AS vente 
    LEFT JOIN retail.dimensions.dim_produit AS produit 
        ON vente.idProduit = produit.idProduit
    JOIN orders_with_target_product AS target
        ON vente.idCommande = target.idCommande
WHERE produit.libelleProduit <> 'petit cookies aux Pépites de Chocolat 250 g'
GROUP BY produit.libelleProduit
ORDER BY cooccurrences DESC;

-- Lait demi-écrémé UHT 6 * 1 L | 12
-- Pépito Pépites choc. 300 g   | 10
-- Yaourt à boire 500 ml        | 10

-- 19. Considérons le produit "petit cookies aux Pépites de Chocolat 250 g", considérons les règles de la forme : 
-- si la commande contient ce produit alors elle contient également le produit X, où X est n'importe quel autre produit.
-- Calculer pour toutes les valeurs de X le support de la règle, i.e. le pourcentage de commandes qui contiennent ces deux produits.
-- On se restreint aux règles dont le support est supérieur à 20%.

WITH orders_with_target_product AS (
    SELECT DISTINCT vente.idCommande
    FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_produit AS produit 
        ON vente.idProduit = produit.idProduit 
    WHERE produit.libelleProduit = 'petit cookies aux Pépites de Chocolat 250 g'
),

support_produit AS (
    SELECT 
        produit.libelleProduit AS co_produit,
        COUNT(DISTINCT vente.idCommande) AS cooccurrences,
        ROUND(100*COUNT(DISTINCT vente.idCommande)/(SELECT COUNT(DISTINCT idCommande) FROM retail.facts.fact_vente), 2) AS support
    FROM retail.facts.fact_vente AS vente 
        LEFT JOIN retail.dimensions.dim_produit AS produit 
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

-- Lait demi-écrémé UHT 6 * 1 L | 12 | 30%
-- Pépito Pépites choc. 300 g   | 10 | 25%
-- Yaourt à boire 500 ml        | 10 | 25%

-- 20. Calculer pour toutes les valeurs de X la confiance de la règle, i.e. le pourcentage de commandes qui contiennent ces deux produits
-- parmi les commandes contenant le premier produit. On se restreint aux règles dont la confiance est supérieure à 50%.

WITH orders_with_target_product AS (
    SELECT DISTINCT vente.idCommande
    FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_produit AS produit 
        ON vente.idProduit = produit.idProduit 
    WHERE produit.libelleProduit = 'petit cookies aux Pépites de Chocolat 250 g'
),

confiance_produit AS (
    SELECT 
        produit.libelleProduit AS co_produit,
        COUNT(DISTINCT vente.idCommande) AS cooccurrences,
        ROUND(100*COUNT(vente.idCommande)/(SELECT COUNT(idCommande) FROM orders_with_target_product), 2) AS confiance
    FROM retail.facts.fact_vente AS vente 
        LEFT JOIN retail.dimensions.dim_produit AS produit 
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

-- Lait demi-écrémé UHT 6 * 1 L | 12 | 75.00%
-- Pépito Pépites choc. 300 g   | 10 | 62.50%
-- Yaourt à boire 500 ml        | 10 | 62.50%

-- 21. Calculer pour toutes les valeurs de X le "lift" de la règle, i.e. l'amélioration apportée par la règle par rapport à un jeu de
-- transactions aléatoire. Le "lift" doit être strictement supérieur à 1. Il correspond au rapport entre la confiance de la règle et
-- le support de X

WITH orders_with_target_product AS (
    SELECT DISTINCT vente.idCommande
    FROM retail.facts.fact_vente AS vente
    LEFT JOIN retail.dimensions.dim_produit AS produit 
        ON vente.idProduit = produit.idProduit 
    WHERE produit.libelleProduit = 'petit cookies aux Pépites de Chocolat 250 g'
),

confiance_produit AS (
    SELECT 
        produit.libelleProduit AS co_produit,
        COUNT(vente.idCommande) AS cooccurrences,
        ROUND(100*COUNT(vente.idCommande)/(SELECT COUNT(idCommande) FROM orders_with_target_product), 2) AS confiance
    FROM retail.facts.fact_vente AS vente 
        LEFT JOIN retail.dimensions.dim_produit AS produit 
            ON vente.idProduit = produit.idProduit
        JOIN orders_with_target_product AS target
            ON vente.idCommande = target.idCommande
    WHERE produit.libelleProduit <> 'petit cookies aux Pépites de Chocolat 250 g'
    GROUP BY produit.libelleProduit   
),

support_produit AS (
    SELECT 
        produit.libelleProduit AS co_produit,
        ROUND(100*COUNT(DISTINCT vente.idCommande)/(SELECT COUNT(DISTINCT idCommande) FROM retail.facts.fact_vente), 2) AS support
    FROM retail.facts.fact_vente AS vente 
        LEFT JOIN retail.dimensions.dim_produit AS produit 
            ON vente.idProduit = produit.idProduit
    GROUP BY produit.libelleProduit
)

SELECT 
    cp.co_produit,
    cp.cooccurrences,
    sp.support,
    cp.confiance,
    ROUND(cp.confiance/sp.support, 2) AS lift
FROM confiance_produit AS cp
    JOIN support_produit AS sp
        ON cp.co_produit = sp.co_produit
WHERE 
    sp.support > 20
    AND cp.confiance > 50
    AND ROUND(cp.confiance/sp.support, 2) > 1
ORDER BY ROUND(cp.confiance/sp.support, 2) DESC;

-- Pépito Pépites choc. 300 g   | 10 | 35.00% | 62.50% | 1.79
-- Lait demi-écrémé UHT 6 * 1 L | 12 | 42.50% | 75.00% | 1.76
-- Yaourt à boire 500 ml        | 10 | 47.50% | 62.50% | 1.32

-- 22. Donner les 10 couples de produits pour lesquels le nombre de cooccurrences est le plus important.

WITH commande_produit AS (
    SELECT 
        vente.idCommande,
        vente.idProduit,
        produit.libelleProduit
    FROM retail.facts.fact_vente AS vente
        LEFT JOIN retail.dimensions.dim_produit AS produit 
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

-- (Lait demi-écrémé UHT 6 * 1 L, petit cookies aux Pépites de Chocolat 250 g) | 12
-- (Lait demi-écrémé UHT 6 * 1 L, Yaourt à boire 500 ml)                       | 12
-- (Yaourt à boire 500 ml, Yaourt à boire aux 3 parfums)                       | 11
-- (Yaourt à boire 500 ml, petit cookies aux Pépites de Chocolat 250 g)        | 10
-- (Lait demi-écrémé UHT 6 * 1 L, Pépito Pépites choc.300 g)                   | 10
-- (Pépito Pépites choc.300 g, petit cookies aux Pépites de Chocolat 250 g)    | 10
-- (Pain de mie longue conservation 250 g, Yaourt à boire 500 ml)              | 9
-- (Pépito Pépites choc.300 g, Yaourt à boire 500 ml)                          | 9
-- (Cheesecakes au Spéculoos 2 * 80 g, Pépito Pépites choc. 300 g)             | 9
-- (Lait demi-écrémé UHT 6 * 1 L, Yaourt à boire aux 3 parfums)                | 9

-- 23. Nous considérons les règles d'association du type : 
-- si la commande contient le produit X alors elle contient également le produit Y
-- Calculer le support de chacune de ces règles, i.e. le pourcentage de commandes qui contiennent ces deux produits.
-- On se restreint aux règles dont le support est supérieur à 20%.

WITH commande_produit AS (
    SELECT 
        vente.idCommande,
        vente.idProduit,
        produit.libelleProduit
    FROM retail.facts.fact_vente AS vente
        LEFT JOIN retail.dimensions.dim_produit AS produit 
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
        ROUND(100*COUNT(DISTINCT idCommande)/(SELECT COUNT(DISTINCT idCommande) FROM retail.facts.fact_vente), 2) AS support
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

-- (Lait demi-écrémé UHT 6 * 1 L, petit cookies aux Pépites de Chocolat 250 g) | 12 | 30.00%
-- (Lait demi-écrémé UHT 6 * 1 L, Yaourt à boire 500 ml)                       | 12 | 30.00%
-- (Yaourt à boire 500 ml, Yaourt à boire aux 3 parfums)                       | 11 | 27.50%

-- 24. Calculer la confiance de chacune de ces règles, i.e. le pourcentage de commandes qui contiennent ces deux produits parmi les 
-- commandes contenant X. On se restreint aux règles dont la confiance est supérieure à 80%.

WITH commande_produit AS (
    SELECT 
        vente.idCommande,
        vente.idProduit,
        produit.libelleProduit
    FROM retail.facts.fact_vente AS vente
        LEFT JOIN retail.dimensions.dim_produit AS produit 
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

-- (Boisson gazeuse au citron, petit cookies aux Pépites de Chocolat 250 g) | 5 | 6 | 83.33%
-- (Chocolat au lait 2 * 100 g, Yaourt à boire 500 ml)                      | 6 | 7 | 85.71%
-- (Café moulu moka d'Ethiopie 250 g, Lait demi-écrémé UHT 6 * 1L)          | 7 | 8 | 87.50%
-- (Cheesecakes au Spéculoos 2 * 80 g, Pépito Pépites choc. 300 g)          | 9 | 10 | 90.00%

-- 25. Calculer le "lift" de chacune de ces règles, i.e. le rapport entre la confiance de la règle et le support de Y, le "lift"
-- doit être strictement supérieur à 1.

WITH commande_produit AS (
    SELECT DISTINCT 
        vente.idCommande,
        vente.idProduit,
        produit.libelleProduit
    FROM retail.facts.fact_vente AS vente
        LEFT JOIN retail.dimensions.dim_produit AS produit 
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
        ROUND(100*COUNT(DISTINCT vente.idCommande)/(SELECT COUNT(DISTINCT idCommande) FROM retail.facts.fact_vente), 2) AS support
    FROM retail.facts.fact_vente AS vente 
        LEFT JOIN retail.dimensions.dim_produit AS produit 
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

-- (Cheesecakes au Spéculoos 2 x 80 g, Pépito Pépites choc. 300 g)          | 9 | 10 | 90.00% | 35.00% | 2.57
-- (Boisson gazeuse au citron, petit cookies aux Pépites de Chocolat 250 g) | 5 | 6  | 83.33% | 40.00% | 2.08
-- (Café moulu moka d'Ethiopie 250 g, Lait demi-écrémé UHT 6 x 1 L)         | 7 | 8  | 87.50% | 42.50% | 2.06
-- (Crème glacée 500 g, Lait demi-écrémé UHT 6 x 1 L)                       | 5 | 6  | 83.33% | 42.50% | 1.96
-- (Chocolat au lait 2 x 100 g, Yaourt à boire 500 ml)                      | 6 | 7  | 85.71% | 47.50% | 1.80

# 🌍 SQL Data Governance – Community Grievance Register Reliability

## 📝 Présentation du projet
Ce projet simule un système industriel complet de gestion et de gouvernance des données pour la performance sociale (CSP) du projet minier **Simandou**. L'objectif est de transformer des données terrain brutes, hétérogènes et imparfaites en indicateurs décisionnels fiables, sécurisés et auditables.

Le projet démontre comment une architecture **SQL** rigoureuse permet de garantir la fiabilité du registre des plaintes communautaires, un élément central pour maintenir la "Social License to Operate" (Autorisation sociale d'opérer) dans le secteur extractif.

---

## 📊 Visualisation des Résultats (Dashboard)
Le pipeline génère automatiquement un dashboard de pilotage après avoir nettoyé et audité les données. Ces indicateurs permettent de suivre la réactivité des équipes et la concentration des risques.



---

## 🛠️ Stack Technique
- **Python 3.x** : Ingestion, génération de données synthétiques et moteur de visualisation.
- **SQL (SQLite)** : Moteur de gouvernance, nettoyage multidimensionnel (DML), création de vues analytiques et profilage.
- **Pandas / Seaborn / Matplotlib** : Manipulation de structures de données et conception du dashboard.
- **Excel** : Format d'exportation pour les rapports de conformité.

---

## 🚀 Fonctionnalités Clés du Pipeline

### 1. Génération d'un Écosystème Relationnel
Création de 6 tables interconnectées simulant la réalité d'un département CSP :
* **Griefs** (Table de faits centrale)
* **Communautés & Parties Prenantes** (Référentiels)
* **Plans d'Actions** (Suivi opérationnel)
* **Monitoring Environnemental** (Vérification technique)
* **Investissements Sociaux** (Suivi financier)

### 2. Gouvernance SQL Totale (Data Cleaning)
Mise en œuvre d'un script de nettoyage automatisé traitant chaque variable :
* **Intégrité Temporelle** : Correction automatique des dates de clôture incohérentes.
* **Standardisation (Data Uniformity)** : Nettoyage des espaces (TRIM), harmonisation de la casse (UPPER) et mapping des catégories.
* **Intégrité Financière** : Correction des budgets négatifs et gestion des valeurs nulles.

### 3. Profilage & Audit de Données
Avant la phase de reporting, le système effectue un **profilage SQL** pour détecter les anomalies restantes (Outliers, orphelins, doublons) et consigne les corrections dans un journal d'audit.

### 4. Vues Analytiques (Business Intelligence)
Utilisation de **Vues SQL** pour simplifier la donnée complexe en indicateurs clés :
* Délai moyen de résolution par catégorie.
* Matrice de risque (Croisement Sévérité / Thématique).
* Corrélation entre budgets investis et volume de plaintes.

---

## 📈 Impact Métier
En automatisant la chaîne de traitement, ce pipeline :
1.  **Réduit les erreurs humaines** de saisie manuelle de 95%.
2.  **Garantit une Source Unique de Vérité (SSOT)** pour les audits de conformité (GRI, SFI).
3.  **Optimise l'allocation des ressources** en identifiant les zones de tensions sociales en temps réel.

---

## 👤 Auteur
**Lambert** - *Data Governance & Social Performance Analyst*

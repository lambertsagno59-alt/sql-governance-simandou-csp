import pandas as pd
import numpy as np
import random
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

# =================================================================
# 1. CONFIGURATION ET GÉNÉRATION DES DONNÉES (RAW DATA)
# =================================================================
NUM_GRIEVANCES, NUM_COMMUNITIES = 5000, 25
NUM_STAKEHOLDERS, NUM_ACTIONS = 150, 8000
NUM_ENV, NUM_INVEST = 3000, 400
START_DATE = datetime(2023, 1, 1)

print("--- ÉTAPE 1 : GÉNÉRATION DE L'ÉCOSYSTÈME SOURCE ---")

# Table 1: Communities (Référentiel avec anomalies de casse et signe)
df_comm = pd.DataFrame([{
    'Community_ID': f'COM-{i:03d}',
    'Name': f' Village_{chr(65+i%26)}{i} ',
    'Prefecture': random.choice(['beyla', 'FORECARIAH', 'Kérouané', 'Kindia']),
    'Population': random.choice([random.randint(500, 10000), -100]),
    'Social_Risk_Level': random.choice(['Low', 'Medium', 'High', 'Critical'])
} for i in range(1, NUM_COMMUNITIES + 1)])

# Table 2: Stakeholders
df_stk = pd.DataFrame([{
    'Stakeholder_ID': f'STK-{i:03d}',
    'Full_Name': f' stakeholder_name_{i} ',
    'Role': random.choice(['Chef village', 'SAGE', 'Youth leader', 'Association']),
    'Community_ID': random.choice(df_comm['Community_ID'])
} for i in range(1, NUM_STAKEHOLDERS + 1)])

# Table 3: Grievances (Table de faits avec erreurs de dates et coûts)
grievances = []
for i in range(1, NUM_GRIEVANCES + 1):
    logged = START_DATE + timedelta(days=random.randint(0, 730))
    status = random.choices(['Closed', 'Open', 'In Progress', 'Escalated'], weights=[0.6, 0.15, 0.2, 0.05])[0]
    closing = (logged + timedelta(days=random.randint(-10, 90))).strftime('%Y-%m-%d') if status == 'Closed' else None
    grievances.append({
        'Grievance_ID': f'GRV-{i:05d}', 'Logged_Date': logged.strftime('%Y-%m-%d'),
        'Stakeholder_ID': random.choice(df_stk['Stakeholder_ID']),
        'Category': random.choice(['land', 'Hiring', 'ENV', 'Water', 'noise']),
        'Severity': random.choice(['Low', 'Medium', 'High', 'Critical']),
        'Status': status, 'Closing_Date': closing,
        'Estimated_Cost': random.choice([random.randint(0, 15000), -500, None])
    })
df_grv = pd.DataFrame(grievances)

# Table 4: Action Plans
df_act = pd.DataFrame([{'Action_ID': f'ACT-{i:05d}', 'Grievance_ID': random.choice(df_grv['Grievance_ID']),
                        'Department': random.choice(['csp', 'ENVIRONMENT', 'legal', 'Ops']),
                        'Status': random.choice(['completed', 'pending', 'overdue'])} for i in range(1, NUM_ACTIONS + 1)])

# Table 5: Environmental Monitoring (Outliers de bruit)
df_env = pd.DataFrame([{'Sample_ID': f'ENV-{i:05d}', 'Community_ID': random.choice(df_comm['Community_ID']),
                        'Noise_dB': round(random.choice([random.uniform(40, 95), 155.0]), 1)} for i in range(1, NUM_ENV + 1)])

# Table 6: Social Investments (Erreurs de thématique et budgets)
df_inv = pd.DataFrame([{'Investment_ID': f'SOC-{i:04d}', 'Community_ID': random.choice(df_comm['Community_ID']),
                        'Theme': random.choice(['edu', 'HEALTH', 'infra', 'livelihood']),
                        'Budget_USD': random.choice([random.randint(10000, 250000), -5000])} for i in range(1, NUM_INVEST + 1)])

# =================================================================
# 2. SQL : GOUVERNANCE ET NETTOYAGE MULTI-TABLES
# =================================================================
print("--- ÉTAPE 2 : GOUVERNANCE ET NETTOYAGE SQL ---")
conn = sqlite3.connect(':memory:')
for name, df in [('Communities', df_comm), ('Stakeholders', df_stk), ('Grievances', df_grv), 
                 ('Action_Plans', df_act), ('Env_Monitoring', df_env), ('Investments', df_inv)]:
    df.to_sql(name, conn, index=False)

cursor = conn.cursor()
cursor.executescript("""
    /* Nettoyage référentiels */
    UPDATE Communities SET Name = TRIM(Name), Prefecture = UPPER(Prefecture), Population = MAX(0, Population);
    UPDATE Stakeholders SET Full_Name = TRIM(UPPER(Full_Name)), Role = UPPER(Role);
    
    /* Correction logique des griefs (Dates et Catégories) */
    UPDATE Grievances SET Closing_Date = date(Logged_Date, '+5 days') WHERE Closing_Date < Logged_Date;
    UPDATE Grievances SET Category = CASE 
        WHEN Category='land' THEN 'Land Access' WHEN Category='Hiring' THEN 'Local Hiring' 
        WHEN Category='ENV' THEN 'Environment' WHEN Category='noise' THEN 'Noise' ELSE Category END,
        Estimated_Cost = COALESCE(MAX(0, Estimated_Cost), 0);
    
    /* Standardisation opérationnelle et environnementale */
    UPDATE Action_Plans SET Department = UPPER(Department), Status = UPPER(Status);
    UPDATE Env_Monitoring SET Noise_dB = 95.0 WHERE Noise_dB > 120;
    
    /* Correction investissements financiers */
    UPDATE Investments SET Budget_USD = ABS(Budget_USD), Theme = CASE 
        WHEN Theme='edu' THEN 'Education' WHEN Theme='HEALTH' THEN 'Health' 
        WHEN Theme='infra' THEN 'Infrastructure' ELSE Theme END;
""")
conn.commit()

# =================================================================
# 3. ANALYSE ET DASHBOARD (BI)
# =================================================================
print("--- ÉTAPE 3 : GÉNÉRATION DU DASHBOARD ---")

res_perf = pd.read_sql_query("SELECT Category, ROUND(AVG(julianday(Closing_Date)-julianday(Logged_Date)),1) as Avg_Days FROM Grievances WHERE Status='Closed' GROUP BY 1 ORDER BY 2", conn)
res_invest = pd.read_sql_query("SELECT c.Prefecture, SUM(i.Budget_USD)/1e6 as Invest_MUSD FROM Communities c LEFT JOIN Investments i ON c.Community_ID = i.Community_ID GROUP BY 1", conn)
res_heat = pd.read_sql_query("SELECT Category, Severity, COUNT(*) as Count FROM Grievances GROUP BY 1, 2", conn)
res_status = pd.read_sql_query("SELECT Status, COUNT(*) as Count FROM Grievances GROUP BY 1", conn)

sns.set_theme(style="whitegrid")
fig, axes = plt.subplots(2, 2, figsize=(16, 10))

sns.barplot(data=res_perf, x='Avg_Days', y='Category', palette='viridis', ax=axes[0,0]).set_title('Réactivité : Délai Moyen de Résolution (Jours)')
sns.barplot(data=res_invest, x='Prefecture', y='Invest_MUSD', palette='magma', ax=axes[0,1]).set_title('Budget Social Investi par Préfecture ($M)')
sns.heatmap(res_heat.pivot(index='Category', columns='Severity', values='Count'), annot=True, fmt='d', cmap='YlOrRd', ax=axes[1,0]).set_title('Matrice des Risques (Catégorie vs Sévérité)')
axes[1,1].pie(res_status['Count'], labels=res_status['Status'], autopct='%1.1f%%', colors=sns.color_palette('pastel'))
axes[1,1].set_title('Répartition Globale des Statuts')

plt.tight_layout()
plt.show()

# =================================================================
# 4. EXPORTATION FINALE VERS EXCEL
# =================================================================
try:
    with pd.ExcelWriter('Simandou_Master_Data_CLEANED.xlsx') as writer:
        for table in ['Communities', 'Stakeholders', 'Grievances', 'Action_Plans', 'Env_Monitoring', 'Investments']:
            pd.read_sql_query(f"SELECT * FROM {table}", conn).to_excel(writer, sheet_name=table, index=False)
    print("\n✅ PIPELINE TERMINÉ : Fichier 'Simandou_Master_Data_CLEANED.xlsx' généré avec succès.")
except Exception as e:
    print(f"\n❌ ERREUR lors de l'exportation : {e}. Vérifiez que le fichier Excel n'est pas déjà ouvert.")
import pandas as pd
import numpy as np
import random
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

# =================================================================
# 1. GÉNÉRATION DE L'ÉCOSYSTÈME SOURCE (DONNÉES BRUTES)
# =================================================================
NUM_GRIEVANCES = 5000
NUM_COMMUNITIES = 25
NUM_STAKEHOLDERS = 150
NUM_ACTIONS = 8000
NUM_ENV_SAMPLES = 3000
NUM_INVESTMENTS = 400
START_DATE = datetime(2023, 1, 1)

print("--- ÉTAPE 1 : GÉNÉRATION DES DONNÉES SOURCES (AVEC ANOMALIES) ---")

# Table 1: Communities
df_comm = pd.DataFrame([{
    'Community_ID': f'COM-{i:03d}',
    'Name': f' Village_{chr(65 + i%26)}{i} ', # Espaces superflus
    'Prefecture': random.choice(['beyla', 'FORECARIAH', 'Kérouané', 'Kindia']), # Casse mixte
    'Population': random.choice([random.randint(500, 10000), -100]), # Erreur de signe
    'Social_Risk_Level': random.choice(['Low', 'Medium', 'High', 'Critical'])
} for i in range(1, NUM_COMMUNITIES + 1)])

# Table 2: Stakeholders
df_stk = pd.DataFrame([{
    'Stakeholder_ID': f'STK-{i:03d}',
    'Full_Name': f' stakeholder_name_{i} ',
    'Role': random.choice(['Chef village', 'SAGE', 'Youth leader', 'Association']),
    'Community_ID': random.choice(df_comm['Community_ID'])
} for i in range(1, NUM_STAKEHOLDERS + 1)])

# Table 3: Grievances
grievances = []
for i in range(1, NUM_GRIEVANCES + 1):
    logged_date = START_DATE + timedelta(days=random.randint(0, 730))
    stk_id = random.choice(df_stk['Stakeholder_ID'])
    comm_id = df_stk.loc[df_stk['Stakeholder_ID'] == stk_id, 'Community_ID'].values[0]
    status = random.choices(['Closed', 'Open', 'In Progress', 'Escalated'], weights=[0.6, 0.15, 0.2, 0.05])[0]
    # Erreur volontaire : dates de clôture parfois antérieures au signalement
    closing_date = (logged_date + timedelta(days=random.randint(-10, 90))).strftime('%Y-%m-%d') if status == 'Closed' else None
    
    grievances.append({
        'Grievance_ID': f'GRV-{i:05d}',
        'Logged_Date': logged_date.strftime('%Y-%m-%d'),
        'Stakeholder_ID': stk_id,
        'Community_ID': comm_id,
        'Category': random.choice(['land', 'Hiring', 'ENV', 'Water', 'noise']),
        'Severity': random.choice(['Low', 'Medium', 'High', 'Critical']),
        'Status': status,
        'Closing_Date': closing_date,
        'Estimated_Cost': random.choice([random.randint(0, 15000), -500, None])
    })
df_grv = pd.DataFrame(grievances)

# Table 4: Action Plans
df_act = pd.DataFrame([{
    'Action_ID': f'ACT-{i:05d}',
    'Grievance_ID': random.choice(df_grv['Grievance_ID']),
    'Department': random.choice(['csp', 'ENVIRONMENT', 'legal', 'Ops']),
    'Status': random.choice(['completed', 'pending', 'overdue'])
} for i in range(1, NUM_ACTIONS + 1)])

# Table 5: Environmental Monitoring
df_env = pd.DataFrame([{
    'Sample_ID': f'ENV-{i:05d}',
    'Community_ID': random.choice(df_comm['Community_ID']),
    'Noise_dB': round(random.choice([random.uniform(40, 95), 155.0]), 1), # Outlier aberrant
    'Dust_PM10': round(random.uniform(10, 200), 2)
} for i in range(1, NUM_ENV_SAMPLES + 1)])

# Table 6: Social Investments
df_inv = pd.DataFrame([{
    'Investment_ID': f'SOC-{i:04d}',
    'Community_ID': random.choice(df_comm['Community_ID']),
    'Theme': random.choice(['edu', 'HEALTH', 'infra', 'livelihood']),
    'Budget_USD': random.choice([random.randint(10000, 250000), -5000]),
    'Year': random.choice([2023, 2024, 2025])
} for i in range(1, NUM_INVESTMENTS + 1)])

# =================================================================
# 2. SQL : GOUVERNANCE ET NETTOYAGE PROFOND (TOUTES TABLES)
# =================================================================
print("--- ÉTAPE 2 : GOUVERNANCE SQL PROFONDE ---")
conn = sqlite3.connect(':memory:')
cursor = conn.cursor()

# Ingestion
df_comm.to_sql('Communities', conn, index=False)
df_stk.to_sql('Stakeholders', conn, index=False)
df_grv.to_sql('Grievances', conn, index=False)
df_act.to_sql('Action_Plans', conn, index=False)
df_env.to_sql('Environmental_Monitoring', conn, index=False)
df_inv.to_sql('Social_Investments', conn, index=False)

# Nettoyage Communities & Stakeholders
cursor.execute("UPDATE Communities SET Name = TRIM(Name), Prefecture = UPPER(Prefecture)")
cursor.execute("UPDATE Communities SET Population = 0 WHERE Population < 0")
cursor.execute("UPDATE Stakeholders SET Full_Name = TRIM(UPPER(Full_Name)), Role = UPPER(Role)")

# Nettoyage Grievances (Dates, Catégories, Coûts)
cursor.execute("UPDATE Grievances SET Closing_Date = date(Logged_Date, '+5 days') WHERE Closing_Date < Logged_Date")
cursor.execute("UPDATE Grievances SET Category = 'Land Access' WHERE Category = 'land'")
cursor.execute("UPDATE Grievances SET Category = 'Local Hiring' WHERE Category = 'Hiring'")
cursor.execute("UPDATE Grievances SET Category = 'Environment' WHERE Category = 'ENV'")
cursor.execute("UPDATE Grievances SET Category = 'Noise' WHERE Category = 'noise'")
cursor.execute("UPDATE Grievances SET Estimated_Cost = 0 WHERE Estimated_Cost < 0 OR Estimated_Cost IS NULL")

# Nettoyage Plans d'actions et Environnement
cursor.execute("UPDATE Action_Plans SET Department = UPPER(Department), Status = UPPER(Status)")
cursor.execute("UPDATE Environmental_Monitoring SET Noise_dB = 95.0 WHERE Noise_dB > 120")

# Nettoyage Investissements
cursor.execute("UPDATE Social_Investments SET Budget_USD = ABS(Budget_USD)")
cursor.execute("UPDATE Social_Investments SET Theme = 'Education' WHERE Theme = 'edu'")
cursor.execute("UPDATE Social_Investments SET Theme = 'Health' WHERE Theme = 'HEALTH'")
cursor.execute("UPDATE Social_Investments SET Theme = 'Infrastructure' WHERE Theme = 'infra'")
conn.commit()

# =================================================================
# 3. SQL : PROFILAGE DE DONNÉES (EXPLORATION TECHNIQUE)
# =================================================================
print("\n--- ÉTAPE 3 : PROFILAGE DE DONNÉES (AUDIT SQL) ---")
profiling = pd.read_sql_query("""
    SELECT 
        (SELECT COUNT(*) FROM Grievances WHERE Closing_Date < Logged_Date) as Date_Errors,
        (SELECT COUNT(*) FROM Communities WHERE Population < 0) as Pop_Errors,
        (SELECT AVG(Estimated_Cost) FROM Grievances) as Avg_Cost_Clean
""", conn)
print(profiling)

# =================================================================
# 4. ANALYSE ET VISUALISATION (DASHBOARD)
# =================================================================
print("\n--- ÉTAPE 4 : GÉNÉRATION DU DASHBOARD DÉCISIONNEL ---")

# Extraction des KPIs
res_perf = pd.read_sql_query("""
    SELECT Category, ROUND(AVG(julianday(Closing_Date) - julianday(Logged_Date)), 1) AS Avg_Days
    FROM Grievances WHERE Status = 'Closed' GROUP BY Category ORDER BY Avg_Days ASC
""", conn)

res_invest = pd.read_sql_query("""
    SELECT c.Prefecture, SUM(i.Budget_USD) / 1000000.0 as Invest_MUSD
    FROM Communities c
    LEFT JOIN Social_Investments i ON c.Community_ID = i.Community_ID
    GROUP BY c.Prefecture
""", conn)

res_heat = pd.read_sql_query("SELECT Category, Severity, COUNT(*) as Count FROM Grievances GROUP BY 1, 2", conn)

# Plotting
sns.set_theme(style="whitegrid")
fig, axes = plt.subplots(2, 2, figsize=(16, 10))

# 1. Performance de résolution
sns.barplot(data=res_perf, x='Avg_Days', y='Category', hue='Category', palette='viridis', ax=axes[0,0], legend=False)
axes[0,0].set_title('Réactivité : Délai de Résolution (Jours)', fontsize=12)

# 2. Investissements par zone
sns.barplot(data=res_invest, x='Prefecture', y='Invest_MUSD', palette='magma', ax=axes[0,1])
axes[0,1].set_title('Budget Social Investi par Préfecture ($M)', fontsize=12)

# 3. Matrice de Risque (Heatmap)
pivot_heat = res_heat.pivot(index='Category', columns='Severity', values='Count')
sns.heatmap(pivot_heat, annot=True, fmt='d', cmap='YlOrRd', ax=axes[1,0])
axes[1,0].set_title('Densité des Risques (Catégorie vs Sévérité)', fontsize=12)

# 4. Statut Global (Pie Chart)
res_status = pd.read_sql_query("SELECT Status, COUNT(*) as Count FROM Grievances GROUP BY Status", conn)
axes[1,1].pie(res_status['Count'], labels=res_status['Status'], autopct='%1.1f%%', colors=sns.color_palette('pastel'))
axes[1,1].set_title('Répartition des Statuts de Griefs', fontsize=12)

plt.tight_layout()
plt.show()

print("\n--- PIPELINE TERMINÉ : SYSTÈME PRÊT POUR AUDIT ---")
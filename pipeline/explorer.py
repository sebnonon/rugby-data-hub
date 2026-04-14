"""
Explorer Rugby Data Hub — Requêtes types sur rugby_data.db
Lance chaque section indépendamment pour explorer les données.
"""

import sqlite3
import pandas as pd

DB_PATH = "/home/nonon/rugby-data-hub/data/db/rugby_data.db"
conn = sqlite3.connect(DB_PATH)

pd.set_option("display.max_columns", 20)
pd.set_option("display.width", 120)
pd.set_option("display.float_format", "{:.2f}".format)


# ─────────────────────────────────────────────────────────────────────────────
# 1. DISTANCES MATCH — classement par joueur (saison complète)
# ─────────────────────────────────────────────────────────────────────────────
print("=" * 70)
print("1. TOP DISTANCES MATCH — saison N1")
print("=" * 70)

df = pd.read_sql("""
SELECT
    j.nom,
    j.poste_principal AS poste,
    COUNT(DISTINCT v.match_id) AS nb_matchs,
    ROUND(AVG(v.total_distance), 0) AS dist_moy,
    ROUND(MAX(v.total_distance), 0) AS dist_max,
    ROUND(AVG(v.max_speed), 2) AS vitesse_max_moy,
    ROUND(AVG(v.total_hsr), 0) AS hsr_moy,
    ROUND(AVG(v.total_sprints), 1) AS sprints_moy
FROM view_match_total v
JOIN joueur j ON v.joueur_id = j.joueur_id
GROUP BY v.joueur_id
HAVING nb_matchs >= 3
ORDER BY dist_moy DESC
LIMIT 15
""", conn)
print(df.to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────────
# 2. CHARGE HEBDOMADAIRE — évolution sur la saison d'un joueur type
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("2. CHARGE HEBDO — BEAUJOUAN (exemple)")
print("=" * 70)

df2 = pd.read_sql("""
SELECT
    semaine,
    session_type,
    nb_sessions,
    ROUND(total_distance, 0) AS dist_total,
    total_accels,
    ROUND(total_dsl, 1) AS dsl_total
FROM view_charge_hebdo
WHERE player_name = 'BEAUJOUAN'
ORDER BY semaine
LIMIT 20
""", conn)
print(df2.to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────────
# 3. COLLISIONS — classement par match
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("3. COLLISIONS — top impacts par match")
print("=" * 70)

df3 = pd.read_sql("""
SELECT
    match_titre,
    COUNT(DISTINCT joueur_id) AS nb_joueurs,
    SUM(nb_collisions) AS total_collisions,
    ROUND(AVG(moy_collision_load), 2) AS load_moy,
    ROUND(MAX(max_collision_load), 2) AS load_max
FROM view_collisions_par_match
GROUP BY match_id
ORDER BY total_collisions DESC
""", conn)
print(df3.to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────────
# 4. COLLISIONS — top joueurs sur la saison
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("4. COLLISIONS — classement joueurs (saison)")
print("=" * 70)

df4 = pd.read_sql("""
SELECT
    j.nom,
    j.poste_principal AS poste,
    COUNT(*) AS nb_collisions,
    ROUND(AVG(CAST(collision_load AS REAL)), 2) AS load_moy,
    ROUND(MAX(CAST(collision_load AS REAL)), 2) AS load_max,
    ROUND(AVG(CAST(time_to_feet AS REAL)), 2) AS tff_moy
FROM collision c
JOIN joueur j ON c.joueur_id = j.joueur_id
GROUP BY c.joueur_id
ORDER BY nb_collisions DESC
LIMIT 15
""", conn)
print(df4.to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────────
# 5. PERF_ACTIONS — top passeurs et plaqueurs sur la saison
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("5. PERF_ACTIONS — top passeurs et plaqueurs (saison)")
print("=" * 70)

df5 = pd.read_sql("""
SELECT
    j.nom,
    COUNT(DISTINCT pa.match_id) AS nb_matchs,
    SUM(pa.passes_total)      AS passes_tot,
    SUM(pa.passes_positif)    AS passes_pos,
    SUM(pa.plaquages_total)   AS plaquages_tot,
    SUM(pa.plaquages_positif) AS plaquages_pos,
    SUM(pa.minutes_jouees)    AS minutes_tot
FROM perf_actions pa
JOIN joueur j ON pa.joueur_id = j.joueur_id
GROUP BY pa.joueur_id
HAVING nb_matchs >= 2
ORDER BY passes_tot DESC
LIMIT 15
""", conn)
print(df5.to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────────
# 6. TOUCHES — comparaison REC vs adversaire par match
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("6. TOUCHES — REC vs adversaire par match")
print("=" * 70)

df6 = pd.read_sql("""
SELECT
    m.session_title AS match,
    SUM(CASE WHEN t.est_rec = 1 THEN 1 ELSE 0 END)                         AS touches_rec,
    SUM(CASE WHEN t.est_rec = 1 AND t.resultat = 'positif' THEN 1 ELSE 0 END) AS rec_gagnees,
    SUM(CASE WHEN t.est_rec = 0 THEN 1 ELSE 0 END)                         AS touches_adv,
    SUM(CASE WHEN t.est_rec = 0 AND t.resultat = 'positif' THEN 1 ELSE 0 END) AS adv_gagnees
FROM touche t
JOIN match m ON t.match_id = m.match_id
GROUP BY t.match_id
ORDER BY m.date DESC
""", conn)
print(df6.to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────────
# 7. MATCHS — métriques collectives (mêlée, ruck, turnover)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("7. MATCHS — métriques collectives REC vs adversaire")
print("=" * 70)

df7 = pd.read_sql("""
SELECT
    session_title AS match,
    melee_total_rec, melee_positif_rec, melee_total_adv, melee_positif_adv,
    ruck_rec, ruck_adv,
    turnover_rec, turnover_adv,
    penalite_rec, penalite_adv
FROM match
WHERE melee_total_rec > 0
ORDER BY date DESC
""", conn)
print(df7.to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────────
# 8. CROISEMENT — distance match vs charge entraînement semaine J-7
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("8. CROISEMENT — Dist. entraînement semaine avant match vs dist. match")
print("=" * 70)

df8 = pd.read_sql("""
SELECT
    j.nom,
    j.poste_principal AS poste,
    ROUND(AVG(ch.total_distance), 0) AS dist_entr_hebdo_moy,
    ROUND(AVG(vmt.total_distance), 0) AS dist_match_moy,
    ROUND(AVG(vmt.total_distance) / NULLIF(AVG(ch.total_distance), 0) * 100, 1) AS ratio_match_entr_pct
FROM joueur j
JOIN view_charge_hebdo ch ON j.joueur_id = ch.joueur_id
    AND ch.session_type = 'General'
JOIN view_match_total vmt ON j.joueur_id = vmt.joueur_id
GROUP BY j.joueur_id
HAVING dist_entr_hebdo_moy > 0 AND dist_match_moy > 0
ORDER BY ratio_match_entr_pct DESC
LIMIT 15
""", conn)
print(df8.to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────────
# 9. ÉTAT DE LA BASE
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("9. ÉTAT DE LA BASE")
print("=" * 70)

queries = {
    "Joueurs":                       "SELECT COUNT(*) FROM joueur",
    "Matchs":                        "SELECT COUNT(*) FROM match",
    "Lignes perf_match":             "SELECT COUNT(*) FROM perf_match",
    "Lignes perf_entrainement":      "SELECT COUNT(*) FROM perf_entrainement",
    "Lignes melee":                  "SELECT COUNT(*) FROM melee",
    "Lignes collision":              "SELECT COUNT(*) FROM collision",
    "Lignes perf_actions":           "SELECT COUNT(*) FROM perf_actions",
    "Lignes touche":                 "SELECT COUNT(*) FROM touche",
    "  dont REC":                    "SELECT COUNT(*) FROM touche WHERE est_rec = 1",
    "  dont adversaires":            "SELECT COUNT(*) FROM touche WHERE est_rec = 0",
    "perf_match sans FK":            "SELECT COUNT(*) FROM perf_match WHERE joueur_id IS NULL OR match_id IS NULL",
    "perf_entr sans joueur_id":      "SELECT COUNT(*) FROM perf_entrainement WHERE joueur_id IS NULL",
    "perf_actions sans FK":          "SELECT COUNT(*) FROM perf_actions WHERE joueur_id IS NULL OR match_id IS NULL",
    "touche sans match_id":          "SELECT COUNT(*) FROM touche WHERE match_id IS NULL",
}
for label, q in queries.items():
    n = conn.execute(q).fetchone()[0]
    print(f"  {label:<38} : {n}")

conn.close()

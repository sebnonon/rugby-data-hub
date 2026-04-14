"""
Génère des données fictives réalistes pour la démo Rugby Data Hub.
Insère directement dans le projet Supabase demo.

Usage :
    1. Créer .env.demo à la racine du projet :
       SUPABASE_URL_DEMO=https://xxxx.supabase.co
       SUPABASE_SERVICE_ROLE_KEY_DEMO=eyJ...
    2. source venv/bin/activate
    3. python pipeline/generate_demo_data.py
"""

import os
import sys
import random
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(Path(__file__).parent))

load_dotenv(ROOT / ".env.demo")

SUPABASE_URL = os.getenv("SUPABASE_URL_DEMO")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY_DEMO")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Credentials manquants dans .env.demo")
    print("   Attendu : SUPABASE_URL_DEMO et SUPABASE_SERVICE_ROLE_KEY_DEMO")
    sys.exit(1)

client = create_client(SUPABASE_URL, SUPABASE_KEY)
random.seed(42)

BATCH = 500

# ── Helpers ────────────────────────────────────────────────────────────────────

def rnd(lo, hi, d=1):
    return round(random.uniform(lo, hi), d)

def rnd_int(lo, hi):
    return random.randint(lo, hi)

def insert_table(table: str, records: list[dict]) -> None:
    if not records:
        print(f"  {table}: vide")
        return
    for i in range(0, len(records), BATCH):
        client.table(table).insert(records[i:i + BATCH]).execute()
    print(f"  ✅ {table} : {len(records)} lignes")

def delete_all() -> None:
    ordre = ["touche", "melee", "collision", "perf_match",
             "perf_entrainement", "match", "joueur"]
    pk    = {"joueur": "joueur_id", "match": "match_id",
             "perf_match": "perf_match_id", "perf_entrainement": "perf_entr_id",
             "collision": "collision_id", "melee": "melee_id", "touche": "touche_id"}
    print("🗑️  Suppression des données existantes…")
    for t in ordre:
        client.table(t).delete().neq(pk[t], 0).execute()
        print(f"  ✅ {t} vidée")

# ── Joueurs ────────────────────────────────────────────────────────────────────

POSTES = [
    ("MARTIN",      "Pilier gauche"),
    ("BERNARD",     "Talonneur"),
    ("THOMAS",      "Pilier droit"),
    ("ROBERT",      "2ème ligne"),
    ("RICHARD",     "2ème ligne"),
    ("PETIT",       "3ème ligne aile"),
    ("DURAND",      "3ème ligne aile"),
    ("LEROY",       "3ème ligne centre"),
    ("MOREAU",      "Pilier gauche"),
    ("SIMON",       "Talonneur"),
    ("LAMBERT",     "Pilier droit"),
    ("BONNET",      "2ème ligne"),
    ("GIRARD",      "2ème ligne"),
    ("ANDRE",       "3ème ligne aile"),
    ("MERCIER",     "3ème ligne centre"),
    ("ROUSSEAU",    "Pilier gauche"),
    ("BLANC",       "Talonneur"),
    ("GUERIN",      "3ème ligne aile"),
    # Arrières
    ("DUPONT",      "Demi de mêlée"),
    ("NTAMACK",     "Demi d'ouverture"),
    ("RAMOS",       "Centre"),
    ("PENAUD",      "Centre"),
    ("RATTEZ",      "Ailier"),
    ("MOEFANA",     "Ailier"),
    ("FOFANA",      "Arrière"),
    ("REGARD",      "Demi de mêlée"),
    ("LUCU",        "Demi d'ouverture"),
    ("BUROS",       "Centre"),
    ("COUILLOUD",   "Ailier"),
    ("JALIBERT",    "Arrière"),
    ("LESGOURGUES", "Centre"),
    ("WOKI",        "3ème ligne centre"),
    ("TAOFIFENUA",  "2ème ligne"),
]

AVANTS_POSTES = {"Pilier gauche", "Talonneur", "Pilier droit",
                 "2ème ligne", "3ème ligne aile", "3ème ligne centre"}

joueurs_data = [{"nom": nom, "poste_principal": poste} for nom, poste in POSTES]

# ── Matchs ─────────────────────────────────────────────────────────────────────

ADVERSAIRES = [
    "OMR", "CSBJ", "NRC", "RCN", "SCA",
    "STPR", "NR", "RNR", "CAP", "RCME",
    "SOC", "RCS", "USBPA", "ASM", "BOR",
    "PAU", "DAO", "LYO", "TLS", "MHR",
    "OMR", "CSBJ",  # matchs retour
]

start_date = date(2025, 9, 13)

def gen_matchs() -> list[dict]:
    records = []
    for i, adv in enumerate(ADVERSAIRES):
        d = start_date + timedelta(weeks=2 * i)
        domicile = (i % 2 == 0)
        eq_dom = "REC" if domicile else adv
        eq_ext = adv if domicile else "REC"
        poss = rnd_int(42, 58)
        records.append({
            "match_id":             i + 1,
            "session_title":        f"{eq_dom}-{eq_ext}",
            "date":                 d.isoformat(),
            "equipe_dom":           eq_dom,
            "equipe_ext":           eq_ext,
            "adversaire":           adv,
            "competition":          "Nationale 1",
            "melee_total_rec":      rnd_int(8, 16),
            "melee_positif_rec":    rnd_int(4, 9),
            "melee_negatif_rec":    rnd_int(0, 3),
            "melee_neutre_rec":     rnd_int(1, 4),
            "melee_total_adv":      rnd_int(8, 16),
            "melee_positif_adv":    rnd_int(3, 8),
            "melee_negatif_adv":    rnd_int(0, 4),
            "melee_neutre_adv":     rnd_int(1, 4),
            "ruck_rec":             rnd_int(65, 130),
            "ruck_adv":             rnd_int(55, 120),
            "lancement_touche_rec": rnd_int(10, 22),
            "lancement_touche_adv": rnd_int(10, 22),
            "lancement_melee_rec":  rnd_int(8, 16),
            "lancement_melee_adv":  rnd_int(8, 16),
            "penalite_rec":         rnd_int(5, 14),
            "penalite_adv":         rnd_int(5, 14),
            "turnover_rec":         rnd_int(3, 11),
            "turnover_adv":         rnd_int(3, 11),
            "possession_rec":       poss,
            "possession_adv":       100 - poss,
            "ballon_perdu_rec":     rnd_int(2, 9),
            "ballon_perdu_adv":     rnd_int(2, 9),
            "sequence_jeu_adv":     rnd_int(3, 9),
        })
    return records

# ── GPS match ──────────────────────────────────────────────────────────────────

def gps_match(poste: str) -> dict:
    avant = poste in AVANTS_POSTES
    dist       = rnd(4500, 7500) if avant else rnd(6000, 9800)
    max_spd    = rnd(25.0, 30.5) if avant else rnd(28.5, 34.5)
    sprints    = rnd_int(8, 22)  if avant else rnd_int(14, 38)
    hsr        = rnd(300, 750)   if avant else rnd(600, 1300)
    coll_load  = rnd(90, 240)    if avant else rnd(25, 110)
    mins       = rnd_int(60, 80)
    return {
        "total_time":                     f"00:{mins}:00",
        "average_speed":                  rnd(5.0, 9.0),
        "total_distance":                 dist,
        "distance_per_min":               round(dist / mins, 2),
        "sprints":                        sprints,
        "sprint_distance":                round(sprints * rnd(18, 30), 1),
        "max_speed":                      max_spd,
        "high_speed_running_absolute":    hsr,
        "hsr_per_minute_absolute":        round(hsr / mins, 2),
        "hml_distance":                   rnd(800, 2100),
        "hmld_per_minute":                rnd(10, 32),
        "accelerations_absolute":         rnd_int(40, 130),
        "decelerations_absolute":         rnd_int(40, 130),
        "accelerations_per_min_absolute": rnd(0.5, 2.2),
        "decels_per_min_absolute":        rnd(0.5, 2.2),
        "step_balance":                   rnd(0.8, 1.2),
        "collision_load":                 coll_load,
        "collisions":                     rnd_int(6, 32) if avant else rnd_int(2, 15),
        "dynamic_stress_load":            rnd(200, 650),
        "metabolic_distance_absolute":    rnd(1600, 4200),
        "average_metabolic_power":        rnd(8.0, 15.5),
        "max_acceleration":               rnd(3.0, 6.2),
        "max_deceleration":               rnd(3.0, 6.2),
        "acute":                          rnd_int(200, 620),
        "chronic":                        rnd_int(280, 520),
        "acute_chronic_ratio":            rnd(0.65, 1.35),
    }

# ── Actions match ──────────────────────────────────────────────────────────────

def actions_match(poste: str) -> dict:
    avant = poste in AVANTS_POSTES

    def bloc(lo, hi):
        t = rnd_int(lo, hi)
        p = rnd_int(max(0, int(t * 0.45)), t)
        n = rnd_int(0, t - p)
        return t, p, n, t - p - n

    result = {}
    specs = {
        "passes":         (5, 28) if not avant else (3, 14),
        "porteur":        (5, 22),
        "plaquages":      (6, 20) if avant else (3, 13),
        "soutiens":       (5, 22),
        "contacts":       (4, 16),
        "ballons_perdus": (0, 5),
        "fautes":         (0, 5),
        "def_battus":     (0, 6),
        "duels_aeriens":  (0, 6),
        "franchissements":(0, 5),
        "interceptions":  (0, 3),
        "essais":         (0, 2),
        "passes_contact": (0, 6),
        "grattages":      (0, 5) if avant else (0, 2),
        "contests":       (0, 4),
        "contre_rucks":   (0, 5),
    }
    for action, (lo, hi) in specs.items():
        t, p, n, neu = bloc(lo, hi)
        result[f"{action}_total"]   = t
        result[f"{action}_positif"] = p
        result[f"{action}_negatif"] = n
        result[f"{action}_neutre"]  = neu

    jap = rnd_int(0, 9) if not avant else rnd_int(0, 3)
    result.update({
        "jap_total":              jap,
        "jap_positif":            rnd_int(0, jap),
        "jap_negatif":            rnd_int(0, jap),
        "jap_neutre":             0,
        "jap_depuis_propre_22m":  rnd_int(0, 2),
        "jap_depuis_mi_terrain":  rnd_int(0, 3),
        "jap_depuis_camp_adverse":rnd_int(0, 2),
        "jap_en_touche":          rnd_int(0, 4),
        "jap_camp_adverse":       rnd_int(0, 3),
        "minutes_jouees":         rnd_int(40, 80),
        "buts_penalite_reussis":  0,
        "buts_penalite_rates":    0,
        "buts_transfo_reussies":  0,
        "buts_transfo_ratees":    0,
        "buts_drop_reussis":      0,
        "buts_drop_rates":        0,
        "cartons_jaunes":         1 if random.random() < 0.04 else 0,
        "cartons_rouges":         0,
    })
    return result

# ── GPS entraînement ───────────────────────────────────────────────────────────

SESSION_INTENSITY = {"J+2": 0.45, "Reprise": 0.55, "J-2": 0.90, "J-1": 0.65}

def gps_entrainement(poste: str, session_type: str) -> dict:
    avant     = poste in AVANTS_POSTES
    intensity = SESSION_INTENSITY.get(session_type, 0.75)
    base_dist = 3800 if avant else 5200
    dist      = rnd(base_dist * intensity * 0.85, base_dist * intensity * 1.15)
    max_spd   = rnd(22.0, 28.0) if avant else rnd(26.0, 33.5)
    sprints   = rnd_int(int(4 * intensity), int(22 * intensity))
    mins      = rnd_int(50, 95)
    return {
        "session_type":                 session_type,
        "total_time":                   f"00:{mins}:00",
        "total_distance":               round(dist, 1),
        "distance_per_min":             round(dist / mins, 2),
        "max_speed":                    max_spd,
        "sprints":                      sprints,
        "sprint_distance":              round(sprints * rnd(14, 26), 1),
        "high_speed_running_absolute":  rnd(180, 650),
        "hsr_per_minute_absolute":      rnd(2.5, 9.0),
        "hml_distance":                 rnd(450, 1600),
        "hmld_per_minute":              rnd(6.0, 20.0),
        "accelerations_absolute":       rnd_int(25, 95),
        "decelerations_absolute":       rnd_int(25, 95),
        "dynamic_stress_load":          rnd(90, 420),
        "metabolic_distance_absolute":  rnd(700, 2700),
        "max_acceleration":             rnd(2.4, 5.2),
        "acute":                        rnd_int(140, 520),
        "chronic":                      rnd_int(190, 480),
        "acute_chronic_ratio":          rnd_int(55, 145),  # stocké ×100 (schéma INTEGER)
    }

# ── Collisions ─────────────────────────────────────────────────────────────────

def gen_collisions(joueur_id: int, match_id: int, poste: str) -> list[dict]:
    avant = poste in AVANTS_POSTES
    n = rnd_int(8, 28) if avant else rnd_int(3, 14)
    records = []
    used_times = set()
    for _ in range(n):
        # Génère un start_time unique (format MM:SS)
        for _ in range(50):
            mi = rnd_int(1, 2)
            mins = rnd_int(0, 39)
            secs = rnd_int(0, 59)
            key = (mi, mins, secs)
            if key not in used_times:
                used_times.add(key)
                break
        start = f"{mins:02d}:{secs:02d}"
        dur_s = rnd_int(1, 4)
        end_s = secs + dur_s
        end   = f"{mins:02d}:{min(end_s, 59):02d}"
        records.append({
            "joueur_id":            joueur_id,
            "match_id":             match_id,
            "mi_temps":             mi,
            "start_time":           start,
            "end_time":             end,
            "duration":             f"00:0{dur_s}",
            "collision_load":       rnd(5.0, 45.0),
            "time_to_feet":         rnd(0.5, 4.5),
            "post_collision_accel": rnd(0.5, 5.5),
            "_source_file":         f"demo_collisions_match{match_id}",
        })
    return records

# ── Mêlées ─────────────────────────────────────────────────────────────────────

POSTES_MELEE = {"Pilier gauche", "Talonneur", "Pilier droit", "2ème ligne"}

def gen_melees(joueur_id: int, match_id: int, poste: str) -> list[dict]:
    if poste not in POSTES_MELEE:
        return []
    n_scrums = rnd_int(8, 16)
    records = []
    for scrum_num in range(1, n_scrums + 1):
        mi = 1 if scrum_num <= n_scrums // 2 else 2
        mins = rnd_int(0, 39)
        secs = rnd_int(0, 59)
        records.append({
            "joueur_id":              joueur_id,
            "match_id":               match_id,
            "scrum_id":               (match_id - 1) * 20 + scrum_num,
            "scrum_num":              scrum_num,
            "mi_temps":               mi,
            "start_time":             f"{mins:02d}:{secs:02d}",
            "end_time":               f"{mins:02d}:{min(secs + rnd_int(3, 8), 59):02d}",
            "duration":               f"00:0{rnd_int(3, 8)}",
            "avg_total_impact":       str(rnd(50, 300)),
            "avg_front_row_impact":   str(rnd(60, 200)),
            "avg_second_row_impact":  str(rnd(40, 180)),
            "avg_back_row_impact":    str(rnd(30, 150)),
            "scrum_sync_time":        str(rnd(0.1, 0.5)),
            "impact":                 str(rnd(80, 350)),
            "sync_time":              str(rnd(0.1, 0.5)),
            "scrum_load":             str(rnd(100, 400)),
            "time_to_feet":           str(rnd(0.5, 3.0)),
            "post_scrum_accel":       str(rnd(0.5, 4.5)),
            "_source_file":           f"demo_melees_match{match_id}",
        })
    return records

# ── Touches ────────────────────────────────────────────────────────────────────

def gen_touches(match_id: int) -> list[dict]:
    records = []
    for equipe, est_rec in [("REC", 1), ("ADV", 0)]:
        n = rnd_int(10, 22)
        for k in range(n):
            records.append({
                "match_id":  match_id,
                "equipe":    equipe,
                "est_rec":   est_rec,
                "resultat":  random.choice(["gagnée", "perdue", "contestée"]),
                "alignement":random.choice(["3", "4", "5", "6", "7"]),
                "sortie":    random.choice(["sauteur", "hors jeu", "sortie directe"]),
                "zone":      random.choice(["22m", "mi_terrain", "22m_adv"]),
                "start_sec": rnd(0, 4800),
                "_source_file": f"demo_touches_match{match_id}",
            })
    return records

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("🏉 Génération des données demo Rugby Data Hub\n")

    # 1. Suppression
    delete_all()
    print()

    # 2. Joueurs
    print("📤 Insertion des données…")
    insert_table("joueur", joueurs_data)

    # Récupère les joueur_id assignés par Supabase
    joueurs_inserted = client.table("joueur").select("joueur_id, nom, poste_principal").execute().data
    joueur_map = {j["nom"]: (j["joueur_id"], j["poste_principal"]) for j in joueurs_inserted}

    # 3. Matchs
    matchs_records = gen_matchs()
    insert_table("match", matchs_records)
    match_ids = [m["match_id"] for m in matchs_records]

    # 4. perf_match : 22 joueurs par match (rotation)
    squad_size = len(joueurs_data)
    perf_match_records = []
    for match_id in match_ids:
        # Tire 22 joueurs au hasard pour ce match
        joueurs_match = random.sample(list(joueur_map.items()), min(22, squad_size))
        source_gps     = f"demo_gps_matchs_match{match_id}"
        source_actions = f"demo_actions_matchs_match{match_id}"
        for nom, (joueur_id, poste) in joueurs_match:
            row = {"joueur_id": joueur_id, "match_id": match_id}
            row.update(gps_match(poste))
            row.update(actions_match(poste))
            row["_source_file_gps"]     = source_gps
            row["_source_file_actions"] = source_actions
            perf_match_records.append(row)
    insert_table("perf_match", perf_match_records)

    # 5. perf_entrainement : 4 séances par cycle de match
    perf_entr_records = []
    for i, match_id in enumerate(match_ids):
        match_date = start_date + timedelta(weeks=2 * i)
        sessions = [
            (match_date - timedelta(days=6), "J+2",   f"Mercredi {match_date - timedelta(days=6)}"),
            (match_date - timedelta(days=5), "Reprise",f"Lundi {match_date - timedelta(days=5)}"),
            (match_date - timedelta(days=3), "J-2",   f"Jeudi {match_date - timedelta(days=3)}"),
            (match_date - timedelta(days=1), "J-1",   f"Vendredi {match_date - timedelta(days=1)}"),
        ]
        for sess_date, session_type, titre in sessions:
            source = f"gps_entrainements_{sess_date.isoformat()}_{session_type}.csv"
            # 26-30 joueurs à l'entraînement
            joueurs_entr = random.sample(list(joueur_map.items()), rnd_int(26, min(30, squad_size)))
            for nom, (joueur_id, poste) in joueurs_entr:
                row = {
                    "joueur_id":    joueur_id,
                    "seance_type":  random.choice(["collectif", "mixé", "séparé"]),
                    "titre":        titre,
                    "date":         sess_date.isoformat(),
                    "_source_file": source,
                }
                row.update(gps_entrainement(poste, session_type))
                perf_entr_records.append(row)
    insert_table("perf_entrainement", perf_entr_records)

    # 6. Collisions
    collision_records = []
    for row in perf_match_records:
        joueur_id = row["joueur_id"]
        match_id  = row["match_id"]
        nom = next(n for n, (jid, _) in joueur_map.items() if jid == joueur_id)
        _, poste = joueur_map[nom]
        collision_records.extend(gen_collisions(joueur_id, match_id, poste))
    insert_table("collision", collision_records)

    # 7. Mêlées
    melee_records = []
    for row in perf_match_records:
        joueur_id = row["joueur_id"]
        match_id  = row["match_id"]
        nom = next(n for n, (jid, _) in joueur_map.items() if jid == joueur_id)
        _, poste = joueur_map[nom]
        melee_records.extend(gen_melees(joueur_id, match_id, poste))
    insert_table("melee", melee_records)

    # 8. Touches
    touche_records = []
    for match_id in match_ids:
        touche_records.extend(gen_touches(match_id))
    insert_table("touche", touche_records)

    print("\n✅ Données demo générées avec succès.")
    print(f"   {len(joueurs_data)} joueurs")
    print(f"   {len(matchs_records)} matchs")
    print(f"   {len(perf_match_records)} lignes perf_match")
    print(f"   {len(perf_entr_records)} lignes perf_entrainement")
    print(f"   {len(collision_records)} collisions")
    print(f"   {len(melee_records)} mêlées")
    print(f"   {len(touche_records)} touches")


if __name__ == "__main__":
    main()

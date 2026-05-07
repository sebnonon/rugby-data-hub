"""
Génère des séances d'entraînement synthétiques réalistes pour la saison courante
et les insère dans la base Supabase courante (.env).

Cadence cible :
  - Semaine de match : 5 séances (Lun J+2, Mar Reprise, Mer Reprise, Jeu J-2, Ven J-1)
  - Semaine sans match : 5 séances (Lun Reprise, Mar Reprise, Mer J-2, Jeu Reprise, Ven J-1)

Idempotent : ne ré-insère pas une séance déjà présente sur (date, session_type).
Toutes les insertions portent un _source_file préfixé "synthetic_" pour suppression ciblée :

    DELETE FROM perf_entrainement WHERE _source_file LIKE 'synthetic_%';

Usage :
    python pipeline/generate_synthetic_entrainements.py --dry-run
    python pipeline/generate_synthetic_entrainements.py
    python pipeline/generate_synthetic_entrainements.py --yes
"""

import argparse
import random
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from supabase_client import get_client  # noqa: E402

random.seed(42)

BATCH = 500

AVANTS_POSTES = {
    "Pilier gauche", "Talonneur", "Pilier droit",
    "2ème ligne", "3ème ligne aile", "3ème ligne centre",
}

JOURS_FR = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]

# Pattern hebdomadaire : (offset_lundi, session_type, intensité)
PATTERN_MATCH_WEEK = [
    (0, "J+2",     0.45),  # Lundi
    (1, "Reprise", 0.55),  # Mardi
    (2, "Reprise", 0.65),  # Mercredi
    (3, "J-2",     0.90),  # Jeudi
    (4, "J-1",     0.55),  # Vendredi
]

PATTERN_NO_MATCH_WEEK = [
    (0, "Reprise", 0.50),  # Lundi
    (1, "Reprise", 0.65),  # Mardi
    (2, "J-2",     0.85),  # Mercredi
    (3, "Reprise", 0.70),  # Jeudi
    (4, "J-1",     0.55),  # Vendredi
]


def rnd(lo, hi, d=1):
    return round(random.uniform(lo, hi), d)


def rnd_int(lo, hi):
    return random.randint(lo, hi)


def get_saison(d: date) -> str:
    return f"{d.year}/{d.year+1}" if d.month >= 8 else f"{d.year-1}/{d.year}"


def saison_bornes(saison: str) -> tuple[date, date]:
    y0 = int(saison.split("/")[0])
    return date(y0, 8, 1), date(y0 + 1, 7, 31)


def gps_entrainement(poste: str | None, session_type: str, intensity: float) -> dict:
    avant = (poste or "") in AVANTS_POSTES
    base_dist = 3800 if avant else 5200
    dist = rnd(base_dist * intensity * 0.85, base_dist * intensity * 1.15)
    max_spd = rnd(22.0, 28.0) if avant else rnd(26.0, 33.5)
    sprints = rnd_int(max(1, int(4 * intensity)), max(2, int(22 * intensity)))
    mins = rnd_int(50, 95)
    return {
        "session_type":                 session_type,
        "total_time":                   f"00:{mins:02d}:00",
        "total_distance":               round(dist, 1),
        "distance_per_min":             round(dist / mins, 2),
        "max_speed":                    max_spd,
        "sprints":                      sprints,
        "sprint_distance":              round(sprints * rnd(14, 26), 1),
        "high_speed_running_absolute":  rnd(180 * intensity, 650 * intensity),
        "hsr_per_minute_absolute":      rnd(2.5 * intensity, 9.0 * intensity),
        "hml_distance":                 rnd(450 * intensity, 1600 * intensity),
        "hmld_per_minute":              rnd(6.0 * intensity, 20.0 * intensity),
        "accelerations_absolute":       rnd_int(int(25 * intensity), int(95 * intensity) + 1),
        "decelerations_absolute":       rnd_int(int(25 * intensity), int(95 * intensity) + 1),
        "dynamic_stress_load":          rnd(90 * intensity, 420 * intensity),
        "metabolic_distance_absolute":  rnd(700 * intensity, 2700 * intensity),
        "max_acceleration":             rnd(2.4, 5.2),
        "acute":                        rnd_int(140, 520),
        "chronic":                      rnd_int(190, 480),
        "acute_chronic_ratio":          rnd_int(75, 130),
    }


def fetch_all(client, table: str, columns: str, filters: dict | None = None):
    """Pagination simple pour contourner la limite par défaut de Supabase (1000)."""
    out, page_size, offset = [], 1000, 0
    while True:
        q = client.table(table).select(columns)
        if filters:
            for k, v in filters.items():
                q = q.gte(k, v[0]).lte(k, v[1])
        res = q.range(offset, offset + page_size - 1).execute()
        rows = res.data or []
        out.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Calcule sans insérer.")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation interactive.")
    args = parser.parse_args()

    t0 = time.time()
    client = get_client()
    today = date.today()
    saison = get_saison(today)
    s_start, s_end = saison_bornes(saison)
    print(f"📅 Saison courante : {saison}  ({s_start} → {s_end})")
    print(f"📅 Aujourd'hui     : {today}\n")

    # 1. Matches de la saison
    matches = fetch_all(
        client, "match", "match_id, date",
        filters={"date": (s_start.isoformat(), s_end.isoformat())},
    )
    match_dates = sorted({datetime.fromisoformat(m["date"]).date() for m in matches if m.get("date")})
    print(f"🏉 Matches saison : {len(match_dates)}")
    if match_dates:
        print(f"   premier : {match_dates[0]}   dernier : {match_dates[-1]}")

    # 2. Joueurs
    joueurs = client.table("joueur").select("joueur_id, nom, poste_principal").execute().data or []
    print(f"👥 Joueurs        : {len(joueurs)}")
    if not joueurs:
        print("❌ Aucun joueur en base — abandon.")
        return

    # 3. Séances déjà présentes (saison) — par jour
    existing = fetch_all(
        client, "perf_entrainement", "date, session_type",
        filters={"date": (s_start.isoformat(), s_end.isoformat())},
    )
    existing_dates = set()  # dates avec au moins une séance
    for r in existing:
        if r.get("date"):
            existing_dates.add(datetime.fromisoformat(r["date"]).date())
    print(f"📊 Jours déjà couverts par une séance (saison) : {len(existing_dates)}\n")

    # 4. Détermine la plage à couvrir
    if match_dates:
        first_day = match_dates[0] - timedelta(days=match_dates[0].weekday())  # lundi de la semaine du 1er match
        last_day = min(today, match_dates[-1] + timedelta(days=14))
    else:
        first_day = s_start
        last_day = today
    last_day = last_day - timedelta(days=last_day.weekday()) + timedelta(days=6)  # dimanche

    # 5. Itère semaine par semaine
    week_match = {(d - timedelta(days=d.weekday())) for d in match_dates}
    cursor = first_day - timedelta(days=first_day.weekday())  # lundi
    plan_summary = []
    rows_to_insert = []

    while cursor <= last_day:
        is_match_week = cursor in week_match
        pattern = PATTERN_MATCH_WEEK if is_match_week else PATTERN_NO_MATCH_WEEK
        quota = 5

        # Compte les jours de la semaine déjà couverts par une séance (existante ou ajoutée)
        week_days = {cursor + timedelta(days=k) for k in range(7)}
        already = sum(1 for d in week_days if d in existing_dates)
        deficit = max(0, quota - already)

        added_this_week = 0
        if deficit > 0:
            # Parcourt le pattern et ajoute uniquement sur des jours libres, jusqu'à combler
            for offset, stype, intensity in pattern:
                if added_this_week >= deficit:
                    break
                seance_date = cursor + timedelta(days=offset)
                if seance_date > today:
                    continue
                if seance_date in existing_dates:
                    continue
                joueurs_seance = joueurs
                jour_fr = JOURS_FR[seance_date.weekday()]
                source = f"synthetic_{seance_date.isoformat()}_{stype}.csv"
                for j in joueurs_seance:
                    row = {
                        "joueur_id":    j["joueur_id"],
                        "seance_type":  random.choice(["collectif", "mixé", "séparé"]),
                        "titre":        f"{jour_fr} {seance_date.strftime('%d/%m')} — {stype}",
                        "date":         seance_date.isoformat(),
                        "_source_file": source,
                    }
                    row.update(gps_entrainement(j.get("poste_principal"), stype, intensity))
                    rows_to_insert.append(row)
                added_this_week += 1
                existing_dates.add(seance_date)  # évite doublon dans la même run
        plan_summary.append((cursor, is_match_week, already, added_this_week, quota))
        cursor += timedelta(days=7)

    # 6. Récapitulatif
    n_match_weeks = sum(1 for _, m, _, _, _ in plan_summary if m)
    n_no_match_weeks = len(plan_summary) - n_match_weeks
    n_seances = sum(n for _, _, _, n, _ in plan_summary)
    print(f"📈 Plan de génération")
    print(f"   semaines       : {len(plan_summary)} ({n_match_weeks} de match, {n_no_match_weeks} sans match)")
    print(f"   séances ajoutées : {n_seances}")
    print(f"   lignes joueur×séance : {len(rows_to_insert)}\n")

    if not rows_to_insert:
        print("✅ Rien à insérer — la base est déjà conforme à la cadence cible.")
        return

    print("Détail par semaine (existantes / quota → +ajoutées) :")
    for monday, is_m, already, n, quota in plan_summary:
        tag = "MATCH " if is_m else "      "
        marker = "✓" if n == 0 else f"+{n}"
        print(f"   {monday}  [{tag}]  {already}/{quota}  {marker}")
    print()

    if args.dry_run:
        print("🔍 --dry-run actif : aucune insertion.")
        return

    if not args.yes:
        rep = input(f"Insérer {len(rows_to_insert)} lignes dans perf_entrainement ? [y/N] ").strip().lower()
        if rep not in ("y", "yes", "o", "oui"):
            print("⛔ Annulé.")
            return

    # 7. Insertion par batches
    print(f"📤 Insertion en cours (batches de {BATCH})…")
    for i in range(0, len(rows_to_insert), BATCH):
        batch = rows_to_insert[i:i + BATCH]
        client.table("perf_entrainement").insert(batch).execute()
        print(f"   {i + len(batch):>6} / {len(rows_to_insert)}")

    elapsed = time.time() - t0
    print(f"\n✅ Terminé en {elapsed:.1f}s — {len(rows_to_insert)} lignes insérées.")
    print("   Pour annuler : DELETE FROM perf_entrainement WHERE _source_file LIKE 'synthetic_%';")


if __name__ == "__main__":
    main()

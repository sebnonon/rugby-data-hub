"""
Migration SQLite → Supabase.
Lit les tables depuis rugby_data.db et les pousse dans Supabase par batch.

Usage :
    source venv/bin/activate
    python pipeline/migrate_to_supabase.py
"""

import sqlite3
import numpy as np
import pandas as pd
from pathlib import Path

from supabase_client import get_client

DB_PATH = Path(__file__).parent.parent / "data" / "db" / "rugby_data.db"

# Ordre d'insertion respectant les FK (tables de référence en premier)
TABLES = [
    "joueur",
    "match",
    "perf_entrainement",
    "perf_match",
    "collision",
    "melee",
    "touche",
]

# Ordre de suppression : inverse des FK (tables de faits en premier)
TABLES_DELETE_ORDER = [
    "touche",
    "melee",
    "collision",
    "perf_match",
    "perf_entrainement",
    "match",
    "joueur",
]

BATCH_SIZE = 500

# Colonne PK de chaque table (pour le DELETE)
TABLE_PK = {
    "joueur":            "joueur_id",
    "match":             "match_id",
    "perf_entrainement": "perf_entr_id",
    "perf_match":        "perf_match_id",
    "collision":         "collision_id",
    "melee":             "melee_id",
    "touche":            "touche_id",
}

# Colonnes PK auto-générées par SERIAL — à exclure de l'INSERT
# (joueur_id et match_id sont des FK dans les tables de faits, on les garde)
TABLE_SERIAL_PK = {
    "joueur":            "joueur_id",
    "perf_entrainement": "perf_entr_id",
    "perf_match":        "perf_match_id",
    "collision":         "collision_id",
    "melee":             "melee_id",
    "touche":            "touche_id",
}


def clean_records(df: pd.DataFrame) -> list[dict]:
    """
    Convertit un DataFrame en liste de dicts compatibles JSON.
    - NaN / NaT → None
    - numpy int/float → types Python natifs
    """
    records = []
    for row in df.to_dict(orient="records"):
        clean = {}
        for k, v in row.items():
            if isinstance(v, (np.integer,)):
                clean[k] = int(v)
            elif isinstance(v, (np.floating, float)):
                fv = float(v)
                if np.isnan(fv):
                    clean[k] = None
                elif fv == int(fv):
                    # Convertit 1.0 → 1 pour les colonnes INTEGER de Supabase
                    clean[k] = int(fv)
                else:
                    clean[k] = fv
            elif pd.isna(v) if not isinstance(v, (list, dict)) else False:
                clean[k] = None
            else:
                clean[k] = v
        records.append(clean)
    return records


def delete_all(client) -> None:
    """
    Supprime toutes les tables dans l'ordre inverse des FK
    pour éviter les violations de contraintes référentielles.
    """
    print("🗑️  Suppression des données existantes…")
    for table in TABLES_DELETE_ORDER:
        pk = TABLE_PK[table]
        client.table(table).delete().neq(pk, 0).execute()
        print(f"  ✅ {table} vidée")


def drop_serial_pk(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Supprime la colonne PK SERIAL avant l'INSERT pour laisser Supabase la générer."""
    pk_col = TABLE_SERIAL_PK.get(table_name)
    if pk_col and pk_col in df.columns:
        df = df.drop(columns=[pk_col])
    return df


def insert_table(client, table_name: str, records: list[dict]) -> None:
    """Insère les données par batch dans la table Supabase."""
    total = len(records)
    if total == 0:
        print(f"  {table_name}: vide, rien à insérer")
        return

    for i in range(0, total, BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        client.table(table_name).insert(batch).execute()
        print(f"  {table_name}: {min(i + BATCH_SIZE, total)}/{total}", end="\r")

    print(f"  ✅ {table_name}: {total} lignes insérées          ")


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Base SQLite introuvable : {DB_PATH}\n"
            "Lance d'abord : python pipeline/pipeline_rec.py"
        )

    conn = sqlite3.connect(DB_PATH)
    client = get_client()
    print(f"✅ Connexion SQLite : {DB_PATH}")
    print(f"✅ Connexion Supabase OK\n")

    # Phase 1 : suppression complète dans l'ordre inverse des FK
    delete_all(client)

    # Phase 2 : insertion dans l'ordre respectant les FK
    print("\n📤 Insertion des données…")
    for table in TABLES:
        print(f"  {table}…")
        try:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            df = drop_serial_pk(df, table)
            records = clean_records(df)
            insert_table(client, table, records)
        except Exception as e:
            print(f"  ❌ Erreur sur {table} : {e}")

    conn.close()
    print("\n✅ Migration terminée.")


if __name__ == "__main__":
    main()

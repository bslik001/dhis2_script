#!/usr/bin/env python3
"""
pivot_tracked_and_stage.py

Génère un fichier Excel à partir d’exports DHIS2.

Fonctionnalités :
- Onglet 1 : Tracked Entity Instances (pivoté)
- Onglets suivants : un onglet par Program Stage
- Suppression automatique des onglets vides
- Reprise automatique après interruption (state file)
- Colonnes Excel automatiquement ajustées à la largeur du contenu
- Affichage de progression pour le premier onglet
- Argument --strict pour garder uniquement les colonnes essentielles
- Configuration hiérarchique : CLI > .env > valeurs par défaut
"""

import argparse
import json
import math
import os
import sys
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from openpyxl.utils import get_column_letter
from openpyxl import load_workbook

# ==========================================================
# ======================= ARGUMENTS ========================
# ==========================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Génère un fichier Excel DHIS2 à partir de CSV.\n\n"
            "Priorité de configuration :\n"
            "1. Arguments CLI\n"
            "2. Variables d’environnement (.env)\n"
            "3. Valeurs par défaut\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument("--tracked-input", help="CSV des Tracked Entity Instances",
                        default=os.getenv("TRACKED_OUTPUT"))
    parser.add_argument("--stage-input", help="CSV des événements (Program Stages)",
                        default=os.getenv("PIVOT_INPUT"))
    parser.add_argument("--output", help="Fichier Excel de sortie",
                        default=os.getenv("MERGED_PIVOT_OUTPUT", "pivot_final.xlsx"))
    parser.add_argument("--base-url", help="URL de base de l’API DHIS2",
                        default=os.getenv("PIVOT_BASE_URL"))
    parser.add_argument("--token", help="Token API DHIS2",
                        default=os.getenv("PIVOT_TOKEN"))
    parser.add_argument("--program", help="UID du programme DHIS2",
                        default=os.getenv("DOWNLOAD_PROGRAM"))
    parser.add_argument("--aggfunc", help="Fonction d’agrégation pandas (first, last, max, etc.)",
                        default=os.getenv("PIVOT_AGGFUNC", "first"))
    parser.add_argument("--mapping-file", help="Fichier cache UID → displayName des dataElements",
                        default=os.getenv("PIVOT_MAPPING_FILE", "utils/dataelement_mapping.json"))
    parser.add_argument("--state-file", help="Fichier de reprise d’état (progression)",
                        default=os.getenv("PIVOT_STATE_FILE", "utils/progress_state.json"))
    parser.add_argument("--debug-stages", action="store_true",
                        help="Affiche des informations détaillées sur les Program Stages")
    parser.add_argument("--strict", action="store_true",
                        help="Mode strict : ne garder que trackedEntityInstance, ID, serial_number, date, parent_consent")

    return parser.parse_args()

def validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser):
    # Validation intelligente :
    # - Cas A : l'utilisateur a fourni les chemins CSV (tracked_input & stage_input) -> c'est suffisant
    # - Cas B : si un ou plusieurs CSV manquent, on autorise la récupération via l'API DHIS2,
    #          auquel cas base_url + token + program sont obligatoires

    csvs_provided = bool(args.tracked_input) and bool(args.stage_input)
    if csvs_provided:
        return

    # Si on arrive ici, au moins un CSV est manquant -> vérifier qu'on peut télécharger
    required_for_download = {
        "base_url": args.base_url,
        "token": args.token,
        "program": args.program,
    }
    missing = [name for name, value in required_for_download.items() if not value]
    if missing:
        parser.error(
            "Paramètres manquants : soit fournissez --tracked-input et --stage-input, "
            "soit fournissez --base-url, --token et --program pour permettre le téléchargement"
        )

# ==========================================================
# ======================= ÉTAT =============================
# ==========================================================

def load_state(state_file: str) -> dict:
    if os.path.exists(state_file):
        with open(state_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"completed_stages": []}

def save_state(state_file: str, state: dict):
    Path(state_file).parent.mkdir(parents=True, exist_ok=True)
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def clear_state(state_file: str):
    if os.path.exists(state_file):
        os.remove(state_file)

# ==========================================================
# ======================= API DHIS2 =========================
# ==========================================================

def safe_get(url: str, token: str, params: dict | None = None) -> dict:
    headers = {"Authorization": f"ApiToken {token}"}
    response = requests.get(url, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    return response.json()

def get_program_stages(base_url: str, token: str, program_id: str) -> list[dict]:
    url = f"{base_url}/programs/{program_id}/programStages.json"
    params = {"fields": "id,displayName,sortOrder", "paging": "false"}
    data = safe_get(url, token, params)
    return sorted(data.get("programStages", []), key=lambda s: s.get("sortOrder", math.inf))

def get_stage_dataelements(base_url: str, token: str, stage_id: str) -> list[str]:
    url = f"{base_url}/programStages/{stage_id}.json"
    params = {"fields": "programStageDataElements[dataElement[id,displayName],sortOrder]", "paging": "false"}
    data = safe_get(url, token, params)
    elements = sorted(data.get("programStageDataElements", []), key=lambda x: x.get("sortOrder", math.inf))
    return [e["dataElement"]["displayName"] for e in elements if e.get("dataElement")]

# ==========================================================
# ================== LOGICHE DE TELECHARGEMENT ==============
# ==========================================================

def build_url(base_url: str, params: dict) -> str:
    """Reconstruit une URL en ajoutant la query string si besoin.

    Cette version est utilisée pour les téléchargements CSV (tracked/entity/events).
    """
    from urllib.parse import urlencode
    query_string = urlencode(params)
    return f"{base_url}?{query_string}" if query_string else base_url


def download_stream_to_file(full_url: str, output_file: str, token: str):
    """Télécharge une ressource HTTP en streaming vers un fichier local.

    On utilise un header ApiToken et on affiche une barre de progression (tqdm).
    """
    headers = {"Authorization": f"ApiToken {token}"}
    print(f"URL générée pour téléchargement : {full_url}")
    try:
        with requests.get(full_url, headers=headers, stream=True, timeout=60) as response:
            response.raise_for_status()
            total_size = int(response.headers.get("content-length", 0))
            from tqdm import tqdm
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "wb") as f, tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="Téléchargement",
            ) as bar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))
        print(f"✅ Fichier téléchargé : {output_file}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors du téléchargement : {e}")
        raise


def download_dhis2_tracked_if_missing(tracked_path: str, base_url: str, token: str, program: str):
    """Télécharge `trackedEntityInstances.csv` si le fichier local est absent.

    - Construit l'endpoint en partant de `base_url` (ajoute `/trackedEntityInstances.csv` si nécessaire)
    - Utilise quelques paramètres utiles (program, dates) lus depuis les variables d'environnement
    - Cette fonction ne modifie pas les scripts originaux `download_tracked.py` ou `download.py`,
      elle reprend simplement leur logique ici pour l'intégration.
    """
    if os.path.exists(tracked_path):
        print(f"ℹ️ Fichier tracked déjà présent : {tracked_path}")
        return

    if not token:
        raise RuntimeError("Token manquant : impossible de télécharger les trackedEntityInstances")

    # Déterminer URL de base pour tracked (si base_url ne contient pas .csv)
    if base_url.endswith(".csv"):
        tracked_base = base_url
    else:
        tracked_base = base_url.rstrip("/") + "/trackedEntityInstances.csv"

    # Paramètres communs : on prend ce qui est disponible dans l'environnement
    params = {}
    if program:
        params["program"] = program
    # dates possibles
    start = os.getenv("DOWNLOAD_START_DATE") or os.getenv("TRACKED_PROGRAM_START_DATE")
    end = os.getenv("DOWNLOAD_END_DATE") or os.getenv("TRACKED_PROGRAM_END_DATE")
    if start:
        params["programStartDate"] = start
    if end:
        params["programEndDate"] = end

    full_url = build_url(tracked_base, params)
    download_stream_to_file(full_url, tracked_path, token)


def download_dhis2_events_if_missing(events_path: str, base_url: str, token: str, program: str):
    """Télécharge `events.csv` (export events) si le fichier local est absent.

    Construit l'endpoint `/events.csv` et utilise des paramètres courants (orgUnit, program, dates...).
    """
    if os.path.exists(events_path):
        print(f"ℹ️ Fichier events déjà présent : {events_path}")
        return

    if not token:
        raise RuntimeError("Token manquant : impossible de télécharger les events")

    if base_url.endswith(".csv"):
        events_base = base_url
    else:
        events_base = base_url.rstrip("/") + "/events.csv"

    params = {}
    # paramètres recherchés dans .env
    org = os.getenv("DOWNLOAD_ORG_UNIT")
    if org:
        params["orgUnit"] = org
    if program:
        params["program"] = program
    stage = os.getenv("DOWNLOAD_PROGRAM_STAGE")
    if stage:
        params["programStage"] = stage
    start = os.getenv("DOWNLOAD_START_DATE")
    end = os.getenv("DOWNLOAD_END_DATE")
    if start:
        params["startDate"] = start
    if end:
        params["endDate"] = end
    # quelques options utiles par défaut
    params.setdefault("ouMode", os.getenv("DOWNLOAD_OU_MODE", "DESCENDANTS"))
    params.setdefault("skipPaging", os.getenv("DOWNLOAD_SKIP_PAGING", "true"))

    full_url = build_url(events_base, params)
    download_stream_to_file(full_url, events_path, token)

# ==========================================================
# ===================== MAPPING =============================
# ==========================================================

def load_mapping(mapping_file: str) -> dict:
    if os.path.exists(mapping_file):
        with open(mapping_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_mapping(mapping_file: str, mapping: dict):
    Path(mapping_file).parent.mkdir(parents=True, exist_ok=True)
    with open(mapping_file, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)

def build_de_mapping_from_api(base_url: str, token: str) -> dict:
    url = f"{base_url}/dataElements.json"
    params = {"paging": "false", "fields": "id,displayName"}
    data = safe_get(url, token, params)
    return {de["id"]: de["displayName"] for de in data.get("dataElements", [])}

# ==========================================================
# ======================= PIVOTS ============================
# ==========================================================

def pivot_tracked_df(input_file: str, aggfunc: str) -> pd.DataFrame:
    df = pd.read_csv(input_file, dtype=str)
    pivot = df.pivot_table(index="trackedEntityInstance", columns="displayName", values="value", aggfunc=aggfunc).reset_index().fillna("")
    pivot.insert(pivot.columns.get_loc("trackedEntityInstance")+1, "ID", range(1, len(pivot)+1))
    return pivot

# ==========================================================
# ===================== EXCEL UTIL =========================
# ==========================================================

def auto_adjust_column_width(ws):
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                value = str(cell.value) if cell.value is not None else ""
                if len(value) > max_length:
                    max_length = len(value)
            except:
                pass
        ws.column_dimensions[column].width = max_length + 2

def write_with_progress(df: pd.DataFrame, writer, sheet_name: str, chunk_size: int = 50):
    total = len(df)
    if total == 0:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        return
    df.head(0).to_excel(writer, sheet_name=sheet_name, index=False)
    ws = writer.sheets[sheet_name]
    for start in range(0, total, chunk_size):
        end = min(start + chunk_size, total)
        chunk = df.iloc[start:end]
        for r_idx, row in enumerate(chunk.values, start=start+2):
            for c_idx, value in enumerate(row, start=1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        progress = (end/total)*100
        sys.stdout.write(f"\r   → Lignes {start+1} à {end} / {total} ({progress:.1f}%)")
        sys.stdout.flush()
    auto_adjust_column_width(ws)
    print(f"\n✅ Onglet '{sheet_name}' écrit avec succès\n")

# ==========================================================
# ========================= MAIN ============================
# ==========================================================

def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    args = parse_args()
    validate_args(args, parser)

    # Chargement de l’état
    state = load_state(args.state_file)

    # ------------------------------------------------------------------
    # Avant de lancer les pivots, s'assurer que les fichiers CSV d'entrée existent.
    # - Si `--tracked-input` ou `--stage-input` ne pointe pas vers un fichier existant,
    #   on tente un téléchargement automatique via l'API DHIS2 en réutilisant la logique
    #   présente dans `download_tracked.py` et `download.py` (intégrée ci-dessus).
    # - Les téléchargements utilisent les variables d'environnement complémentaires
    #   si elles existent (dates, orgUnit, programStage, etc.).
    # ------------------------------------------------------------------

    # Défauts locaux si aucun chemin n'est fourni
    if not args.tracked_input:
        args.tracked_input = os.getenv("TRACKED_OUTPUT", "data/trackedEntityInstances.csv")
    if not args.stage_input:
        args.stage_input = os.getenv("PIVOT_INPUT", "data/data.csv")

    # Tenter de télécharger les fichiers manquants
    try:
        download_dhis2_tracked_if_missing(args.tracked_input, args.base_url, args.token, args.program)
        download_dhis2_events_if_missing(args.stage_input, args.base_url, args.token, args.program)
    except Exception as e:
        print(f"Erreur lors du téléchargement automatique des fichiers d'entrée : {e}")
        # On continue et on laissera pandas lever l'erreur si les fichiers sont vraiment introuvables

    # Pivot du premier onglet
    tracked_df = pivot_tracked_df(args.tracked_input, args.aggfunc)
    print(f"▶️ Pivot du premier onglet : {len(tracked_df)} lignes")

    # Réorganisation des colonnes
    cols = list(tracked_df.columns)
    serial_cols = [c for c in cols if "_serial_number" in str(c).lower()]
    date_cols = [c for c in cols if "_date_" in str(c).lower()]
    parent_cols = [c for c in cols if "_parent_consent" in str(c).lower()]

    if args.strict:
        ordered_cols = ["trackedEntityInstance"] + serial_cols + ["ID"] + date_cols + parent_cols
    else:
        ordered_cols = ["trackedEntityInstance"] + serial_cols + ["ID"]
        remaining = [c for c in cols if c not in ordered_cols]
        remaining_sorted = sorted(remaining)
        ordered_cols += remaining_sorted

    tracked_df = tracked_df[ordered_cols]

    # -----------------------------
    # Création / ouverture du writer pour le premier onglet
    # -----------------------------
    if os.path.exists(args.output):
        wb = load_workbook(args.output)
        if "TrackedEntities" not in wb.sheetnames:
            writer = pd.ExcelWriter(args.output, engine="openpyxl", mode="a", if_sheet_exists="overlay")
            write_with_progress(tracked_df, writer, "TrackedEntities")
            writer.close()
        else:
            print(f"⏩ Fichier existant détecté et premier onglet déjà présent, reprise des onglets restants")
    else:
        writer = pd.ExcelWriter(args.output, engine="openpyxl")
        write_with_progress(tracked_df, writer, "TrackedEntities")
        writer.close()

    # Réouvrir en mode append pour les Program Stages
    writer = pd.ExcelWriter(args.output, engine="openpyxl", mode="a", if_sheet_exists="overlay")

    # Chargement des événements
    df = pd.read_csv(args.stage_input, dtype=str)

    # Mapping dataElements
    mapping = load_mapping(args.mapping_file)
    missing = [u for u in df["dataElement"].unique() if u not in mapping]
    if missing:
        api_map = build_de_mapping_from_api(args.base_url, args.token)
        for uid in missing:
            mapping[uid] = api_map.get(uid, uid)
        save_mapping(args.mapping_file, mapping)
    df["dataElement"] = df["dataElement"].map(mapping)

    # Pivot global des événements
    pivot = df.pivot_table(index="enrollment", columns="dataElement", values="value", aggfunc=args.aggfunc).reset_index().fillna("")
    pivot.insert(pivot.columns.get_loc("enrollment")+1, "ID", range(1, len(pivot)+1))

    stages = get_program_stages(args.base_url, args.token, args.program)

    try:
        for stage in stages:
            stage_name = stage["displayName"][:31]

            if stage_name in state["completed_stages"]:
                print(f"⏩ Déjà traité : {stage_name}")
                continue

            print(f"▶️ Traitement : {stage_name}")
            elements = get_stage_dataelements(args.base_url, args.token, stage["id"])
            cols = ["enrollment", "ID"] + [c for c in elements if c in pivot.columns]
            sheet_df = pivot[cols]

            if sheet_df.columns.tolist() == ["enrollment", "ID"]:
                print("⚠️ Stage vide ignoré")
                state["completed_stages"].append(stage_name)
                save_state(args.state_file, state)
                continue

            sheet_df.to_excel(writer, sheet_name=stage_name, index=False)
            ws = writer.sheets[stage_name]
            auto_adjust_column_width(ws)

            state["completed_stages"].append(stage_name)
            save_state(args.state_file, state)

            print(f"✅ Terminé : {stage_name}")

    finally:
        writer.close()

    clear_state(args.state_file)
    print("\n🎉 Fichier Excel généré avec succès")

if __name__ == "__main__":
    main()


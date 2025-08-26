#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 1 — Download & prepare raw play-by-play for the 1997 Finals (Bulls–Jazz).

What this script does:
1) Pulls *playoff* play-by-play for 1996–97 (and optionally 1997–98) directly from a public GitHub dataset.
2) Writes full-season playoff PBP CSV(s) to data/raw/.
3) Detects Bulls–Jazz playoff games (i.e., 1997 NBA Finals) and writes a finals-only CSV.
4) Prints a short sanity report including whether Steve Kerr's made shot appears in Game 6 (1997-06-13).

Data source (no keys needed): https://github.com/shufinskiy/nba_data  (play-by-play since 1996/97). 
Manual ground-truth page for Game 6 PBP (optional check): 
https://www.basketball-reference.com/boxscores/pbp/199706130CHI.html
"""

from pathlib import Path
from itertools import product
from urllib.request import urlopen
from typing import Union, Sequence, Optional, List
from io import BytesIO, TextIOWrapper
import tarfile
import csv
import sys
import re

import pandas as pd

# -----------------------------
# Config
# -----------------------------
# Season *start* years. 1996 -> 1996–97; 1997 -> 1997–98
SEASONS = [1996]            # add 1997 if you want the 1998 Finals too
INCLUDE_9798 = False        # flip to True to also download 1997–98 playoffs
DATA_TYPES = ("nbastats",)  # we only need NBA Stats pbp here
SEASON_TYPE = "po"          # 'rg' regular season, 'po' playoffs
LEAGUE = "nba"


# fetch paths
import sys, os
sys.path.append(os.path.abspath('..'))
import config

#OUT_DIR = Path("data/raw").resolve()
OUT_DIR = Path(config.DATA_RAW_DIR)
#OUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Minimal loader for shufinskiy/nba_data (Python version)
# (adapted from the project's README)
# -----------------------------
def load_nba_data(
    path: Union[Path, str] = Path.cwd(),
    seasons: Union[Sequence, int] = (1996,),
    data: Union[Sequence, str] = ("nbastats",),
    seasontype: str = "po",
    league: str = "nba",
    untar: bool = False,
    in_memory: bool = True,
    use_pandas: bool = True
) -> Optional[Union[List, pd.DataFrame]]:
    """
    Load NBA play-by-play archives from the GitHub dataset.

    If in_memory=True & use_pandas=True, returns a concatenated DataFrame.
    Otherwise, saves .tar.xz archives (and optionally extracts CSVs) to 'path'.
    """
    if isinstance(path, str):
        path = Path(path).expanduser()
    if isinstance(seasons, int):
        seasons = (seasons,)
    if isinstance(data, str):
        data = (data,)

    # Build the "keys" that map to downloadable artifacts
    if seasontype == "rg":
        need_data = [f"{d}_{s}" for d, s in product(data, seasons)]
    elif seasontype == "po":
        need_data = [f"{d}_{seasontype}_{s}" for d, s in product(data, seasons)]
    else:
        # both regular & playoffs
        need_data = [f"{d}_{s}" for d, s in product(data, seasons)]
        need_data += [f"{d}_{seasontype}_{s}" for d, s in product(data, seasons)]

    # Map names -> URLs
    with urlopen("https://raw.githubusercontent.com/shufinskiy/nba_data/main/list_data.txt") as resp:
        v = resp.read().decode("utf-8")
    name_v = [line.split("=")[0] for line in v.split("\n") if "=" in line]
    element_v = [line.split("=")[1] for line in v.split("\n") if "=" in line]

    need_name = [name for name in name_v if name in need_data]
    need_element = [element for (name, element) in zip(name_v, element_v) if name in need_data]

    if in_memory and use_pandas:
        table = pd.DataFrame()
    elif in_memory:
        table = []
    else:
        table = None

    path.mkdir(parents=True, exist_ok=True)

    for name, url in zip(need_name, need_element):
        with urlopen(url) as response:
            if response.status != 200:
                raise RuntimeError(f"Failed to download: {url} (HTTP {response.status})")
            content = response.read()

        if in_memory:
            with tarfile.open(fileobj=BytesIO(content), mode="r:xz") as tar:
                csv_name = f"{name}.csv"
                member = tar.getmember(csv_name)
                f = tar.extractfile(member)
                if use_pandas:
                    df_part = pd.read_csv(f)
                    # Attach season key for traceability
                    df_part["__archive_name"] = name
                    table = pd.concat([table, df_part], axis=0, ignore_index=True)
                else:
                    reader = csv.reader(TextIOWrapper(f, encoding="utf-8"))
                    for row in reader:
                        table.append(row)
        else:
            # Save archive to disk
            archive_path = path / f"{name}.tar.xz"
            archive_path.write_bytes(content)
            if untar:
                with tarfile.open(archive_path) as tar:
                    tar.extract(f"{name}.csv", path)
                archive_path.unlink()

    return table

# -----------------------------
# Helpers to extract teams & finals games
# -----------------------------
TEAM_COL_CANDIDATES = [
    # typical NBA stats PBP columns
    "PLAYER1_TEAM_ABBREVIATION",
    "PLAYER2_TEAM_ABBREVIATION",
    "PLAYER3_TEAM_ABBREVIATION",
    "TEAM_ABBREVIATION",
    "PLAYER1_TEAM_CITY",
    "PLAYER2_TEAM_CITY",
    "PLAYER3_TEAM_CITY",
]

DESC_COLS = ["HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION"]

def collect_team_abbrevs(df: pd.DataFrame) -> pd.Series:
    """Return a Series mapping GAME_ID -> set of team abbrevs seen in that game."""
    # Gather team abbrevs from explicit columns if present
    team_sets = {}
    has_cols = [c for c in TEAM_COL_CANDIDATES if c in df.columns]
    for gid, g in df.groupby("GAME_ID"):
        teams = set()
        for c in has_cols:
            vals = g[c].dropna().astype(str).str.upper().str.strip()
            # Keep obvious NBA tricodes (guard against city names)
            teams.update([v for v in vals if re.fullmatch(r"[A-Z]{2,4}", v)])
        # Fall back: mine descriptions for "CHI" / "UTA"
        if not teams:
            for dcol in [c for c in DESC_COLS if c in df.columns]:
                txt = " ".join(g[dcol].dropna().astype(str).tolist()).upper()
                if " CHI " in f" {txt} " or " CHI." in txt or " CHI," in txt:
                    teams.add("CHI")
                if " UTA " in f" {txt} " or " UTAH" in txt or " UTA," in txt:
                    teams.add("UTA")
        team_sets[gid] = teams
    return pd.Series(team_sets, name="teams")

def finals_game_ids_1997(df: pd.DataFrame) -> List[str]:
    """Identify Bulls–Jazz playoff games in 1996–97 (i.e., the 1997 Finals)."""
    team_sets = collect_team_abbrevs(df)
    gids = [gid for gid, teams in team_sets.items() if {"CHI", "UTA"}.issubset(teams)]
    return sorted(gids)

def find_kerr_make_in_game(df_game: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Look for a Steve Kerr made shot row in a single-game PBP chunk."""
    name_cols = [c for c in df_game.columns if "PLAYER" in c and "NAME" in c]
    desc_cols = [c for c in DESC_COLS if c in df_game.columns]
    candidates = []

    # name-based hit
    for nc in name_cols:
        hits = df_game[df_game[nc].astype(str).str.contains(r"\bSTEVE\s+KERR\b", case=False, na=False)]
        if not hits.empty:
            candidates.append(hits)

    # description-based hit
    for dc in desc_cols:
        hits = df_game[df_game[dc].astype(str).str.contains(r"\bKERR\b.*\bMADE|\bMAKES\b|\bJUMPER\b|\b2-PT\b|\b2PT\b", case=False, na=False)]
        if not hits.empty:
            candidates.append(hits)

    if candidates:
        out = pd.concat(candidates, axis=0).drop_duplicates()
        # Prefer "made" events if EVENTMSGTYPE present (1 == made shot typically)
        if "EVENTMSGTYPE" in out.columns:
            out = out.sort_values(["EVENTMSGTYPE", "EVENTNUM"])  # EVENTNUM increasing over time
        return out
    return None

# -----------------------------
# Main
# -----------------------------
def main():
    seasons = SEASONS.copy()
    if INCLUDE_9798:
        seasons.append(1997)

    print(f"Downloading playoff PBP for seasons: {seasons} …")
    df = load_nba_data(
        path=OUT_DIR, 
        seasons=seasons, 
        data=DATA_TYPES, 
        seasontype=SEASON_TYPE, 
        league=LEAGUE,
        in_memory=True,
        use_pandas=True
    )
    # Basic normalization
    if "GAME_ID" not in df.columns:
        raise RuntimeError("Expected column GAME_ID not found in downloaded data.")
    # Keep only the columns we'll need later + keep everything raw for now
    df.sort_values(["GAME_ID", "PERIOD", "EVENTNUM"], inplace=True, ignore_index=True)

    # Save full playoff PBP for each requested season
    for s in seasons:
        mask = df["__archive_name"].str.contains(f"_{SEASON_TYPE}_{s}$")
        df_s = df.loc[mask].copy()
        out_csv = OUT_DIR / f"pbp_{s}_{s+1}_playoffs.csv"
        df_s.to_csv(out_csv, index=False)
        print(f"✔ Saved season playoffs: {out_csv}  (rows={len(df_s):,})")

    # 1996–97 Finals (Bulls–Jazz) identification
    df_9697 = df[df["__archive_name"].str.contains(f"_{SEASON_TYPE}_1996$")].copy()
    finals_gids = finals_game_ids_1997(df_9697)
    if not finals_gids:
        raise RuntimeError("Couldn’t find Bulls–Jazz playoff games in 1996–97. Check columns/filters.")

    df_finals = df_9697[df_9697["GAME_ID"].isin(finals_gids)].copy()
    out_finals = OUT_DIR / "pbp_1997_finals_chi_uta.csv"
    df_finals.to_csv(out_finals, index=False)
    print(f"✔ Saved 1997 Finals (CHI–UTA): {out_finals}  (games={len(set(finals_gids))}, rows={len(df_finals):,})")
    print(f"  Detected GAME_IDs: {sorted(set(finals_gids))}")

    # Try to spot Steve Kerr's made shot in Game 6
    # He hit the dagger with ~0:25 left in Q4 (Game 6, June 13, 1997).
    # We'll search within the last 2 minutes of Q4 in the CHI home game among the detected Finals games.
    game6_candidates = []
    for gid in sorted(set(finals_gids)):
        g = df_finals[df_finals["GAME_ID"] == gid].copy()
        # Coerce a simple clock (mm:ss) if present, else skip clock filter
        if "PCTIMESTRING" in g.columns:
            # Keep last 2 minutes of regulation
            g = g[(g["PERIOD"] == 4) & g["PCTIMESTRING"].astype(str).str.match(r"^\d+:\d{2}$")]
            g["__sec_left"] = g["PCTIMESTRING"].str.split(":").apply(lambda x: int(x[0]) * 60 + int(x[1]))
            g = g[g["__sec_left"] <= 120]
        hit = find_kerr_make_in_game(g if not g.empty else df_finals[df_finals["GAME_ID"] == gid])
        if hit is not None and not hit.empty:
            hit["__GAME_ID"] = gid
            game6_candidates.append(hit)

    if game6_candidates:
        kerr_hits = pd.concat(game6_candidates).drop_duplicates()
        # Prefer the most "late-clock" event if we computed __sec_left
        if "__sec_left" in kerr_hits.columns:
            kerr_hits = kerr_hits.sort_values(["__GAME_ID", "__sec_left", "EVENTNUM"], ascending=[True, True, True])
        else:
            kerr_hits = kerr_hits.sort_values(["__GAME_ID", "EVENTNUM"])
        # Print a compact preview
        preview_cols = [c for c in ["__GAME_ID", "PERIOD", "PCTIMESTRING", "SCORE", "SCOREMARGIN",
                                    "HOMEDESCRIPTION", "VISITORDESCRIPTION",
                                    "PLAYER1_NAME", "EVENTMSGTYPE"] if c in kerr_hits.columns]
        print("\n🔎 Steve Kerr 'made shot' candidates near the end of Game 6:")
        print(kerr_hits[preview_cols].tail(6).to_string(index=False))
    else:
        print("\n⚠ Couldn’t automatically surface Kerr’s shot. It may still be present; verify later in Step 2.")

    print("\nDone.")
    return 0

if __name__ == "__main__":
    sys.exit(main())

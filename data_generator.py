#!/usr/bin/env python3
"""
Stdlib only. You can run this file without installing extra Python libraries.

data_generator.py
AI generated synthetic generator -- simulated lab instruments that emit deliberately messy data.
Models schema drift, unit differences, corrupt files, encodings, aliases, missingness, duplicates, and an underlying analytical signal.

The generator normally makes random choices.
    Use the --seed option to control that randomness. (See command documentation below.)

Two instruments cover the same chemistry:
    LabTrak 3000   (old, 2016)  -> CSV,  flat, one row per sample, drifting schema
    RX-2100        (new, 2023)  -> JSON, nested, one file per batch, other units

The generator includes an underlying chemistry pattern on purpose. 
For each catalyst, yield is best around a particular temperature, 
while purity tends to fall when a reaction is pushed too hot.
The raw files hide this pattern behind inconsistent formats, missing values, aliases, and faulty measurements.

Chaos setting:
The `chaos` setting controls how messy the generated instrument data is:
    0 = clean, consistent data for testing the happy path
    1 = realistic lab data with missing values, inconsistent formats, duplicate exports, sensor faults, and occasional corrupt files
    2+ = extra-messy data for stress-testing the pipeline
    
    A higher chaos value increases the chance that these issues appear.


The generator has two ways to create data:

    (- backfill) creates a historical set of lab files all at once. Use it when
        you want enough past data to demonstrate the whole pipeline immediately.

    (-stream) keeps creating new files over time, like a live lab instrument.
        Use it when you want to watch new data arrive and move through the pipeline.


usage: data_generator.py [-h] {backfill,stream} ...

Generate deliberately messy fake lab instrument data.
if no destination specified, default writes to: data/incoming

commands:
  backfill
    Write a historical campaign of files all at once.
  stream
    Keep creating new files over time, like a live instrument.

common options:
  --out PATH
    Output folder for generated CSV and JSON files.
    Default: data/incoming

  --seed NUMBER
    Random seed. Use the same seed to reproduce the same pattern of data.
    Default: 7

  --chaos NUMBER
    How messy the generated data should be.
    0 = clean data; 1 = realistic messy data; 2+ = very messy data.
    Default: 1.0

  --truth PATH
    Write a separate answer-key CSV containing the intended clean values.
    This file is not used by Airflow or dbt.
    Default: no answer key

backfill options:
  --days NUMBER
    Number of historical days to generate.
    Default: 45

  --min-per-day NUMBER
    Minimum batches generated per day.
    Default: 2

  --max-per-day NUMBER
    Maximum batches generated per day.
    Default: 7

  --end ISO_DATE
    End date for the historical campaign.
    Default: now

  --quiet
    Do not print every generated file.

stream options:
  --interval SECONDS
    Approximate time between generated batches.
    Default: 8.0

  --jitter NUMBER
    Random variation in timing.
    0 = exactly on schedule; 0.4 = up to roughly 40% earlier or later.
    Default: 0.4

  --count NUMBER
    Stop after this many batches.
    0 = run forever.
    Default: 0


Quick start:


python data_generator.py --help

python data_generator.py backfill --days 45 
python data_generator.py backfill --days 45 --min-per-day 2 --max-per-day 5
python data_generator.py backfill --days 45 --chaos 0
python data_generator.py backfill --days 45 --chaos 2
python data_generator.py backfill --days 45 --seed 7
python data_generator.py backfill --days 45 --truth data/truth.csv
python data_generator.py backfill --days 45 --out data/test_incoming

python data_generator.py stream
python data_generator.py stream --interval 8
python data_generator.py stream --interval 3 --jitter 0
python data_generator.py stream --count 10
python data_generator.py stream --out data/test_incoming --count 10



"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

# This hidden file is the generator's memory. It stores the next batch number
# and campaign settings so a later run can continue where the previous one
# stopped. Airflow and dbt do not use this file.
STATE_FILE = ".labsim_state.json"

# ---------------------------------------------------------------------------
# The underlying chemistry (the thing an analyst is supposed to rediscover)
# ---------------------------------------------------------------------------

CATALYSTS = {
    #                 t_opt  ymax  width  pur_base  pur_slope
    "Pd/C":      dict(t_opt=85.0,  ymax=78.0, width=18.0, pur_base=97.5, pur_slope=0.090),
    "PtO2":      dict(t_opt=70.0,  ymax=83.0, width=12.0, pur_base=98.2, pur_slope=0.140),
    "Ni-Raney":  dict(t_opt=110.0, ymax=64.0, width=25.0, pur_base=94.0, pur_slope=0.060),
    "Ru/Al2O3":  dict(t_opt=130.0, ymax=59.0, width=30.0, pur_base=92.5, pur_slope=0.040),
    "none":      dict(t_opt=140.0, ymax=12.0, width=40.0, pur_base=88.0, pur_slope=0.020),
}

# How each catalyst gets written down in the wild. First entry is canonical.
CATALYST_ALIASES = {
    "Pd/C":     ["Pd/C", "Pd-C", "pd/c", "10% Pd/C", "Pd/C ", "Palladium on carbon"],
    "PtO2":     ["PtO2", "PtO₂", "Adams catalyst", "pto2", "PtO2 "],
    "Ni-Raney": ["Ni-Raney", "Raney Ni", "raney nickel", "Ni(R)", "Ra-Ni"],
    "Ru/Al2O3": ["Ru/Al2O3", "Ru/alumina", "ru-al2o3", "Ru / Al2O3"],
    "none":     ["none", "NONE", "blank", "no cat", "uncatalysed"],
}

SOLVENTS = ["toluene", "THF", "MeOH", "EtOAc", "DCM", "toluene "]

OPERATORS = ["jsm", "M. Müller", "a.osei", "R. Silva", "K. Tanaka", "s ahmed", "JSM", "novak_j"]

NOTES = [
    "", "", "", "",
    "rerun of prev batch",
    "stirrer bar stuck ~10 min",
    "reagent from new bottle",
    "line pressure low, see logbook",
    "SEE NOTEBOOK P.114",
    "colour off, greenish",
    "power blip at 03:20",
]

MISSING_MARKERS = ["", "", "N/A", "n/a", "NULL", "null", "-", "--", "?", "#N/A", "NaN"]
TEMP_SENTINELS = [-999.0, -999.9, 9999.0, 0.0]
QC_FLAGS = ["DETECTOR_SATURATED", "THERMOCOUPLE_FAULT", "NO_PEAK",
            "BASELINE_DRIFT", "OVERRANGE", "MANUAL_ENTRY"]


def true_yield(temp_c: float, cat: str, loading: float, lot: float, age: float) -> float:
    """Return the clean, intended yield before instrument problems are added."""
    p = CATALYSTS[cat]
    peak = p["ymax"] * math.exp(-((temp_c - p["t_opt"]) ** 2) / (2 * p["width"] ** 2))
    # Loading saturates. Blanks carry no catalyst but still creep along thermally.
    load_term = 1.0 if cat == "none" else 1.0 - math.exp(-loading / 1.2)
    return max(0.0, peak * load_term * lot * age)


def true_purity(temp_c: float, cat: str, y: float) -> float:
    """Return the clean, intended purity before instrument problems are added."""
    p = CATALYSTS[cat]
    overcook = max(0.0, temp_c - (p["t_opt"] - 5.0))  # side products above the optimum
    return min(99.9, max(55.0, p["pur_base"] - p["pur_slope"] * overcook - 0.02 * y))


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Sample:
    """One result measured from a batch, including any simulated quality issues."""
    idx: int
    yield_pct: float | None
    purity_pct: float | None
    faults: list[str] = field(default_factory=list)


@dataclass
class Batch:
    """The clean internal version of one experiment before it is written as a file."""
    n: int                      # global batch counter
    started: datetime
    instrument: str             # "labtrak" | "rx2100"
    temp_c: float               # what actually happened
    temp_reported: float | None # what the thermocouple claims (None = dropout)
    catalyst: str               # canonical name
    catalyst_written: str       # how it got typed in
    loading: float
    solvent: str
    operator: str
    lot: str
    note: str
    samples: list[Sample]
    progress: float             # 0..1 through the campaign, drives schema drift
    faults: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

class Lab:
    """Keep the slow changes in the simulated lab between batches.

    This includes changing catalyst lots, the move from the older LabTrak
    instrument to the newer RX-2100, and the last temperature reading that a
    faulty sensor can accidentally repeat.
    """

    def __init__(self, rng: random.Random, chaos: float, start: datetime, span_days: float):
        self.rng = rng
        self.chaos = chaos
        self.start = start
        self.span_days = max(span_days, 1.0)
        self._lots: dict[str, tuple[str, float, datetime]] = {}
        self._last_temp = 85.0

    def lot_for(self, cat: str, when: datetime) -> tuple[str, float]:
        """Catalyst lots turn over every ~12 days. Occasionally a lot is just bad."""
        cur = self._lots.get(cat)
        if cur is None or (when - cur[2]).days > self.rng.randint(9, 16):
            code = f"{cat.split('/')[0][:2].upper()}-{self.rng.randint(2200, 2299)}"
            factor = self.rng.gauss(1.0, 0.06)
            if self.rng.random() < 0.10:
                factor *= self.rng.uniform(0.45, 0.7)   # dud lot, whole week looks wrong
            self._lots[cat] = (code, max(0.2, factor), when)
        code, factor, _ = self._lots[cat]
        return code, factor

    def instrument_for(self, progress: float) -> str:
        """The new box gets phased in over the campaign; the old one lingers."""
        p_new = 1.0 / (1.0 + math.exp(-8.0 * (progress - 0.45)))
        p_new = 0.08 + 0.82 * p_new
        return "rx2100" if self.rng.random() < p_new else "labtrak"

    def batch(self, n: int, when: datetime) -> Batch:
        rng, c = self.rng, self.chaos
        progress = min(1.0, max(0.0, (when - self.start).total_seconds()
                                / (self.span_days * 86400)))

        cat = rng.choices(list(CATALYSTS), weights=[8, 5, 5, 3, 1])[0]
        loading = 0.0 if cat == "none" else round(rng.choice([0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 5.0])
                                                  + rng.gauss(0, 0.08), 2)

        # Operators sweep a coarse grid, with the occasional wild shot.
        if rng.random() < 0.85:
            temp = rng.choice([50, 60, 70, 80, 90, 100, 110, 120, 130, 140]) + rng.gauss(0, 2.5)
        else:
            temp = rng.uniform(40, 165)
        temp = round(temp, 1)

        lot_code, lot_factor = self.lot_for(cat, when)
        age = 1.0 - 0.12 * progress + rng.gauss(0, 0.02)   # slow decline, recalibration overdue

        y_true = true_yield(temp, cat, loading, lot_factor, age)
        p_true = true_purity(temp, cat, y_true)

        faults: list[str] = []
        temp_reported: float | None = temp + rng.gauss(0, 0.4)

        if rng.random() < 0.05 * c:                        # thermocouple dropout
            temp_reported = None
            faults.append("temp_dropout")
        elif rng.random() < 0.03 * c:                      # reading stuck at previous batch
            temp_reported = self._last_temp
            faults.append("temp_stuck")
        elif rng.random() < 0.02 * c:                      # sensor writes its error code
            temp_reported = rng.choice(TEMP_SENTINELS)
            faults.append("temp_sentinel")
        else:
            self._last_temp = temp_reported

        samples = []
        for i in range(rng.randint(2, 6)):
            sy: float | None = max(0.0, y_true + rng.gauss(0, 1.8))
            sp: float | None = min(99.9, p_true + rng.gauss(0, 0.6))
            sf: list[str] = []
            r = rng.random()
            if r < 0.05 * c:
                sy = None
                sf.append("NO_PEAK")
            elif r < 0.08 * c:
                sy = -abs(rng.gauss(4, 3))                 # integrator went negative
                sf.append("BASELINE_DRIFT")
            elif r < 0.10 * c:
                sy = rng.uniform(101, 138)                 # impossible yield
                sf.append("OVERRANGE")
            if rng.random() < 0.05 * c:
                sp = None
                sf.append("DETECTOR_SATURATED")
            elif rng.random() < 0.02 * c:
                sp = 100.0                                 # suspiciously perfect
                sf.append("DETECTOR_SATURATED")
            samples.append(Sample(i + 1, sy, sp, sf))

        return Batch(
            n=n,
            started=when,
            instrument=self.instrument_for(progress),
            temp_c=temp,
            temp_reported=temp_reported,
            catalyst=cat,
            catalyst_written=rng.choice(CATALYST_ALIASES[cat]) if rng.random() < 0.6 * c else cat,
            loading=loading,
            solvent=rng.choice(SOLVENTS),
            operator=rng.choice(OPERATORS),
            lot=lot_code,
            note=rng.choice(NOTES),
            samples=samples,
            progress=progress,
            faults=faults,
        )


# ---------------------------------------------------------------------------
# Rendering: the old CSV instrument
# ---------------------------------------------------------------------------

def _num(x, digits=1, comma=False):
    """Format a number the way an instrument export might write it."""
    if x is None:
        return None
    s = f"{x:.{digits}f}"
    return s.replace(".", ",") if comma else s


def _maybe_missing(rng, s, p):
    """Sometimes replace a value with a messy marker such as N/A or --."""
    if s is None:
        return rng.choice(MISSING_MARKERS)
    if rng.random() < p:
        return rng.choice(MISSING_MARKERS)
    return s


def _date_str(rng, dt: datetime, chaos: float) -> str:
    """Write the same date in several possible formats to simulate schema drift."""
    fmts = ["%d/%m/%Y", "%Y-%m-%d", "%m-%d-%y", "%d-%b-%Y", "%Y-%m-%d %H:%M:%S"]
    if rng.random() < 0.7 * chaos:
        return dt.strftime(rng.choice(fmts))
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def render_csv(batch: Batch, rng: random.Random, chaos: float) -> list[tuple[str, bytes]]:
    """Turn one batch into one or more deliberately inconsistent LabTrak CSV files."""
    c = chaos
    euro = rng.random() < 0.06 * c          # semicolon + decimal comma export
    sep = ";" if euro else ","
    p_missing = 0.06 * c

    # Schema drift: columns get renamed and added as firmware is updated.
    if batch.progress < 0.33:
        cols = ["RUN_ID", "DATE", "TEMP", "CAT", "LOAD", "YIELD", "PURITY", "OP"]
    elif batch.progress < 0.66:
        cols = ["RUN_ID", "DATE", "TEMP", "CAT", "LOAD", "SOLVENT", "YIELD", "PURITY", "OP"]
    else:
        cols = ["RUN_ID", "SAMPLE", "DATE", "TEMP_C", "CATALYST", "LOAD_MOLPCT",
                "SOLVENT", "YIELD_PCT", "PURITY_PCT", "OPERATOR", "NOTES"]

    lines: list[str] = []
    if rng.random() < 0.5:
        lines.append(f"# LabTrak 3000 v2.1{rng.randint(0,9)} export "
                     f"{batch.started:%Y-%m-%d %H:%M} lot={batch.lot}")
    lines.append(sep.join(cols))

    temp_s = _num(batch.temp_reported, 1, euro)
    for s in batch.samples:
        row = {
            "RUN_ID": f"R{batch.n:04d}",
            "SAMPLE": str(s.idx),
            "DATE": _date_str(rng, batch.started, c),
            "TEMP": _maybe_missing(rng, temp_s, p_missing),
            "TEMP_C": _maybe_missing(rng, temp_s, p_missing),
            "CAT": batch.catalyst_written,
            "CATALYST": batch.catalyst_written,
            "LOAD": _maybe_missing(rng, _num(batch.loading, 2, euro), p_missing),
            "LOAD_MOLPCT": _maybe_missing(rng, _num(batch.loading, 2, euro), p_missing),
            "SOLVENT": _maybe_missing(rng, batch.solvent, p_missing * 1.5),
            "YIELD": _maybe_missing(rng, _num(s.yield_pct, 1, euro), p_missing),
            "YIELD_PCT": _maybe_missing(rng, _num(s.yield_pct, 1, euro), p_missing),
            "PURITY": _maybe_missing(rng, _num(s.purity_pct, 1, euro), p_missing),
            "PURITY_PCT": _maybe_missing(rng, _num(s.purity_pct, 1, euro), p_missing),
            "OP": batch.operator,
            "OPERATOR": batch.operator,
            "NOTES": batch.note,
        }
        # Old instrument sometimes appends the unit into the cell.
        for k in ("YIELD", "YIELD_PCT", "PURITY", "PURITY_PCT"):
            if rng.random() < 0.05 * c and row[k] not in MISSING_MARKERS:
                row[k] = row[k] + "%"
        vals = []
        for col in cols:
            v = row.get(col, "")
            if rng.random() < 0.04 * c:
                v = " " + v if rng.random() < 0.5 else v + " "   # stray whitespace
            if sep in v or '"' in v:
                v = '"' + v.replace('"', '""') + '"'
            vals.append(v)
        lines.append(sep.join(vals))

    text = "\n".join(lines) + "\n"

    if rng.random() < 0.03 * c:                  # instrument died mid-write
        text = text[: int(len(text) * rng.uniform(0.4, 0.85))]
    if rng.random() < 0.02 * c:                  # header only, run aborted
        text = "\n".join(lines[: 1 + int(lines[0].startswith("#"))]) + "\n"

    enc = "utf-8"
    if rng.random() < 0.05 * c:
        enc = "cp1252"
    data = text.encode(enc, errors="replace")
    if enc == "utf-8" and rng.random() < 0.06 * c:
        data = b"\xef\xbb\xbf" + data             # BOM

    d = batch.started
    names = [
        f"LT3000_{d:%Y%m%d}_R{batch.n:04d}.csv",
        f"labtrak_export_{d:%d-%m-%Y}_{batch.n}.csv",
        f"LT3000 {d:%Y%m%d} run{batch.n}.CSV",
    ]
    name = names[0] if rng.random() < 0.7 else rng.choice(names)
    out = [(name, data)]
    if rng.random() < 0.03 * c:                   # same batch exported twice
        stem, _, ext = name.rpartition(".")
        out.append((f"{stem} (1).{ext}", data))
    return out


# ---------------------------------------------------------------------------
# Rendering: the new JSON instrument
# ---------------------------------------------------------------------------

def render_json(batch: Batch, rng: random.Random, chaos: float) -> list[tuple[str, bytes]]:
    """Turn one batch into one or more deliberately inconsistent RX-2100 JSON files."""
    c = chaos
    p_missing = 0.06 * c
    v2 = batch.progress > 0.4 or rng.random() < 0.3   # firmware 2.x rolls out mid-campaign

    def drop(d: dict, key: str, p: float):
        """Sometimes null, sometimes the key just isn't there at all."""
        if rng.random() < p:
            d[key] = None if rng.random() < 0.6 else d.get(key)
            if d[key] is None and rng.random() < 0.4:
                d.pop(key, None)

    if not v2:
        # ---- firmware 1.x: flat-ish, parallel arrays, Celsius, percent ----
        doc = {
            "schema": "1.4",
            "batch_id": f"RX{batch.n:06d}",
            "timestamp": batch.started.strftime("%Y-%m-%dT%H:%M:%S"),
            "temp_c": batch.temp_reported,
            "catalyst": batch.catalyst_written,
            "loading_molpct": batch.loading,
            "solvent": batch.solvent,
            "operator": batch.operator,
            "results": {
                "yield_pct": [None if s.yield_pct is None else round(s.yield_pct, 2)
                              for s in batch.samples],
                "purity_pct": [None if s.purity_pct is None else round(s.purity_pct, 2)
                               for s in batch.samples],
            },
            "flags": sorted({f for s in batch.samples for f in s.faults}),
        }
        if batch.temp_reported is None and rng.random() < 0.5:
            doc["temp_c"] = rng.choice(TEMP_SENTINELS)
        drop(doc, "solvent", p_missing * 2)
        drop(doc, "operator", p_missing * 2)
    else:
        # ---- firmware 2.x: nested, Kelvin, yield as a fraction ----
        temp_k = None if batch.temp_reported is None else round(batch.temp_reported + 273.15, 2)
        doc = {
            "schemaVersion": "2.1",
            "batchId": f"RX-2100-{batch.n:06d}",
            "startedAt": batch.started.astimezone(timezone.utc)
                              .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "instrument": {
                "model": "RX-2100",
                "serial": f"RX21-{7000 + (batch.n % 3):04d}",
                "firmware": rng.choice(["2.1.4", "2.1.4", "2.1.7", "2.2.0-rc1"]),
            },
            "conditions": {
                "temperature": {"value": temp_k, "unit": "K"},
                "catalyst": {
                    "name": batch.catalyst_written,
                    "lot": batch.lot,
                    "loading": {"value": batch.loading, "unit": "mol%"},
                },
                "solvent": batch.solvent,
            },
            "samples": [
                {
                    "id": f"S{s.idx}",
                    "yield": None if s.yield_pct is None else round(s.yield_pct / 100.0, 5),
                    "purity": None if s.purity_pct is None else round(s.purity_pct, 3),
                    "flags": s.faults or [],
                }
                for s in batch.samples
            ],
            "qc": {
                "passed": not any(s.faults for s in batch.samples),
                "reviewer": None if rng.random() < 0.6 else batch.operator,
                "comment": batch.note or None,
            },
        }
        if temp_k is None and rng.random() < 0.4:
            doc["conditions"]["temperature"] = {"value": rng.choice(TEMP_SENTINELS), "unit": "K"}
        drop(doc["conditions"], "solvent", p_missing * 2)
        for s in doc["samples"]:
            if rng.random() < p_missing:
                s.pop("purity", None)
            if rng.random() < 0.02 * c:
                s["flags"] = list(set(s.get("flags", []) + [rng.choice(QC_FLAGS)]))

    text = json.dumps(doc, indent=2, ensure_ascii=False)

    if rng.random() < 0.02 * c:                 # write interrupted -> invalid JSON
        text = text[: int(len(text) * rng.uniform(0.3, 0.8))]

    data = text.encode("utf-8")
    d = batch.started
    names = [
        f"RX2100_{d:%Y%m%dT%H%M%S}_b{batch.n:05d}.json",
        f"rx2100-batch-{batch.n:05d}.json",
        f"RX-2100_{d:%Y-%m-%d}_{batch.n:05d}.json",
    ]
    name = names[0] if rng.random() < 0.7 else rng.choice(names)
    out = [(name, data)]
    if rng.random() < 0.02 * c:                 # re-export after a "fix"
        out.append((name.replace(".json", "_v2.json"), data))
    return out


def render(batch: Batch, rng: random.Random, chaos: float) -> list[tuple[str, bytes]]:
    """Choose the correct file format for the batch, or simulate a batch lost before export."""
    if rng.random() < 0.01 * chaos:
        return []                                # batch simply never made it to disk
    if batch.instrument == "labtrak":
        return render_csv(batch, rng, chaos)
    return render_json(batch, rng, chaos)


# ---------------------------------------------------------------------------
# Answer key
# ---------------------------------------------------------------------------

TRUTH_COLS = ["batch", "started_utc", "instrument", "temp_c", "catalyst", "lot",
              "loading_molpct", "solvent", "n_samples", "mean_yield_pct",
              "mean_purity_pct", "faults", "files"]


def truth_row(b: Batch, files: list[str]) -> str:
    """Create one answer-key row from the clean internal batch values."""
    ys = [s.yield_pct for s in b.samples if s.yield_pct is not None and 0 <= s.yield_pct <= 100]
    ps = [s.purity_pct for s in b.samples if s.purity_pct is not None]
    cells = [
        str(b.n),
        b.started.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        b.instrument,
        f"{b.temp_c:.1f}",
        b.catalyst,
        b.lot,
        f"{b.loading:.2f}",
        b.solvent.strip(),
        str(len(b.samples)),
        f"{sum(ys)/len(ys):.2f}" if ys else "",
        f"{sum(ps)/len(ps):.2f}" if ps else "",
        "|".join(b.faults + sorted({f for s in b.samples for f in s.faults})),
        "|".join(files),
    ]
    return ",".join('"' + c.replace('"', '""') + '"' if "," in c else c for c in cells)


# ---------------------------------------------------------------------------
# Emission
# ---------------------------------------------------------------------------

def emit(batch: Batch, outdir: Path, rng: random.Random, chaos: float,
         truth_fh=None, verbose=True) -> int:
    """Render a batch, write its files to disk, and optionally write an answer-key row."""
    files = render(batch, rng, chaos)
    for name, data in files:
        (outdir / name).write_bytes(data)
    if truth_fh:
        truth_fh.write(truth_row(batch, [n for n, _ in files]) + "\n")
        truth_fh.flush()
    if verbose:
        tag = "CSV " if batch.instrument == "labtrak" else "JSON"
        if not files:
            print(f"  batch {batch.n:5d}  {tag}  <lost>")
        for name, data in files:
            print(f"  batch {batch.n:5d}  {tag}  {name}  ({len(data)} B)")
    return len(files)


def load_state(outdir: Path) -> dict:
    """Read the generator's saved progress, or return an empty state for a new folder."""
    p = outdir / STATE_FILE
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    return {}


def save_state(outdir: Path, state: dict) -> None:
    """Save progress after each run so future batches continue their numbering."""
    (outdir / STATE_FILE).write_text(json.dumps(state, indent=2))


def open_truth(path: str | None):
    """Open the optional answer-key file and add its header when it is new."""
    if not path:
        return None
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    new = not p.exists() or p.stat().st_size == 0
    fh = p.open("a", encoding="utf-8")
    if new:
        fh.write(",".join(TRUTH_COLS) + "\n")
    return fh


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_backfill(args) -> None:
    """Generate a historical campaign of batches all at once."""
    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(args.seed)

    end = datetime.now(timezone.utc) if args.end is None else \
        datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    start = end - timedelta(days=args.days)

    state = load_state(outdir)
    n = state.get("next_batch", 1)

    lab = Lab(rng, args.chaos, start, args.days)
    truth = open_truth(args.truth)
    files = batches = 0

    day = start
    while day < end:
        if day.weekday() >= 5 and rng.random() < 0.8:      # quiet weekends
            day += timedelta(days=1)
            continue
        for _ in range(rng.randint(args.min_per_day, args.max_per_day)):
            when = day.replace(hour=0, minute=0, second=0) + timedelta(
                hours=rng.uniform(7.5, 19.0), seconds=rng.uniform(0, 3600))
            if when >= end:
                continue
            files += emit(lab.batch(n, when), outdir, rng, args.chaos, truth, args.verbose)
            batches += 1
            n += 1
        day += timedelta(days=1)

    state["next_batch"] = n
    state["campaign_start"] = start.isoformat()
    state["span_days"] = args.days
    state["seed"] = args.seed
    save_state(outdir, state)
    if truth:
        truth.close()

    print(f"\n{batches} batches -> {files} files in {outdir}/  "
          f"(chaos={args.chaos}, seed={args.seed})")
    if args.truth:
        print(f"answer key: {args.truth}")


def cmd_stream(args) -> None:
    """Generate new batches over time, like files arriving from a live instrument."""
    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)
    state = load_state(outdir)
    seed = args.seed if args.seed is not None else state.get("seed", 0)
    rng = random.Random(seed + int(time.time()) % 9973 if args.seed is None else seed)

    n = state.get("next_batch", 1)
    # With no prior backfill, drop in mid-rollout so both instruments show up.
    start = datetime.fromisoformat(state["campaign_start"]) if "campaign_start" in state \
        else datetime.now(timezone.utc) - timedelta(days=20)
    span = state.get("span_days", 45)

    lab = Lab(rng, args.chaos, start, span)
    truth = open_truth(args.truth)

    print(f"streaming into {outdir}/ every ~{args.interval}s from batch {n}. ctrl-c to stop.")
    written = 0
    try:
        while args.count == 0 or written < args.count:
            emit(lab.batch(n, datetime.now(timezone.utc)), outdir, rng,
                 args.chaos, truth, verbose=True)
            n += 1
            written += 1
            state["next_batch"] = n
            state.setdefault("campaign_start", start.isoformat())
            state.setdefault("span_days", span)
            state["seed"] = seed
            save_state(outdir, state)
            if args.count and written >= args.count:
                break
            time.sleep(max(0.0, args.interval * rng.uniform(1 - args.jitter, 1 + args.jitter)))
    except KeyboardInterrupt:
        print("\nstopped.")
    finally:
        if truth:
            truth.close()
    print(f"{written} batches written.")


def build_parser() -> argparse.ArgumentParser:
    """Define the command-line options shown by `python data_generator.py --help`."""
    p = argparse.ArgumentParser(
        description="Generate deliberately messy fake lab instrument data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Quick start:")[1] if "Quick start:" in __doc__ else None,
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        """Add the options shared by both backfill and stream."""
        sp.add_argument("--out", default="data/incoming", help="output directory")
        sp.add_argument("--seed", type=int, default=7, help="RNG seed (reproducible runs)")
        sp.add_argument("--chaos", type=float, default=1.0,
                        help="0 = pristine data, 1 = normal lab, 2+ = cursed")
        sp.add_argument("--truth", default=None,
                        help="write a per-batch answer-key CSV here (keep it OUT of --out)")

    b = sub.add_parser("backfill", help="write a historical campaign all at once")
    common(b)
    b.add_argument("--days", type=int, default=45)
    b.add_argument("--min-per-day", type=int, default=2)
    b.add_argument("--max-per-day", type=int, default=7)
    b.add_argument("--end", default=None, help="ISO date to end at (default: now)")
    b.add_argument("--quiet", dest="verbose", action="store_false")
    b.set_defaults(func=cmd_backfill, verbose=True)

    s = sub.add_parser("stream", help="keep dropping new batches into the directory")
    common(s)
    s.add_argument("--interval", type=float, default=8.0, help="seconds between batches")
    s.add_argument("--jitter", type=float, default=0.4, help="0..1 timing randomness")
    s.add_argument("--count", type=int, default=0, help="stop after N batches (0 = forever)")
    s.set_defaults(func=cmd_stream)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    args.chaos = max(0.0, args.chaos)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())

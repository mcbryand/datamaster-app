from __future__ import annotations

from pathlib import Path
import os
import re

import pdfplumber
import pandas as pd

from app.engine_datamaster import parse_pdfs_to_raw, _clean, _is_noise, ACC_RE, DATE_RE

PDF_PATH = Path("SME_KDS 1.PDF")

BAD_IN_NAME = re.compile(
    r"(?:IDR|USD|CNY|KREDIT|BANK GARANSI|TIME LOAN|TRUST RECEIPT|L/C|LETTER OF CREDIT)",
    re.I
)

def _extract_facility_like_lines(pdf_path: Path) -> list[tuple[int,int,str]]:
    """
    Facility-like = ada ACC (10 digit) dan >=2 tanggal (dd-mm-yyyy)
    Return list of (page_idx, line_idx, line_text)
    """
    out: list[tuple[int,int,str]] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for p_i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            raw = [_clean(x) for x in text.split("\n")]
            raw = [x for x in raw if x and (not _is_noise(x))]
            for l_i, ln in enumerate(raw, start=1):
                if ACC_RE.search(ln) and len(DATE_RE.findall(ln)) >= 2:
                    out.append((p_i, l_i, ln))
    return out

def _show_context(pdf_path: Path, needle: str, window: int = 2, max_hits: int = 3) -> None:
    """
    Print context lines around needle (ACC) for debugging.
    """
    hits = 0
    with pdfplumber.open(str(pdf_path)) as pdf:
        for p_i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            raw = [_clean(x) for x in text.split("\n")]
            raw = [x for x in raw if x and (not _is_noise(x))]

            for l_i, ln in enumerate(raw):
                if needle in ln:
                    hits += 1
                    a = max(0, l_i - window)
                    b = min(len(raw), l_i + window + 1)
                    print(f"\n[CONTEXT] page={p_i} line={l_i+1} needle={needle}")
                    for k in range(a, b):
                        mark = ">>" if k == l_i else "  "
                        print(f"{mark} {k+1:03d}: {raw[k]}")
                    if hits >= max_hits:
                        return

def main():
    if not PDF_PATH.exists():
        raise SystemExit(f"SMOKE_FAIL: PDF tidak ditemukan: {PDF_PATH}")

    df = parse_pdfs_to_raw([PDF_PATH])

    if df is None or df.empty:
        raise SystemExit("SMOKE_FAIL: Output kosong (0 rows).")

    # 1) Nama tidak boleh tercemar currency/fasilitas
    bad = df[df["Nama"].astype(str).str.contains(BAD_IN_NAME, regex=True)]
    if len(bad) > 0:
        print("\n--- BAD NAME SAMPLE ---")
        print(bad[["Nama", "ACC No", "Fasilitas", "Cabang"]].head(10).to_string(index=False))
        raise SystemExit(f"SMOKE_FAIL: Nama tercemar fasilitas/currency: {len(bad)} baris")

    # 2) Nama tidak boleh AO-like (diawali "- ")
    ao_like = df[df["Nama"].astype(str).str.match(r"^\s*-\s+", na=False)]
    if len(ao_like) > 0:
        print("\n--- AO-LIKE NAME SAMPLE ---")
        print(ao_like[["Nama","ACC No","Fasilitas"]].head(10).to_string(index=False))
        raise SystemExit(f"SMOKE_FAIL: Nama AO masih nempel (AO-like): {len(ao_like)} baris")

    # 3) INVARIANT KRITIS: 1 ACC No tidak boleh dipakai >1 Nama (indikasi salah tempel debitur)
    g = df.groupby("ACC No")["Nama"].nunique()
    multi_owner = g[g > 1]
    if len(multi_owner) > 0:
        acc = str(multi_owner.index[0])
        print("\n--- ACC MULTI OWNER (SAMPLE) ---")
        print(multi_owner.head(10).to_string())
        print("\n--- ROWS FOR SAMPLE ACC ---")
        print(df[df["ACC No"].astype(str) == acc][["Nama","ACC No","Fasilitas","Plafond","Tgl PMK/PK Akhir"]].to_string(index=False))

        # print context dari PDF biar kelihatan harusnya nempel ke siapa
        _show_context(PDF_PATH, acc, window=3, max_hits=3)

        raise SystemExit(f"SMOKE_FAIL: ACC No dipakai >1 debitur (contoh ACC={acc}). Total ACC bermasalah={len(multi_owner)}")

    # 4) Coverage check: hitung facility-like lines di PDF vs rows output
    facility_lines = _extract_facility_like_lines(PDF_PATH)
    pdf_fac_count = len(facility_lines)
    out_rows = len(df)

    # toleransi kecil untuk merge/split edge cases
    if out_rows < int(pdf_fac_count * 0.90):
        print(f"\n--- COVERAGE FAIL ---")
        print(f"PDF facility-like lines: {pdf_fac_count}")
        print(f"Output rows: {out_rows}")
        print("\nSample facility-like lines (first 10):")
        for p_i, l_i, ln in facility_lines[:10]:
            print(f"p{p_i} ln{l_i}: {ln}")
        raise SystemExit("SMOKE_FAIL: Output rows jauh lebih sedikit dari facility-like lines di PDF (parser drop baris).")

    borrowers = df["Nama"].nunique()
    print(f"SMOKE_OK. Rows: {out_rows} Borrowers: {borrowers} PDF_FacilityLike: {pdf_fac_count}")

if __name__ == "__main__":
    main()

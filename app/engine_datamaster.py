from __future__ import annotations

import re
import os
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
import pdfplumber

# ========= Regex dasar =========
DATE_RE = re.compile(r"\b(\d{2}-\d{2}-\d{4})\b")
ACC_RE  = re.compile(r"\b(\d{10})\b")
CIF_RE  = re.compile(r"\((\d{11})\)")

ROWSTART_RE = re.compile(r"^\s*(\d+)\s+")  # nomor urut di awal baris

# Borrower anchor yg bisa muncul di tengah baris:
# "5 UTOMO (00018964777)" atau "10 SUBUR JAYA ... (000...)"
EMBED_BORROWER_RE = re.compile(r"\b\d+\s+[^()]{2,120}\(\d{11}\)")

CORP_MARKERS = {"PT", "CV", "UD", "PD", "TB", "KOPERASI"}
NON_NAME_NOISE = {"IDR", "USD", "CNY", "LOAN", "INSTALLMENT"}

# Kata-kata sektor/komoditi/group yang tidak boleh masuk nama
SECTOR_WORDS_RE = re.compile(
    r"\b("
    r"PETERNAKAN|MAKANAN|DISTRIBUSI|PROPERTI|TELEKOMUNIKASI|OTOMOTIF|"
    r"TRANSPORTASI|BAHAN|PERKEBUNAN|MEDIA|JASA|PERTAMBANGAN|FARMASI|"
    r"TEKSTIL|PRASARANA|HASIL|INDUSTRI|KEBUTUHAN|PERALATAN|PACKAGING|"
    r"KONSTRUKSI|MIGAS|LISTRIK|LOGAM|KIMIA|PLASTIK|KAYU|KEHUTANAN|"
    r"RETAILER|TOSERBA|ROKOK|TEMBAKAU|RESTORAN|ELEKTRONIK|ALAT-ALAT|"
    r"PERMESINAN|ALAT|BERAT|LOGISTIK|PERTANIAN|KONSUMEN|PERLENGKAPAN|"
    r"SEJENISNYA"
    r")\b",
    flags=re.IGNORECASE
)

# ========= Helpers =========
def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _is_noise(line: str) -> bool:
    if not line:
        return True
    u = line.strip()

    prefixes = (
        "LAPORAN JATUH TEMPO",
        "CABANG",
        "DATA PER",
        "KODE AO",
        "NO NAMA",
        "MIS;",
        "HAL :",
    )
    if u.upper().startswith(prefixes):
        return True

    # Header AO style: "20810 - KONDANG SISWONO" -> bukan data borrower
    if re.match(r"^\d{4,6}\s*-\s*[A-Z][A-Z .'/,-]{2,}$", u.upper()):
        return True

    return False

def cabang_from_filename(pdf_path: Path) -> str:
    """
    Support:
      - SME_KDS.PDF
      - SME_KDS 1.PDF
      - SME-KDS.pdf
      - SME_SMG.PDF, SME_CLC.PDF, dll
    """
    name = pdf_path.name.upper()
    m = re.search(r"SME[\s_\-]*([A-Z]{3})", name)
    return m.group(1) if m else ""

def _parse_amount_token(tok: str | None) -> int | None:
    if not tok:
        return None
    t = tok.replace(",", "").strip()
    if not t:
        return None
    if not re.fullmatch(r"\d+", t):
        return None
    try:
        return int(t)
    except Exception:
        return None

def normalize_fasilitas(f: str) -> str:
    s = _clean(f).upper()
    s = re.sub(r"\s+", " ", s).strip()

    # normalisasi facility prioritas
    if "LETTER OF CREDIT" in s or "L/C" in s or re.search(r"\bLC\b", s):
        return "L/C"
    if "TRUST RECEIPT" in s or re.search(r"\bTR\b", s):
        return "TRUST RECEIPT"
    if "KREDIT MULTI" in s or "MULTI FASILITAS" in s:
        return "KREDIT MULTI FASILITAS"
    if "TIME LOAN" in s or re.search(r"\bTL\b", s):
        return "TIME LOAN"
    if "KREDIT LOKAL" in s or re.search(r"\bKL\b", s):
        return "KREDIT LOKAL"
    if "BANK GARANSI" in s or re.search(r"\bBG\b", s):
        return "BANK GARANSI"

    return s

def is_priority_facility(f: str) -> bool:
    u = (f or "").upper()
    return any(
        k in u
        for k in (
            "BANK GARANSI",
            "KREDIT LOKAL",
            "TIME LOAN",
            "KREDIT MULTI FASILITAS",
            "L/C",
            "TRUST RECEIPT",
        )
    )

def _clean_company_name(tokens: list[str]) -> list[str]:
    upp = [re.sub(r"[^\w/&\-.']", "", t).upper() for t in tokens]
    for i, u in enumerate(upp):
        if u in {"PT", "CV"}:
            return tokens[: i + 1]
    return tokens

def _dedup(tokens: list[str]) -> list[str]:
    seen = set()
    out = []
    for t in tokens:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out

def _looks_bad_name(name: str) -> bool:
    if not name:
        return True
    u = name.upper()
    # tidak boleh tercemar currency/facility
    if " IDR " in f" {u} " or " USD " in f" {u} " or " CNY " in f" {u} ":
        return True
    if any(k in u for k in ["KREDIT", "BANK GARANSI", "TIME LOAN", "TRUST RECEIPT", "L/C", "LETTER OF CREDIT"]):
        return True
    if DATE_RE.search(name) or ACC_RE.search(name):
        return True
    return False

def _derive_name_and_cif(line: str) -> tuple[str, str]:
    """
    Extract borrower name + cif from a line that contains CIF.
    IMPORTANT: Nama tidak boleh mengandung sektor/komoditi/group.
    """
    s = _clean(line)
    s = re.sub(r"^\s*-\s+", "", s).strip()
    cif_m = CIF_RE.search(s)
    if not cif_m:
        return "", ""
    cif = cif_m.group(1)

    # buang prefix MILIK/MILIKI jika ada
    s2 = re.sub(r"^(MILIK(I)?)(\s*:\s*|\s+)+", "", s, flags=re.IGNORECASE).strip()

    # potong sebelum CIF token (kalau CIF muncul "normal")
    before_cif = _clean(s2[:cif_m.start()])

    # hapus nomor urut di depan
    before_cif = ROWSTART_RE.sub("", before_cif).strip()

    # AO sering muncul sebagai '- NAMA AO' setelah nomor urut: '1 - NAMA AO (CIF) ...'
    # Itu BUKAN debitur -> jangan jadikan borrower.
    if before_cif.lstrip().startswith('-'):
        return "", cif

    # kalau di before_cif ada ACC (berarti CIF datang belakangan), maka nama harus diambil
    # dari awal sampai sebelum ACC (dan sebelum sektor)
    acc_m = ACC_RE.search(s2)
    if acc_m and acc_m.start() < cif_m.start():
        seg = _clean(s2[ROWSTART_RE.match(s2).end():] if ROWSTART_RE.match(s2) else s2)
        seg = seg[:acc_m.start()].strip()
    else:
        seg = before_cif

    # buang sektor/komoditi/group dari seg
    seg = SECTOR_WORDS_RE.split(seg)[0].strip()
    toks = [t for t in re.split(r"\s+", seg) if t]

    # corporate vs personal
    upp = {t.upper() for t in toks}
    is_corp = bool(upp & CORP_MARKERS)

    if is_corp:
        toks = _clean_company_name(toks)
    else:
        toks = toks[:3]  # batasi personal name

    toks = [re.sub(r"[^\w/&\-.']", "", t) for t in toks]
    toks = [t for t in toks if t and t.upper() not in NON_NAME_NOISE]
    toks = _dedup(toks)

    name = _clean(" ".join(toks))
    if _looks_bad_name(name):
        return "", cif
    return name, cif

def _split_embedded_borrowers(lines: list[str]) -> list[str]:
    """
    Split jika ada borrower anchor di tengah baris:
    contoh: "BANK GARANSI ... 5 UTOMO (000...) ..."
    maka dipecah jadi:
      - "BANK GARANSI ..."
      - "5 UTOMO (000...) ..."
    Ini wajib supaya facility tidak nyampur ke nama.
    """
    out: list[str] = []
    for ln in lines:
        # --- FIX: remove AO name placed right before CIF (general) ---
        ln = re.sub(r"\s*-\s*[A-Za-z][A-Za-z .,'/-]{2,}(?=\s*\(\d{11}\))", "", ln).strip()

        s = _clean(ln)
        if not s:
            continue

        # kalau ada anchor dan bukan diawal, split semua occurrence
        matches = list(EMBED_BORROWER_RE.finditer(s))
        if not matches:
            out.append(s)
            continue

        # jika anchor pertama mulai di 0 -> tetap bisa ada anchor kedua di tengah, split juga
        cut_points = [m.start() for m in matches if m.start() != 0]
        if not cut_points:
            out.append(s)
            continue

        # split berurutan berdasarkan cut_points
        pts = [0] + sorted(cut_points) + [len(s)]
        for a, b in zip(pts, pts[1:]):
            part = _clean(s[a:b])
            if part:
                out.append(part)
    return out

def _merge_wrapped(lines: list[str]) -> list[str]:
    """
    Merge borrower lines and facility lines split across rows.

    Fixes:
    1) Borrower split where CIF appears on next line (standard)
    2) Borrower split where CIF appears on next line BUT line starts with "- AO ..." (common)
       Example:
         "LILIK SOESILOWATI"
         "- LIE KEZIA (00009461155) DISTRIBUSI..."
       => merged into:
         "LILIK SOESILOWATI (00009461155) DISTRIBUSI..."
    3) Do NOT merge L/C facility lines (often CNY and next line can be new borrower)
    """
    out: list[str] = []
    i = 0

    def _looks_like_borrower_stub(x: str) -> bool:
        # Heuristic: no ACC, not mostly digits, has >=2 alpha tokens
        if not x:
            return False
        if ACC_RE.search(x):
            return False
        if DATE_RE.search(x):
            return False
        toks = [t for t in x.replace("/", " ").split() if t]
        alpha = sum(1 for t in toks if re.search(r"[A-Za-z]", t))
        digit = sum(1 for t in toks if re.fullmatch(r"[\d,.-]+", t))
        return alpha >= 2 and alpha >= digit

    while i < len(lines):
        line = lines[i] or ""
        nxt = lines[i + 1] if i + 1 < len(lines) else ""
        nxt2 = lines[i + 2] if i + 2 < len(lines) else ""

        line = _clean(line)
        nxt = _clean(nxt) if nxt else ""
        nxt2 = _clean(nxt2) if nxt2 else ""

        if not line:
            i += 1
            continue

        # -------------------------
        # BORROWER WRAP (standard):
        # ".... NAMA"  (no CIF) + next line contains (CIF)
        # -------------------------
        if (not CIF_RE.search(line)) and nxt and CIF_RE.search(nxt):
            # Case A: rowstart line (classic)
            if ROWSTART_RE.match(line):
                # Guard: baris header AO seperti '20810 - KONDANG SISWONO' bukan borrower stub
                if re.match(r'^\d{4,6}\s*-\s*[A-Z]', line.upper()):
                    out.append(line)
                    i += 1
                    continue

                comb = _clean(line + " " + nxt)
                out.append(comb)
                i += 2
                continue

            # Case B: borrower stub without rowstart, then AO/CIF on next line
            # Example:
            #   "LILIK SOESILOWATI"
            #   "- LIE KEZIA (00009461155) ..."
            if _looks_like_borrower_stub(line) and (nxt.strip().startswith("-") or nxt.strip().startswith("(") or CIF_RE.search(nxt)):
                comb = _clean(line + " " + nxt)

                # Remove "- AO NAME" right before CIF (general)
                comb = re.sub(
                    r"\s*-\s*[A-Za-z][A-Za-z .,'/-]{2,}(?=\s*\(\d{11}\))",
                    "",
                    comb
                )
                out.append(_clean(comb))
                i += 2
                continue

        # FACILITY WRAP (non L/C):
        # ACC present but currency/dates missing => merge with next line(s)
        # -------------------------
        u = line.upper()
        is_lc = ("L/C" in u) or ("LETTER OF CREDIT" in u)

        if ACC_RE.search(line) and (not is_lc):
            missing_currency = (
                " IDR " not in f" {line} "
                and " USD " not in f" {line} "
                and " CNY " not in f" {line} "
            )
            missing_dates = len(DATE_RE.findall(line)) < 2

            if missing_currency or missing_dates:
                comb = _clean(line + " " + nxt)

                missing_currency2 = (
                    " IDR " not in f" {comb} "
                    and " USD " not in f" {comb} "
                    and " CNY " not in f" {comb} "
                )
                missing_dates2 = len(DATE_RE.findall(comb)) < 2

                if nxt2 and (missing_currency2 or missing_dates2):
                    out.append(_clean(comb + " " + nxt2))
                    i += 3
                    continue

                out.append(_clean(comb))
                i += 2
                continue

        out.append(_clean(line))
        i += 1

    return [x for x in out if x]

def _parse_facility_from_line(line: str) -> dict | None:
    """
    Parse facility line (harus ada ACC + currency + >=2 dates)
    Output:
      acc, fasilitas, plafond, dt_akhir
    Plafond: diambil 'apa adanya' = token angka pertama setelah currency.
    """
    s = _clean(line)
    accs = ACC_RE.findall(s)
    dates = DATE_RE.findall(s)
    if not accs or len(dates) < 2:
        return None

    acc = accs[0]
    toks = s.split()

    # cari index ACC
    try:
        i_acc = toks.index(acc)
    except ValueError:
        return None

    # cari currency
    currency_idx = None
    for j in range(i_acc + 1, len(toks)):
        if toks[j] in ("IDR", "USD", "CNY"):
            currency_idx = j
            break
    if currency_idx is None:
        return None

    fasilitas_raw = " ".join(toks[i_acc + 1:currency_idx]).strip()
    fasilitas = normalize_fasilitas(fasilitas_raw)

    # plafond "apa adanya": token angka pertama setelah currency
    plaf = None
    if currency_idx + 1 < len(toks):
        plaf = _parse_amount_token(toks[currency_idx + 1])

    try:
        dt_akhir = datetime.strptime(dates[-1], "%d-%m-%Y").date()
    except Exception:
        dt_akhir = None

    return {"acc": acc, "fasilitas": fasilitas, "plafond": plaf, "dt_akhir": dt_akhir}


def _parse_facility_from_line_no_acc(line: str, acc_hint: str | None) -> dict | None:
    """
    Facility line tanpa ACC (karena split kolom PDF).
    Pakai acc_hint dari facility sebelumnya dalam borrower yang sama.
    Syarat: ada currency + >=2 dates.
    """
    if not acc_hint:
        return None

    s = _clean(line)
    dates = DATE_RE.findall(s)
    if len(dates) < 2:
        return None
    if not re.search(r"\b(IDR|USD|CNY)\b", s):
        return None
    if ACC_RE.search(s):
        return None  # kalau ada ACC, biarkan parser normal

    toks = s.split()

    # cari currency index
    cur_idx = None
    for j, t in enumerate(toks):
        if t in ("IDR", "USD", "CNY"):
            cur_idx = j
            break
    if cur_idx is None:
        return None

    # kandidat fasilitas: token sebelum currency, buang sektor dulu
    left = " ".join(toks[:cur_idx]).strip()
    left = SECTOR_WORDS_RE.split(left)[0].strip()
    u = left.upper()

    # deteksi fasilitas dari keyword prioritas, fallback ke tail token
    fasilitas_guess = ""
    for key in (
        "BANK GARANSI",
        "KREDIT LOKAL",
        "TIME LOAN",
        "KREDIT MULTI",
        "MULTI FASILITAS",
        "TRUST RECEIPT",
        "LETTER OF CREDIT",
        "L/C",
    ):
        if key in u:
            fasilitas_guess = key
            break
    if not fasilitas_guess:
        tail = left.split()[-4:]
        fasilitas_guess = " ".join(tail).strip()

    # Guard: kalau tidak bisa tebak fasilitas, jangan buat row sampah
    if not fasilitas_guess or not fasilitas_guess.strip():
        return None

    fasilitas = normalize_fasilitas(fasilitas_guess)

    # plafond: angka pertama setelah currency
    plaf = None
    if cur_idx + 1 < len(toks):
        plaf = _parse_amount_token(toks[cur_idx + 1])

    try:
        dt_akhir = datetime.strptime(dates[-1], "%d-%m-%Y").date()
    except Exception:
        dt_akhir = None

    return {"acc": str(acc_hint), "fasilitas": fasilitas, "plafond": plaf, "dt_akhir": dt_akhir}

def parse_pdfs_to_raw(pdf_paths: Iterable[Path]) -> pd.DataFrame:
    rows: list[dict] = []
    current_name = ""
    current_cif = ""

    last_acc = None
    for pdf_path in pdf_paths:
        cabang_code = cabang_from_filename(pdf_path)

        with pdfplumber.open(str(pdf_path)) as pdf:
            for page_i, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                raw_lines = [_clean(x) for x in text.split("\n")]
                raw_lines = [l for l in raw_lines if not _is_noise(l)]

                # 1) split borrower yang nyelip di tengah baris (akar masalah)
                raw_lines = _split_embedded_borrowers(raw_lines)

                # 2) merge wrapped yang aman
                lines = _merge_wrapped(raw_lines)

                for ln_i, ln in enumerate(lines, start=1):
                    ln = _clean(ln)
                    if _is_noise(ln):
                        continue

                    # --- AO line: jangan pernah jadi borrower, tapi facility-nya tetap diproses ---
                    is_ao_line = ln.lstrip().startswith("-") and CIF_RE.search(ln)

                    # (A) Update borrower state jika ada CIF dan nama valid (TANPA syarat rowstart)
                    if CIF_RE.search(ln) and (not is_ao_line):
                        nm, cif = _derive_name_and_cif(ln)
                        if nm and cif:
                            current_name, current_cif = nm, cif

                    # (B) Parse facility dari baris ini (baik borrower line maupun facility line)
                    fac = _parse_facility_from_line(ln)
                    if not fac:
                        fac = _parse_facility_from_line_no_acc(ln, last_acc)
                    if fac and current_name and current_cif:
                        nama_full = f"{current_name} ({current_cif})"
                        rows.append({
                            "Nama": nama_full,
                            "ACC No": str(fac["acc"]),
                            "Cabang": cabang_code,
                            "Fasilitas": str(fac["fasilitas"]).upper(),
                            "Plafond": fac["plafond"],  # plafon apa adanya (1 tetap 1)
                            "Tgl PMK/PK Akhir": fac["dt_akhir"],
                            "SumberFile": pdf_path.name,
                            "IsPriority": bool(is_priority_facility(str(fac["fasilitas"]))),
                        })
                        last_acc = str(fac["acc"]) if fac else last_acc
                    else:
                        if os.environ.get("DM_DEBUG") == "1":
                            if (ACC_RE.search(ln) and DATE_RE.search(ln)) and (not (current_name and current_cif)):
                                print(f"[ORPHAN_FACILITY] p{page_i} ln{ln_i}: {ln}", flush=True)

    df = pd.DataFrame(rows)
    if not df.empty and "Nama" in df.columns:
        # Safety: bersihkan kalau ada sisa prefix "- " (harusnya sudah tidak ada)
        df["Nama"] = df["Nama"].astype(str).str.replace(r"^\s*-\s*", "", regex=True)

    if not df.empty:
        df["ACC No"] = df["ACC No"].astype(str)  # keep leading zero
        df["Fasilitas"] = df["Fasilitas"].astype(str).str.upper().str.replace(r"\s+", " ", regex=True)


    # Dedup: PDF sering mengulang facility line yang sama
    if not df.empty:
        df = df.drop_duplicates(subset=["Nama","ACC No","Fasilitas","Plafond","Tgl PMK/PK Akhir"], keep="first")
    return df


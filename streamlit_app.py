import io
import zipfile
from pathlib import Path
import streamlit as st

from app.engine_datamaster import parse_pdfs_to_raw
from app.excel_datamaster import build_outputs

st.set_page_config(page_title="DATA MASTER", layout="centered")

BASE_DIR = Path(__file__).resolve().parent
TMP_DIR = BASE_DIR / "tmp"
OUT_DIR = BASE_DIR / "output"
TMP_DIR.mkdir(exist_ok=True)
OUT_DIR.mkdir(exist_ok=True)

st.title("DATA MASTER – Upload PDF")
files = st.file_uploader("Pilih PDF (bisa multi file)", type=["pdf"], accept_multiple_files=True)

if "done" not in st.session_state:
    st.session_state.done = False
    st.session_state.full_bytes = None
    st.session_state.zip_bytes = None

if st.button("Process", disabled=not files):
    job_id = "streamlit_job"
    job_tmp = TMP_DIR / job_id
    job_tmp.mkdir(parents=True, exist_ok=True)

    pdf_paths = []
    for f in files:
        dest = job_tmp / f.name
        dest.write_bytes(f.getbuffer())
        pdf_paths.append(dest)

    with st.spinner("Processing..."):
        raw = parse_pdfs_to_raw(pdf_paths)
        outputs = build_outputs(raw, OUT_DIR, job_id)

        # baca hasil excel
        full_path = outputs["full"]
        st.session_state.full_bytes = full_path.read_bytes()

        # ZIP: kalau file zip kamu kosong (placeholder), kita bikin zip real dari FULL saja
        zbuf = io.BytesIO()
        with zipfile.ZipFile(zbuf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(full_path.name, st.session_state.full_bytes)
        st.session_state.zip_bytes = zbuf.getvalue()

        st.session_state.done = True

if st.session_state.done:
    st.success("Done. Silakan download.")
    st.download_button(
        "Download FULL (XLSX)",
        data=st.session_state.full_bytes,
        file_name="gabungan.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    st.download_button(
        "Download ZIP",
        data=st.session_state.zip_bytes,
        file_name="hasil.zip",
        mime="application/zip",
    )

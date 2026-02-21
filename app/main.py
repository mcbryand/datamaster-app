from __future__ import annotations

import os
import uuid
from pathlib import Path
from datetime import datetime, timedelta

from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse

from app.engine_datamaster import parse_pdfs_to_raw
from app.excel_datamaster import build_outputs

BASE_DIR = Path(__file__).resolve().parents[1]
TMP_DIR = BASE_DIR / "tmp"
OUT_DIR = BASE_DIR / "output"
TMP_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

TOKENS: dict[str, dict] = {}

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
      <head>
        <meta name="robots" content="noindex,nofollow"/>
        <meta charset="utf-8"/>
        <title>DATA MASTER</title>
        <style>
          body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Arial; margin: 40px; }
          .box { max-width: 720px; padding: 20px; border: 1px solid #ddd; border-radius: 12px; }
          .row { margin: 12px 0; }
          button { padding: 10px 14px; border: 0; border-radius: 10px; cursor: pointer; }
          button:disabled { opacity: .6; cursor: not-allowed; }
          .btn { background: #111; color: white; }
          .btn2 { background: #2c6bed; color: white; text-decoration: none; padding: 10px 14px; border-radius: 10px; display: inline-block; }
          .muted { color: #666; font-size: 13px; }
          .ok { color: #0a7a2f; }
          .err { color: #b00020; }
          #links { display:none; gap:10px; }
        </style>
      </head>
      <body>
        <div class="box">
          <h2>DATA MASTER – Upload PDF</h2>
          <div class="row">
            <input id="files" type="file" multiple accept="application/pdf"/>
          </div>
          <div class="row">
            <button id="btn" class="btn" onclick="start()">Process</button>
          </div>

          <div class="row muted" id="status">Status: idle</div>

          <div class="row" id="links">
            <a id="full" class="btn2" href="#" download>Download FULL (XLSX)</a>
            <a id="zip" class="btn2" href="#" download>Download ZIP per-bulan</a>
          </div>

          <div class="row muted">
            Catatan: link download berlaku 7 hari.
          </div>
        </div>

        <script>
          async function start(){
            const btn = document.getElementById("btn");
            const status = document.getElementById("status");
            const files = document.getElementById("files").files;

            document.getElementById("links").style.display = "none";

            if(!files || files.length === 0){
              status.innerHTML = '<span class="err">Status: pilih file PDF dulu.</span>';
              return;
            }

            btn.disabled = true;
            status.innerHTML = "Status: uploading...";

            const fd = new FormData();
            for (const f of files) fd.append("files", f);

            let jobId = null;

            try {
              const resp = await fetch("/api/jobs", { method: "POST", body: fd });
              if(!resp.ok){
                const t = await resp.text();
                throw new Error("Upload gagal: " + t);
              }
              const data = await resp.json();
              jobId = data.job_id;
              status.innerHTML = "Status: processing... (job_id: " + jobId + ")";
            } catch (e) {
              status.innerHTML = '<span class="err">Status: ' + e.message + '</span>';
              btn.disabled = false;
              return;
            }

            const maxWaitMs = 10 * 60 * 1000; // 10 menit
            const startTime = Date.now();

            while(true){
              await new Promise(r => setTimeout(r, 1200));

              if(Date.now() - startTime > maxWaitMs){
                status.innerHTML = '<span class="err">Status: timeout. Coba refresh dan cek lagi.</span>';
                btn.disabled = false;
                return;
              }

              try {
                const sresp = await fetch("/api/jobs/" + jobId);
                if(!sresp.ok){
                  const t = await sresp.text();
                  throw new Error("Status check gagal: " + t);
                }
                const sdata = await sresp.json();
                const st = (sdata.status || "").toLowerCase();

                if(st === "done"){
                  if(!sdata.download_full || !sdata.download_zip){
                    throw new Error("Status done tapi link download tidak tersedia.");
                  }

                  status.innerHTML = '<span class="ok">Status: done. Silakan download. (job_id: ' + jobId + ')</span>';
                  document.getElementById("full").href = sdata.download_full;
                  document.getElementById("zip").href = sdata.download_zip;
                  document.getElementById("links").style.display = "flex";
                  btn.disabled = false;
                  return;
                } else {
                  status.innerHTML = "Status: " + st + "... (job_id: " + jobId + ")";
                }
              } catch(e){
                status.innerHTML = '<span class="err">Status: ' + e.message + '</span>';
                btn.disabled = false;
                return;
              }
            }
          }
        </script>
      </body>
    </html>
    """

@app.post("/api/jobs")
async def create_job(background: BackgroundTasks, files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(400, "No files uploaded")

    job_id = str(uuid.uuid4())
    job_tmp = TMP_DIR / job_id
    job_tmp.mkdir(parents=True, exist_ok=True)

    pdf_paths: list[Path] = []
    for f in files:
        if not f.filename.lower().endswith(".pdf"):
            raise HTTPException(400, f"Only PDF allowed: {f.filename}")
        dest = job_tmp / f.filename
        dest.write_bytes(await f.read())
        pdf_paths.append(dest)

    background.add_task(process_job, job_id, pdf_paths)
    return JSONResponse({"job_id": job_id, "status": "processing"})

@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    full_path = OUT_DIR / "FULL" / f"{job_id}_gabungan.xlsx"
    zip_path  = OUT_DIR / "FULL" / f"{job_id}_per-bulan.zip"

    if not (full_path.exists() and zip_path.exists()):
        return {"job_id": job_id, "status": "processing"}

    def token_for(path: Path) -> str:
        for t, meta in TOKENS.items():
            if meta.get("path") == str(path):
                return t
        t = str(uuid.uuid4())
        TOKENS[t] = {"path": str(path), "expires_at": datetime.utcnow() + timedelta(days=7)}
        return t

    return {
        "job_id": job_id,
        "status": "done",
        "download_full": f"/download/{token_for(full_path)}",
        "download_zip": f"/download/{token_for(zip_path)}",
    }

@app.get("/download/{token}")
def download(token: str):
    meta = TOKENS.get(token)
    if not meta:
        raise HTTPException(404, "Invalid token")
    if datetime.utcnow() > meta["expires_at"]:
        TOKENS.pop(token, None)
        raise HTTPException(410, "Link expired")
    path = meta["path"]
    if not os.path.exists(path):
        raise HTTPException(404, "File not found")
    return FileResponse(path, filename=Path(path).name)

def process_job(job_id: str, pdf_paths: list[Path]):
    raw = parse_pdfs_to_raw(pdf_paths)
    outputs = build_outputs(raw, OUT_DIR, job_id)
    # token dibuat saat status check (job_status) supaya tidak perlu copy paste
    return

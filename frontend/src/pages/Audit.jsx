import React, { useEffect, useState } from "react";
import { getJSON, postJSON } from "../api.js";

export default function Audit({onError}){
  const [runs, setRuns] = useState([]);
  const [runId, setRunId] = useState("");
  const [report, setReport] = useState(null);

  useEffect(()=>{
    (async ()=>{
      try{
        const r = await getJSON("/api/calc/runs");
        setRuns(r);
        if(r.length) setRunId(String(r[0].id));
      }catch(e){ onError(String(e)); }
    })();
  }, []);

  async function audit(){
    try{
      const r = await postJSON("/api/audit/run/" + runId, {});
      setReport(r);
    }catch(e){ onError(String(e)); }
  }

  return (
    <div style={{border:"1px solid #eee", borderRadius:14, padding:14}}>
      <h3>Audit (Verifier/Auditor)</h3>
      <div style={{display:"flex", gap:10, alignItems:"center"}}>
        <select value={runId} onChange={e=>setRunId(e.target.value)} style={{padding:10, borderRadius:10, border:"1px solid #ddd"}}>
          {runs.map(r=>(
            <option key={r.id} value={r.id}>#{r.id} — {r.run_type} — {Number(r.total_tco2e).toFixed(6)} tCO2e</option>
          ))}
        </select>
        <button onClick={audit} style={{padding:"10px 12px", borderRadius:10, border:"1px solid #ddd", background:"white"}}>Run Audit</button>
      </div>

      {report && (
        <div style={{marginTop:14}}>
          <div style={{display:"flex", gap:14, alignItems:"baseline"}}>
            <div><b>Score</b>: {report.score}</div>
            <div style={{color:"#666", fontSize:12}}>critical {report.summary.critical} | major {report.summary.major} | minor {report.summary.minor} | info {report.summary.info}</div>
          </div>
          <pre style={{marginTop:10, background:"#f7f7f7", padding:12, borderRadius:12, overflow:"auto"}}>
{JSON.stringify(report, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}

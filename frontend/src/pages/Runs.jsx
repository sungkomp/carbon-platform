import React, { useEffect, useState } from "react";
import { getJSON, postJSON } from "../api.js";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

export default function Runs({onError}){
  const [activities, setActivities] = useState([]);
  const [selected, setSelected] = useState({});
  const [runs, setRuns] = useState([]);
  const [runType, setRunType] = useState("CFO");

  async function refresh(){
    setActivities(await getJSON("/api/activities"));
    setRuns(await getJSON("/api/calc/runs"));
  }

  useEffect(()=>{ refresh().catch(e=>onError(String(e))); }, []);

  function toggle(id){
    setSelected(prev => ({...prev, [id]: !prev[id]}));
  }

  async function run(){
    try{
      const ids = Object.keys(selected).filter(k=>selected[k]).map(k=>Number(k));
      const r = await postJSON("/api/calc/run", { run_type: runType, activity_ids: ids });
      onError("");
      await refresh();
      alert("Run created: " + r.run_id + " total tCO2e=" + r.total_tco2e);
    }catch(e){ onError(String(e)); }
  }

  function exportFile(runId, ext){
    window.open(API_BASE + `/api/reports/run/${runId}.${ext}`, "_blank");
  }

  return (
    <div style={{display:"grid", gridTemplateColumns:"1fr 1fr", gap:14}}>
      <div style={{border:"1px solid #eee", borderRadius:14, padding:14}}>
        <h3>Create Run</h3>
        <div style={{display:"flex", gap:10, alignItems:"center"}}>
          <select value={runType} onChange={e=>setRunType(e.target.value)} style={{padding:10, borderRadius:10, border:"1px solid #ddd"}}>
            <option value="CFO">CFO</option>
            <option value="CFP">CFP</option>
          </select>
          <button onClick={run} style={{padding:"10px 12px", borderRadius:10, border:"1px solid #ddd", background:"white"}}>
            Run
          </button>
        </div>

        <div style={{marginTop:12, fontSize:12, color:"#666"}}>Select activities:</div>
        <div style={{display:"flex", flexDirection:"column", gap:8, marginTop:8}}>
          {activities.map(a=>(
            <label key={a.id} style={{display:"flex", gap:8, alignItems:"center", border:"1px solid #eee", borderRadius:12, padding:10}}>
              <input type="checkbox" checked={!!selected[a.id]} onChange={()=>toggle(a.id)} />
              <div style={{flex:1}}>
                <div style={{fontWeight:700}}>{a.name} <span style={{color:"#666", fontSize:12}}>#{a.id}</span></div>
                <div style={{color:"#666", fontSize:12}}>{a.ef_key}</div>
              </div>
            </label>
          ))}
        </div>
      </div>

      <div style={{border:"1px solid #eee", borderRadius:14, padding:14}}>
        <h3>Runs</h3>
        <div style={{display:"flex", flexDirection:"column", gap:10}}>
          {runs.map(r=>(
            <div key={r.id} style={{border:"1px solid #eee", borderRadius:14, padding:12}}>
              <div style={{display:"flex", gap:8, alignItems:"center"}}>
                <div style={{fontWeight:800}}>{r.run_type}</div>
                <div style={{color:"#666", fontSize:12}}>Run #{r.id}</div>
                <div style={{flex:1}} />
                <button onClick={()=>exportFile(r.id,"pdf")} style={{padding:"6px 10px", borderRadius:10, border:"1px solid #ddd", background:"white"}}>PDF</button>
                <button onClick={()=>exportFile(r.id,"xlsx")} style={{padding:"6px 10px", borderRadius:10, border:"1px solid #ddd", background:"white"}}>Excel</button>
              </div>
              <div style={{marginTop:6}}>
                <b>{Number(r.total_tco2e).toFixed(6)}</b> tCO2e
              </div>
              <div style={{color:"#666", fontSize:12}}>{r.created_at}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

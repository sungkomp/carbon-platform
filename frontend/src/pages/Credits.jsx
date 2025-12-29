import React, { useEffect, useState } from "react";
import { getJSON, postJSON } from "../api.js";

export default function Credits({onError}){
  const [projects, setProjects] = useState([]);
  const [form, setForm] = useState({
    project_code:"PJT_DEMO_001",
    name:"Demo Project",
    methodology:"Demo Methodology",
    baseline_tco2e: 10000,
    project_tco2e: 2000,
    leakage_tco2e: 200,
    buffer_pct: 0.1,
    vintage:"2025"
  });
  const [last, setLast] = useState(null);

  async function refresh(){
    setProjects(await getJSON("/api/credit/projects"));
  }

  useEffect(()=>{ refresh().catch(e=>onError(String(e))); }, []);

  function set(k, v){
    setForm(prev => ({...prev, [k]: v}));
  }

  async function save(){
    try{
      await postJSON("/api/credit/projects", form);
      await refresh();
      onError("");
    }catch(e){ onError(String(e)); }
  }

  async function calc(){
    try{
      const r = await postJSON("/api/credit/calc", {project_code: form.project_code});
      setLast(r);
      await refresh();
    }catch(e){ onError(String(e)); }
  }

  return (
    <div style={{display:"grid", gridTemplateColumns:"1fr 1fr", gap:14}}>
      <div style={{border:"1px solid #eee", borderRadius:14, padding:14}}>
        <h3>Carbon Credit Project Developer</h3>

        {["project_code","name","methodology","vintage"].map(k=>(
          <div key={k} style={{marginBottom:8}}>
            <label style={{display:"block", fontSize:13, color:"#444"}}>{k}</label>
            <input value={form[k]} onChange={e=>set(k, e.target.value)} style={{width:"100%", padding:10, borderRadius:10, border:"1px solid #ddd"}} />
          </div>
        ))}

        {["baseline_tco2e","project_tco2e","leakage_tco2e","buffer_pct"].map(k=>(
          <div key={k} style={{marginBottom:8}}>
            <label style={{display:"block", fontSize:13, color:"#444"}}>{k}</label>
            <input type="number" step="any" value={form[k]} onChange={e=>set(k, Number(e.target.value))} style={{width:"100%", padding:10, borderRadius:10, border:"1px solid #ddd"}} />
          </div>
        ))}

        <div style={{display:"flex", gap:8}}>
          <button onClick={save} style={{padding:"10px 12px", borderRadius:10, border:"1px solid #ddd", background:"white"}}>Save</button>
          <button onClick={calc} style={{padding:"10px 12px", borderRadius:10, border:"1px solid #ddd", background:"white"}}>Calculate Credit</button>
        </div>

        {last && (
          <div style={{marginTop:12, background:"#f7f7f7", padding:12, borderRadius:12}}>
            <div><b>Net credits:</b> {last.net_tco2e} tCO2e</div>
            <div style={{fontSize:12, color:"#666"}}>Gross: {last.gross_tco2e} | Buffer: {last.buffer_tco2e} | Run ID: {last.run_id}</div>
          </div>
        )}
      </div>

      <div style={{border:"1px solid #eee", borderRadius:14, padding:14}}>
        <h3>Projects</h3>
        <div style={{display:"flex", flexDirection:"column", gap:10}}>
          {projects.map(p=>(
            <div key={p.project_code} style={{border:"1px solid #eee", borderRadius:14, padding:12}}>
              <div style={{fontWeight:800}}>{p.project_code} — {p.name}</div>
              <div style={{fontSize:12, color:"#666"}}>{p.methodology} | vintage {p.vintage}</div>
              <div style={{marginTop:8, fontSize:13}}>
                baseline {p.baseline_tco2e} | project {p.project_tco2e} | leakage {p.leakage_tco2e} | buffer {p.buffer_pct}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

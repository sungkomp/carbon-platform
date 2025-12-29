import React, { useEffect, useMemo, useState } from "react";
import { getJSON, postJSON, postFile } from "../api.js";

function Field({spec, value, onChange}){
  const type = spec.type || "text";
  return (
    <div style={{marginBottom:8}}>
      <label style={{display:"block", fontSize:13, color:"#444"}}>{spec.label || spec.name} {spec.unit ? <span style={{color:"#888"}}>({spec.unit})</span> : null}</label>
      <input
        value={value ?? ""}
        onChange={e=>onChange(type==="number" ? (e.target.value===""? "" : Number(e.target.value)) : e.target.value)}
        type={type==="number" ? "number" : "text"}
        step="any"
        style={{width:"100%", padding:10, borderRadius:10, border:"1px solid #ddd"}}
      />
      {spec.help && <div style={{fontSize:12, color:"#777"}}>{spec.help}</div>}
    </div>
  );
}

export default function ActivitiesDynamic({onError}){
  const [efs, setEfs] = useState([]);
  const [activities, setActivities] = useState([]);
  const [efKey, setEfKey] = useState("");
  const [name, setName] = useState("");
  const [inputs, setInputs] = useState({});
  const [scope, setScope] = useState("Scope3");
  const [period, setPeriod] = useState("2025");
  const [file, setFile] = useState(null);

  async function refresh(){
    const e = await getJSON("/api/efs");
    setEfs(e);
    const a = await getJSON("/api/activities");
    setActivities(a);
  }

  useEffect(()=>{ refresh().catch(e=>onError(String(e))); }, []);

  const ef = useMemo(()=> efs.find(x=>x.key===efKey), [efs, efKey]);
  const spec = ef?.activity_id_fields || {};
  const fields = spec.fields || {};

  useEffect(()=>{
    if(!ef) return;
    const next = {};
    for(const [k, fs] of Object.entries(fields)){
      if(fs.default !== undefined) next[k] = fs.default;
    }
    setInputs(next);
  }, [efKey]);

  function setField(k, v){
    setInputs(prev => ({...prev, [k]: v}));
  }

  async function create(){
    try{
      await postJSON("/api/activities", { name: name || (ef?.name || "Activity"), ef_key: efKey, inputs, scope, period });
      setName("");
      setInputs({});
      await refresh();
    }catch(e){ onError(String(e)); }
  }

  async function del(id){
    try{
      await fetch((import.meta.env.VITE_API_BASE || "http://localhost:8000") + "/api/activities/" + id, {
        method:"DELETE",
        headers: { "Authorization": "Bearer " + localStorage.getItem("token") }
      });
      await refresh();
    }catch(e){ onError(String(e)); }
  }

  async function importActivities(){
    if(!file) return;
    try{
      await postFile("/api/activities/import", file);
      setFile(null);
      await refresh();
    }catch(e){ onError(String(e)); }
  }

  return (
    <div style={{display:"grid", gridTemplateColumns:"1fr 1fr", gap:14}}>
      <div style={{border:"1px solid #eee", borderRadius:14, padding:14}}>
        <h3>Create Activity (dynamic)</h3>

        <div style={{marginBottom:8}}>
          <label style={{display:"block", fontSize:13, color:"#444"}}>Emission Factor</label>
          <select value={efKey} onChange={e=>setEfKey(e.target.value)} style={{width:"100%", padding:10, borderRadius:10, border:"1px solid #ddd"}}>
            <option value="">-- select EF --</option>
            {efs.map(x=>(<option key={x.key} value={x.key}>{x.key} — {x.name}</option>))}
          </select>
        </div>

        <div style={{marginBottom:8}}>
          <label style={{display:"block", fontSize:13, color:"#444"}}>Activity name</label>
          <input value={name} onChange={e=>setName(e.target.value)} style={{width:"100%", padding:10, borderRadius:10, border:"1px solid #ddd"}} />
        </div>

        <div style={{display:"grid", gridTemplateColumns:"1fr 1fr", gap:10}}>
          <div>
            <label style={{display:"block", fontSize:13, color:"#444"}}>Scope</label>
            <select value={scope} onChange={e=>setScope(e.target.value)} style={{width:"100%", padding:10, borderRadius:10, border:"1px solid #ddd"}}>
              <option>Scope1</option><option>Scope2</option><option>Scope3</option>
            </select>
          </div>
          <div>
            <label style={{display:"block", fontSize:13, color:"#444"}}>Period</label>
            <input value={period} onChange={e=>setPeriod(e.target.value)} style={{width:"100%", padding:10, borderRadius:10, border:"1px solid #ddd"}} />
          </div>
        </div>

        <div style={{marginTop:10}}>
          {ef && spec.formula && (
            <div style={{background:"#f7f7f7", padding:10, borderRadius:12, marginBottom:10}}>
              <div style={{fontSize:12, color:"#555"}}><b>Formula</b>: {spec.formula.expression} → {spec.formula.output} ({spec.formula.unit})</div>
            </div>
          )}
          {Object.entries(fields).map(([k, fs])=>(
            <Field key={k} spec={{...fs, name:k}} value={inputs[k]} onChange={(v)=>setField(k, v)} />
          ))}
          <button disabled={!efKey} onClick={create} style={{padding:"10px 12px", borderRadius:10, border:"1px solid #ddd", background:"white"}}>
            Create
          </button>
        </div>

        <hr style={{margin:"16px 0"}} />

        <h4>Import activities (CSV/Excel)</h4>
        <div style={{fontSize:12, color:"#666", marginBottom:8}}>
          Required columns: <code>name</code>, <code>ef_key</code>. Optional: <code>inputs</code> (JSON), <code>scope</code>, <code>period</code>.
        </div>
        <input type="file" accept=".csv,.xlsx,.xls" onChange={e=>setFile(e.target.files?.[0] || null)} />
        <div style={{marginTop:8}}>
          <button onClick={importActivities} style={{padding:"10px 12px", borderRadius:10, border:"1px solid #ddd", background:"white"}}>
            Upload & Import
          </button>
        </div>
      </div>

      <div style={{border:"1px solid #eee", borderRadius:14, padding:14}}>
        <h3>Activities</h3>
        <div style={{fontSize:12, color:"#666", marginBottom:10}}>Total: {activities.length}</div>
        <div style={{display:"flex", flexDirection:"column", gap:10}}>
          {activities.map(a=>(
            <div key={a.id} style={{border:"1px solid #eee", borderRadius:14, padding:12}}>
              <div style={{display:"flex", gap:8, alignItems:"center"}}>
                <div style={{fontWeight:700}}>{a.name}</div>
                <div style={{color:"#666", fontSize:12}}>(#{a.id})</div>
                <div style={{flex:1}} />
                <button onClick={()=>del(a.id)} style={{padding:"6px 10px", borderRadius:10, border:"1px solid #ddd", background:"white"}}>Delete</button>
              </div>
              <div style={{fontSize:12, color:"#666"}}>{a.ef_key}</div>
              <pre style={{marginTop:8, background:"#f7f7f7", padding:10, borderRadius:12, overflow:"auto"}}>
{JSON.stringify(a.inputs, null, 2)}
              </pre>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

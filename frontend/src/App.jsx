import React, { useEffect, useState } from "react";
import { getJSON, postJSON, hasRole } from "./api.js";
import Login from "./pages/Login.jsx";
import ActivitiesDynamic from "./pages/ActivitiesDynamic.jsx";
import Credits from "./pages/Credits.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import Runs from "./pages/Runs.jsx";
import Audit from "./pages/Audit.jsx";

export default function App(){
  const [me, setMe] = useState(null);
  const [tab, setTab] = useState("dashboard");
  const [err, setErr] = useState("");

  async function loadMe(){
    try{
      const x = await getJSON("/api/auth/me");
      setMe(x);
    }catch(e){
      setMe(null);
    }
  }

  useEffect(()=>{ loadMe(); }, []);

  if(!me){
    return <Login onLogin={loadMe} />;
  }

  const roles = me.roles || [];
  const tabs = [
    {k:"dashboard", label:"Dashboard", show:true},
    {k:"activities", label:"Activities", show: hasRole(roles, "CALCULATOR","EXPERT","ADMIN")},
    {k:"runs", label:"Runs", show: hasRole(roles, "CALCULATOR","EXPERT","AUDITOR","VERIFIER","ADMIN")},
    {k:"credits", label:"Carbon Credit", show: hasRole(roles, "PROJECT_DEVELOPER","EXPERT","ADMIN")},
    {k:"audit", label:"Audit", show: hasRole(roles, "AUDITOR","VERIFIER","ADMIN")},
  ].filter(t=>t.show);

  return (
    <div style={{fontFamily:"system-ui", padding:16, maxWidth:1100, margin:"0 auto"}}>
      <h2>Carbon Platform</h2>
      <div style={{display:"flex", gap:8, flexWrap:"wrap", marginBottom:12}}>
        {tabs.map(t=>(
          <button key={t.k} onClick={()=>setTab(t.k)} style={{padding:"8px 12px", borderRadius:10, border:"1px solid #ddd", background: tab===t.k? "#eee":"white"}}>
            {t.label}
          </button>
        ))}
        <div style={{flex:1}} />
        <button onClick={()=>{localStorage.removeItem("token"); window.location.reload();}} style={{padding:"8px 12px", borderRadius:10, border:"1px solid #ddd", background:"white"}}>
          Logout
        </button>
      </div>

      {err && <div style={{background:"#ffe6e6", padding:10, borderRadius:10, marginBottom:10}}>{err}</div>}

      {tab==="dashboard" && <Dashboard />}
      {tab==="activities" && <ActivitiesDynamic onError={setErr} />}
      {tab==="runs" && <Runs onError={setErr} />}
      {tab==="credits" && <Credits onError={setErr} />}
      {tab==="audit" && <Audit onError={setErr} />}
    </div>
  );
}

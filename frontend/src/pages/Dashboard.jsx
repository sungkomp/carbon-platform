import React, { useEffect, useState } from "react";
import { getJSON } from "../api.js";

export default function Dashboard(){
  const [data, setData] = useState(null);
  const [err, setErr] = useState("");

  useEffect(()=>{
    (async ()=>{
      try{
        const r = await getJSON("/api/dashboard");
        setData(r);
      }catch(e){
        setErr(String(e));
      }
    })();
  }, []);

  return (
    <div>
      <h3>Dashboard</h3>
      {err && <div style={{background:"#ffe6e6", padding:10, borderRadius:10}}>{err}</div>}
      {!data ? <div>Loading...</div> :
        <div style={{display:"grid", gridTemplateColumns:"repeat(4, 1fr)", gap:10}}>
          {Object.entries(data.counts).map(([k,v])=>(
            <div key={k} style={{border:"1px solid #eee", borderRadius:14, padding:14}}>
              <div style={{color:"#666"}}>{k}</div>
              <div style={{fontSize:24, fontWeight:700}}>{v}</div>
            </div>
          ))}
        </div>
      }
    </div>
  );
}

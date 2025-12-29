import React, { useState } from "react";
import { postJSON } from "../api.js";

export default function Login({onLogin}){
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("admin1234");
  const [err, setErr] = useState("");

  async function submit(e){
    e.preventDefault();
    setErr("");
    try{
      const r = await postJSON("/api/auth/login", {username, password});
      localStorage.setItem("token", r.token);
      onLogin();
    }catch(ex){
      setErr(String(ex));
    }
  }

  return (
    <div style={{maxWidth:420, margin:"80px auto", padding:16, border:"1px solid #eee", borderRadius:14}}>
      <h3>Login</h3>
      <form onSubmit={submit}>
        <div style={{marginBottom:8}}>
          <label>Username</label>
          <input value={username} onChange={e=>setUsername(e.target.value)} style={{width:"100%", padding:10, borderRadius:10, border:"1px solid #ddd"}} />
        </div>
        <div style={{marginBottom:8}}>
          <label>Password</label>
          <input type="password" value={password} onChange={e=>setPassword(e.target.value)} style={{width:"100%", padding:10, borderRadius:10, border:"1px solid #ddd"}} />
        </div>
        <button style={{padding:"10px 12px", borderRadius:10, border:"1px solid #ddd", background:"white"}}>Login</button>
      </form>
      {err && <div style={{marginTop:10, background:"#ffe6e6", padding:10, borderRadius:10}}>{err}</div>}
      <p style={{color:"#666", marginTop:10}}>Dev default: admin / admin1234</p>
    </div>
  );
}

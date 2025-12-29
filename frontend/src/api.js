const BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

function authHeaders(){
  const token = localStorage.getItem("token");
  return token ? { "Authorization": "Bearer " + token } : {};
}

export async function getJSON(path){
  const r = await fetch(BASE + path, { headers: { ...authHeaders() } });
  if(!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function postJSON(path, body){
  const r = await fetch(BASE + path, {
    method: "POST",
    headers: { "Content-Type":"application/json", ...authHeaders() },
    body: JSON.stringify(body)
  });
  if(!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function postFile(path, file){
  const fd = new FormData();
  fd.append("file", file);
  const r = await fetch(BASE + path, { method:"POST", headers: { ...authHeaders() }, body: fd });
  if(!r.ok) throw new Error(await r.text());
  return r.json();
}

export function hasRole(roles, ...need){
  if(!roles) return false;
  if(roles.includes("ADMIN")) return true;
  return need.some(r => roles.includes(r));
}

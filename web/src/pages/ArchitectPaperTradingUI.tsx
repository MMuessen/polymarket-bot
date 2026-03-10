import React, { useEffect, useState } from "react";

export default function ArchitectPaperTradingUI() {

  const [data,setData]=useState<any>({})

  async function load(){
    try{
      const r=await fetch("/architect/data")
      const j=await r.json()
      setData(j)
    }catch(e){}
  }

  useEffect(()=>{
    load()
    const t=setInterval(load,2000)
    return ()=>clearInterval(t)
  },[])

  const quant=data.quant||{}
  const open=data.open_positions||[]
  const trades=data.recent_trades||[]

  return (
  <div style={{padding:40,fontFamily:"sans-serif",background:"#0b0e14",color:"#e6edf3",minHeight:"100vh"}}>

  <h1>Architect Trading Control Center</h1>

  <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:20,marginTop:30}}>

  <div style={{background:"#111826",padding:20,borderRadius:8}}>
  <h3>Execution Funnel</h3>
  <p>Candidate calls: {quant.candidate_calls}</p>
  <p>Allowed: {quant.candidate_allowed}</p>
  <p>Blocked: {quant.candidate_blocked}</p>
  <p>Enter attempts: {quant.enter_calls}</p>
  </div>

  <div style={{background:"#111826",padding:20,borderRadius:8}}>
  <h3>Paper Performance</h3>
  <p>Recent trades: {trades.length}</p>
  </div>

  <div style={{background:"#111826",padding:20,borderRadius:8}}>
  <h3>Variants</h3>
  <pre style={{fontSize:12}}>
{JSON.stringify(data.paper_guardrails_v41||{},null,2)}
  </pre>
  </div>

  </div>

  <div style={{marginTop:40}}>
  <h2>Open Positions</h2>

  <table style={{width:"100%",marginTop:10}}>
  <thead>
  <tr>
  <th>Ticker</th>
  <th>Variant</th>
  <th>Side</th>
  <th>Entry</th>
  </tr>
  </thead>
  <tbody>
  {open.map((p:any,i:number)=>(
  <tr key={i}>
  <td>{p.ticker}</td>
  <td>{p.variant_name}</td>
  <td>{p.side}</td>
  <td>{p.entry_price}</td>
  </tr>
  ))}
  </tbody>
  </table>
  </div>

  </div>
  )
}

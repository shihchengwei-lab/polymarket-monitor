"use strict";
const el = id => document.getElementById(id);
const put = (id, text) => { el(id).textContent = text; };
const none = "—";
const clock = ms => (ms / 1000).toFixed(3) + " s";
let mode = "live", connected = false, last = null, history = [], historyBytes = 0;
let frozen = [], paper = null, animation = 0, anchor = 0, readId = 0;
let fileError = "";
let pinned=null, sessionId=null, switching=false;

function row(parent, cells, tag = "td") {
  const tr = parent.insertRow();
  for (const value of cells) {
    const cell = document.createElement(tag);
    cell.textContent = value ?? none;
    if(tag==="td"){cell.dataset.label=el("cols").rows[0]?.cells[tr.cells.length]?.textContent??"";cell.title=cell.textContent;}
    tr.append(cell);
  }
}
function table(columns, rows, empty) {
  el("cols").replaceChildren(); el("rows").replaceChildren();
  row(el("cols"), columns, "th");
  for (const cells of rows) row(el("rows"), cells);
  put("empty", rows.length ? "" : empty);
}
function metrics(labels, values) {
  ["a","b","c"].forEach((key,i) => { put("l"+key, labels[i]); put(key, values[i] ?? none); });
}
function logs(lines) {
  el("log").replaceChildren(...(lines.length ? lines : ["尚無狀態轉換"]).map(text => {
    const item = document.createElement("li"); item.textContent = text; return item;
  }));
}
// Use integer units for quote arithmetic. Rust Decimal has at most 28 fraction digits.
const scale = 10n ** 28n;
function units(s) {
  if (!/^\d+(\.\d{1,28})?$/.test(s)) throw Error("無效的價格");
  const [i,f=""] = s.split(".");
  return BigInt(i)*scale + BigInt(f.padEnd(28,"0"));
}
function decimal(v) {
  const n = v < 0n ? -v : v;
  const fraction = (n % scale).toString().padStart(28,"0").replace(/0+$/,"");
  return (v<0n?"-":"")+(n/scale).toString()+(fraction?"."+fraction:"");
}
function market(screen) {
  const lines = screen.book.split("\n");
  const head = /^(.*) \| (Ready|NotReady) \((\d+\/\d+) 子市場\)$/.exec(lines[0]);
  const summary = lines.findIndex(s => s.startsWith("YES 中間價總和 "));
  const rows=[], states=new Map();
  let name="", askSum=0n, bidSum=0n, feeSum=0n, depthOk=true, bidsOk=true, feesOk=true, complete=!!head && head[2]==="Ready", yesCount=0;
  for (const line of lines.slice(summary+1)) {
    if (!line.startsWith("  ")) { name=line; continue; }
    const m = /^  (YES|NO) (Disconnected|Stale|Reconciling|Syncing|Synced)(?: \[.*?\])?(?: \| bid (.*?) \/ ask (.*?) \| tick (\S+))?$/.exec(line);
    if (!m) continue;
    rows.push([name,m[1],m[2],m[3],m[4],m[5]]);
    states.set(name+" · "+m[1], m[2]);
    if (m[1]==="YES") {
      yesCount++;
      const ask = m[4]?.split(" × ")[0];
      const bid=m[3]?.split(" × ")[0];
      if(bid && bid!==none) bidSum+=units(bid); else bidsOk=false;
      if (ask && ask!==none){
        const price=units(ask);askSum+=price;
        if(units(m[4].split(" × ")[1])<scale)depthOk=false;
        const rate=screen.desk?.rates?.[name];
        if(typeof rate==="string"){
          // Per one-share fill, nearest 0.00001 pUSD, half up.
          const denominator=scale*scale*(scale/100000n);
          feeSum+=(units(rate)*price*(scale-price)+denominator/2n)/denominator*(scale/100000n);
        }else feesOk=false;
      }else complete=false;
    }
  }
  complete &&= yesCount>0 && yesCount===Number(head[3].split("/")[1]) && !screen.stopped;
  return {title:head?.[1],status:head?.[2], synced:head?.[3], rows,states,
    cost:complete&&feesOk&&depthOk?askSum+feeSum:null,
    edge:complete?decimal(scale-askSum):none,
    net:complete&&feesOk?decimal(scale-askSum-feeSum):none,
    maker:head?.[2]==="Ready" && !screen.stopped && bidsOk && yesCount>0 && yesCount===Number(head[3].split("/")[1])?decimal(scale-bidSum):none,
    note:lines.slice(1,Math.max(1,summary)).join(" · ")};
}
function queueView(m, isCurrent) {
  const select=el("track");
  const quotes=m.rows.filter(r=>r[2]==="Synced" && r[3]?.includes(" × "));
  const selected=pinned?.key??"";
  const options=[new Option("選擇子市場與 Token","")];
  for(const [name,token] of quotes){
    const key=JSON.stringify([name,token]);
    options.push(new Option(name+" · "+token,key));
  }
  if(selected && !options.some(o=>o.value===selected))
    options.push(new Option(JSON.parse(selected).join(" · "),selected));
  select.replaceChildren(...options);select.value=selected;
  select.onchange=()=>{
    const quote=quotes.find(r=>JSON.stringify(r.slice(0,2))===select.value);
    pinned=quote?{key:select.value,level:quote[3].split(" × ")}:null;
    show();
  };
  if(!pinned){put("queueInfo","選取一腿，追蹤同一最佳買價的可見股數變化。");return;}
  const quote=quotes.find(r=>JSON.stringify(r.slice(0,2))===pinned.key);
  const level=quote?.[3].split(" × ");
  const [price,initial]=pinned.level;
  const details=isCurrent && level && units(level[0])===units(price)
    ? "目前 "+level[1]+" 股 · 變化 "+decimal(units(level[1])-units(initial))+" 股"
    : "此價位目前無可核對的買一報價";
  put("queueInfo","買價 "+price+" · 起始 "+initial+" 股 · "+details);
}
function showBook(frame, frames, index) {
  const screen=frame?.screen ?? {book:""};
  const m=market(screen);
  put("title",m.title || screen.slug || "行情觀測站");
  put("heading","最佳買賣盤");
  put("status",mode==="history"?"盤面回看":!connected?"本機服務離線":screen.stopped?"行情已停止":m.status || "等待行情");
  const active=mode==="history" || connected;
  el("queue").hidden=false;queueView(m,active && !screen.stopped);
  metrics(["Taker 毛價差 · 最佳賣價","Taker 淨價差 · 每腿 1 股扣費後","Maker 空間 · 1 − 最佳買價總和"],active?[m.edge,m.net,m.maker]:[none,none,none]);
  table(["子市場","Token","狀態","最佳買價 × 股數","最佳賣價 × 股數","跳動單位"],m.rows,"使用 cockpit 加上事件 slug 接收即時行情，或載入紙上回放。");
  put("notice",fileError || [!active?"目前顯示最後收到的畫面":"",m.note,screen.diagnostic].filter(Boolean).join(" · "));
  logs(frames.slice(0,index+1).slice(-120).flatMap(x=>x.transitions).slice(-12).reverse());
}
function paperFrame(t) {
  el("queue").hidden=true;
  const records=paper.data.round_records;
  let lo=0,hi=records.length;
  while(lo<hi){const mid=(lo+hi)>>>1;if(records[mid][0]<=t)lo=mid+1;else hi=mid;}
  const events=records.slice(0,lo), orders=new Map(), messages=[];
  for (const [ms,id,leg,op,v] of events) {
    let o=orders.get(id);
    if (op==="decision") {
      o={id,leg,ahead:v[0],bid:v[1],cost:v[2],state:"等待送達",filled:"0"};
      orders.set(id,o);
    }
    if (!o) continue;
    let detail="";
    if(op==="maker"){[o.state,o.filled]=v.maker;}
    if(op==="cancel"){[o.state,o.filled]=v;}
    if(op==="gate_reject"){o.state="閘門拒絕"; detail=v?.reason || "";}
    if(op==="ioc"){detail="補腿 "+leg+" · "+v.maker.join(" / ");}
    messages.push(clock(ms)+" · #"+id+" · "+(detail||o.state));
  }
  const list=[...orders.values()].reverse(), latest=list[0];
  put("title",paper.name);put("heading","紙上掛單與補腿");put("status","紙上回放");
  put("notice",fileError || "");
  metrics(["每腿股數","最近決策整籃成本（整筆）","最近決策價差（整筆）"],
    [paper.data.quantity,latest?.cost,latest?decimal(units(paper.data.quantity)-units(latest.cost)):none]);
  table(["掛單","腿索引","決策買價","入場前方股數","Maker 成交股數","狀態"],list.slice(0,100).map(o=>["#"+o.id,o.leg,o.bid,o.ahead,o.filled,o.state]),"這個時點尚無紙上掛單決策。");
  logs(messages.slice(-12).reverse());
}
function show() {
  for (const id of ["live","history","paper"]) el(id).setAttribute("aria-pressed",String(mode===id));
  el("timeline").hidden=mode==="live";
  el("controls").hidden=el("ledger").hidden=mode!=="live";
  el("bookPanel").classList.toggle("book",mode!=="paper");
  el("basketVisuals").hidden=mode!=="live";
  if(mode==="live"){showDesk();basketVisuals.draw(latestDesk,connected);}
  if(mode==="live"){showBook(last,history,history.length-1);return;}
  const t=Number(el("time").value);put("at",clock(t));
  if(mode==="paper"){paperFrame(t);return;}
  const index=frozen.findLastIndex(f=>f.ms<=t);
  showBook(frozen[index],frozen,index);
}
function stop(){cancelAnimationFrame(animation);animation=0;put("play","播放");}
function select(id) {
  stop(); mode=id; pinned=null;
  if(id==="history") frozen=history.map((f,i,a)=>({...f,ms:f.ms-a[0].ms}));
  if(id!=="live") {
    const r=el("time");r.min=0;r.max=id==="paper"?paper.duration:(frozen.at(-1)?.ms??0);
    r.value=id==="paper"?(paper.data.round_records[0]?.[0]??0):0;
  }
  show();
}
for(const id of ["live","history","paper"]) el(id).onclick=()=>select(id);
el("time").oninput=()=>{stop();show();};
el("play").onclick=()=>{
  if(animation){stop();return;}
  const r=el("time"); if(+r.value>=+r.max)r.value=0;
  anchor=performance.now()-Number(r.value);put("play","暫停");
  function tick(){
    r.value=Math.min(+r.max,Math.floor(performance.now()-anchor));
    show();if(+r.value>=+r.max){stop();return;}
    animation=requestAnimationFrame(tick);
  }
  tick();
};
el("file").onchange=async e=>{
  const file=e.target.files[0];if(!file)return;
  const id=++readId;
  try {
    if(file.size>4*1024*1024)throw Error("回放檔上限為 4 MiB");
    const data=JSON.parse(await file.text());
    if(id!==readId)return;
    units(data.quantity);
    let prev=0;
    const pair=v=>Array.isArray(v)&&typeof v[0]==="string"&&typeof v[1]==="string";
    if(!Number.isSafeInteger(data.last_ms)||data.last_ms<0||!Array.isArray(data.round_records)||data.round_records.length>10000)throw Error();
    for(const [ms,order,leg,op,v] of data.round_records){
      if(!Number.isSafeInteger(ms)||ms<prev||!Number.isSafeInteger(order)||!Number.isSafeInteger(leg))throw Error();
      prev=ms;
      if(op==="decision"){if(!Array.isArray(v))throw Error();for(const s of v.slice(0,3))units(s);units(v[2]);}
      else if(op==="maker"||op==="ioc"){if(!pair(v?.maker))throw Error();}
      else if(op==="cancel"){if(!pair(v))throw Error();}
      else if(op!=="gate_reject")throw Error();
    }
    fileError="";paper={name:file.name,data,duration:Math.max(data.last_ms,prev)};
    el("paper").disabled=false;select("paper");
  }catch{if(id===readId){fileError="無法讀取回放。請選擇 4 MiB 以內、符合紙上回放格式的 JSON 檔。";show();}}
  finally{e.target.value="";}
};
async function loadEvents(){
  let again=60000;
  try{
    const res=await fetch("/events",{signal:AbortSignal.timeout(4000)});
    if(!res.ok)throw Error();
    const data=await res.json(), list=el("popular");
    const prior=list.value;
    list.replaceChildren(new Option(data.loading?"載入清單中…":data.error?"熱門清單更新失敗，可輸入 slug":"選擇熱門事件",""));
    for(const item of data.events){
      const [whole,fraction]=item.volume.split(".");
      const volume=BigInt(whole).toLocaleString("en-US")+(fraction?"."+fraction:"");
      list.add(new Option(item.title+" · "+volume+" pUSD",item.slug));
    }
    list.value=prior;
    if(data.loading || data.error)again=data.loading?2000:15000;
  }catch{again=5000;el("popular").options[0].text="清單暫時無法取得，可輸入 slug";}
  setTimeout(loadEvents,again);
}
el("popular").onchange=e=>{if(e.target.value)el("eventSlug").value=e.target.value;};
el("eventForm").onsubmit=async e=>{
  e.preventDefault();
  if(switching || sessionId===null)return;
  const slug=el("eventSlug").value.trim();
  switching=true;el("switchEvent").disabled=true;put("eventMessage","正在確認事件…");
  try{
    const res=await fetch("/event/"+encodeURIComponent(slug),{method:"POST",
      headers:{"X-Paper-Control":"1","X-Cockpit-Session":String(sessionId)}});
    const text=await res.text();
    if(!res.ok)throw Error(text);
    put("eventMessage","已選擇 "+slug);
  }catch(err){put("eventMessage",err.message);}
  finally{switching=false;el("switchEvent").disabled=false;}
};
function receiveSession(screen){
  if(sessionId===screen.session)return;
  if(sessionId!==null){
    stop();mode="live";last=null;history=[];historyBytes=0;frozen=[];pinned=null;
    basketVisuals.reset();fileError="";put("answer","");
  }
  latestDesk=null;
  sessionId=screen.session;
  el("eventSlug").value=screen.slug;
}
async function poll(){
  try {
    const res=await fetch("/snapshot",{signal:AbortSignal.timeout(4000)});
    if(!res.ok)throw Error();
    const screen=await res.json();connected=true;receiveSession(screen);
    const ratesChanged=JSON.stringify(latestDesk?.rates)!==JSON.stringify(screen.desk?.rates);
    const lastCancel=latestDesk?.cancellations?.at(-1)?.order_id??0;
    const cancellations=(screen.desk?.cancellations??[]).filter(c=>c.order_id>lastCancel).map(c=>"#"+c.order_id+" · "+c.label);
    latestDesk=screen.desk;
    basketVisuals.sample(market(screen).cost);
    if(cancellations.length || ratesChanged || !last || screen.sequence!==last.screen.sequence || screen.book!==last.screen.book){
      // History only needs the fee schedule, not a copy of the account log.
      screen.desk={rates:screen.desk?.rates};
      const m=market(screen), prior=last?.states??new Map(), transitions=[];
      for(const [key,state] of m.states)if(prior.get(key)!==state)transitions.push(key+" · "+(prior.get(key)?prior.get(key)+" → ":"")+state);
      const ms=Math.floor(performance.now());last={ms,screen,states:m.states,transitions:transitions.concat(cancellations).slice(-32)};
      history.push(last);historyBytes+=screen.book.length;
      while(history.length>1 && (history.length>120 || historyBytes>2_000_000)) historyBytes-=history.shift().screen.book.length;
    }
  }catch{connected=false;basketVisuals.sample(null);}
  if(mode==="live")show();
  setTimeout(poll,500);
}
let latestDesk=null, sending=false;
function showDesk(){
  const d=latestDesk, enabled=connected && !!d?.ready && !d.error;
  el("fire").disabled=!enabled || !!d?.busy || sending;
  el("strategy").disabled=!connected || !d?.state || sending;
  el("lag").disabled=el("strategy").disabled;
  if(document.activeElement!==el("lag") && !sending){el("lag").value=d?.lag??150;put("ms",el("lag").value+" ms");}
  el("strategy").value=d?.auto?"on":"off";
  put("deskState",d?.error || (d?.state?d.state+(enabled?"":" · 等待有效行情與成交帶"):"等待紙上引擎"));
  put("cash",d?.cash??none);put("frozenCash",d?.frozen??none);put("pnl",d?.pnl??none);
  put("conservation",d?.conserved===true?"相符":d?.conserved===false?"異常":none);
  put("positions","持倉 · "+(d?.positions?.join("、")||"無"));
  el("actions").replaceChildren(...(d?.log??[]).slice(-8).reverse().map(text=>{const li=document.createElement("li");li.textContent=text;return li;}));
}
async function command(path){
  if(sending || switching || sessionId===null)return;
  const expected=sessionId;
  sending=true;showDesk();
  try{
    const r=await fetch("/desk/"+path,{method:"POST",headers:{"X-Paper-Control":"1","X-Cockpit-Session":String(expected)},signal:AbortSignal.timeout(4000)});
    const message=await r.text();if(sessionId===expected)put("answer",message);
  }catch{put("answer","未確認受理，請核對帳本");}
  finally{sending=false;showDesk();}
}
el("lag").oninput=e=>put("ms",e.target.value+" ms");
el("lag").onchange=e=>command("lag/"+e.target.value);
el("strategy").onchange=e=>command("auto/"+e.target.value);
el("fire").onclick=()=>command("fire");
show();poll();loadEvents();

"use strict";
const basketVisuals = (() => {
  const windowMs = 60_000;
  const samples = [];
  const tiles = new Map();
  let lastEvents = "";
  const symbols = {"掛單":"◆","部分成交":"●","成交":"●","撤單":"×","補腿":"▲"};
  const colors = {"掛單":"#9db6fc","部分成交":"#e9bb78","成交":"#e9bb78","撤單":"#f3989c","補腿":"#74d1d5"};

  function sample(cost) {
    const at = Date.now();
    if (samples.length && samples.at(-1).at > at) samples.length = 0;
    samples.push({at,cost});
    while (samples.length && samples[0].at < at-windowMs-2000) samples.shift();
  }

  function chart(desk, connected, now) {
    const canvas = el("costChart");
    const width = canvas.clientWidth, height = canvas.clientHeight;
    if (!width || !height) return;
    const ratio = window.devicePixelRatio || 1;
    if (canvas.width!==Math.round(width*ratio) || canvas.height!==Math.round(height*ratio)) {
      canvas.width=Math.round(width*ratio); canvas.height=Math.round(height*ratio);
    }
    const ctx=canvas.getContext("2d");
    ctx.setTransform(ratio,0,0,ratio,0,0);ctx.clearRect(0,0,width,height);
    const end=now, start=end-windowMs, left=6, right=width-76, top=16, bottom=height-40;
    const visible=samples.filter(s=>s.at>=start-2000);
    const current=connected && now-(visible.at(-1)?.at??0)<2000 ? visible.at(-1)?.cost : null;
    const alert=current!=null && current<scale;
    el("trend").classList.toggle("opportunity",alert);
    put("basketCost",current==null?"—":"$"+decimal(current));
    put("edgeState",current==null?"等待有效報價":alert?"低於 $1 · 正價差":"待機中");
    // Price comparisons stay fixed-point. Convert only bounded pixel ratios.
    let low=scale,high=scale;
    for(const s of visible) if(s.cost!=null){if(s.cost<low)low=s.cost;if(s.cost>high)high=s.cost;}
    const pad=(high-low)/6n+scale/10000n;
    low-=pad;high+=pad;
    const x=t=>left+(t-start)/windowMs*(right-left);
    const y=v=>bottom-Number((v-low)*100000n/(high-low))/100000*(bottom-top);
    const base=y(scale);
    const line=(x1,y1,x2,y2,color)=>{
      ctx.strokeStyle=color;ctx.beginPath();ctx.moveTo(x1,y1);ctx.lineTo(x2,y2);ctx.stroke();
    };
    ctx.lineWidth=1;
    for(let i=0;i<=4;i++){const gx=left+(right-left)*i/4;line(gx,top,gx,bottom,"#293539");}
    ctx.save();ctx.beginPath();ctx.rect(left,top,right-left,bottom-top+1);ctx.clip();
    ctx.lineWidth=2;
    for(let i=0;i<visible.length;i++){
      const a=visible[i],b=visible[i+1];
      if(a.cost==null)continue;
      const ax=x(a.at), ay=y(a.cost);
      const bx=x(Math.min(b?.at??end,a.at+1500,end));
      if(a.cost<scale){ctx.fillStyle="#19bc877a";ctx.fillRect(ax,base,bx-ax,Math.max(1,ay-base));}
      line(ax,ay,bx,ay,a.cost<scale?"#56f2af":"#9db3c6");
      if(b?.cost!=null && b.at-a.at<=1500){
        const by=y(b.cost);
        line(bx,ay,bx,by,"#9db3c6");
        if(a.cost<scale || b.cost<scale)line(bx,Math.max(base,ay),bx,Math.max(base,by),"#56f2af");
      }
    }
    ctx.restore();
    ctx.lineWidth=1.5;ctx.setLineDash([5,3]);line(left,base,right+3,base,"#ff737b");ctx.setLineDash([]);
    ctx.fillStyle="#ff979a";ctx.font="bold 13px Consolas, monospace";ctx.fillText("$1.0000",right+6,base+4);
    const events=(desk?.markers??[]).filter(e=>e.at>=start && e.at<=end);
    const lanes={"掛單":0,"部分成交":1,"成交":1,"撤單":2,"補腿":3};
    for(const e of events){
      const ex=x(e.at),ey=top+8+lanes[e.kind]*16;
      line(ex,ey+3,ex,bottom,"#74848044");
      ctx.font="bold 14px sans-serif";ctx.fillStyle=colors[e.kind];ctx.fillText(symbols[e.kind],ex-4,ey);
    }
    ctx.fillStyle="#98aaa6";ctx.font="12px sans-serif";ctx.fillText("−60 秒",left,height-10);ctx.fillText("現在",right-24,height-10);
    const key=JSON.stringify(events);
    if(key!==lastEvents){
      lastEvents=key;
      const items=events.slice().reverse().map(e=>{
        const li=document.createElement("li");
        li.textContent=new Date(e.at).toLocaleTimeString()+" · "+e.kind+" · "+(desk.slots?.[e.leg]?.name??"")+" · "+e.detail;
        return li;
      });
      if(!items.length){const li=document.createElement("li");li.textContent="此視窗尚無模擬事件";items.push(li);}
      el("chartEvents").replaceChildren(...items);
    }
  }

  function slots(desk,connected,now) {
    const holdings=desk?.slots??[];
    const amounts=holdings.map(s=>units(s.shares));
    const held=amounts.some(v=>v>0n), aligned=held && amounts.every(v=>v===amounts[0]);
    const status=!connected?"最後收到的持倉":!holdings.length?"等待帳本":!held?"尚無持倉":aligned?"已成套鎖定":desk.busy?"補腿對齊中":"未成套 · 有剩餘曝險";
    put("alignState",status);
    el("slots").dataset.state=connected && held?(aligned?"aligned":"partial"):"idle";
    const names=new Set(holdings.map(s=>s.name));
    for(const [name,tile] of tiles)if(!names.has(name)){tile.root.remove();tiles.delete(name);}
    holdings.forEach((s,i)=>{
      let tile=tiles.get(s.name);
      if(!tile){
        const root=document.createElement("article"),name=document.createElement("div"),shares=document.createElement("strong"),prints=document.createElement("div");
        name.className="slotName";name.textContent=name.title=s.name;prints.className="prints";
        root.append(name,shares,prints);el("slots").append(root);
        tile={root,shares,prints};tiles.set(s.name,tile);
      }
      tile.shares.textContent=s.shares+" 股";
      const recent=(desk.tape??[]).filter(e=>connected && e.leg===i && now-e.at<5000 && e.at<=now).slice(-3);
      const ids=new Set(recent.map(e=>String(e.id)));
      for(const badge of [...tile.prints.children])if(!ids.has(badge.dataset.id))badge.remove();
      for(const e of recent){
        if([...tile.prints.children].some(n=>n.dataset.id===String(e.id)))continue;
        const badge=document.createElement("span");badge.className="tradeBubble";badge.dataset.id=e.id;
        badge.textContent=e.token+" "+(e.side==="BUY"?"買":"賣")+" "+e.size+" 股 @ "+e.price;
        tile.prints.append(badge);
      }
    });
    const q=connected && desk?.queue;
    el("queueMeter").hidden=!q;
    if(q){
      const initial=units(q.initial),remaining=units(q.remaining);
      el("queueProgress").value=initial===0n?100:Number((initial-remaining)*10000n/initial)/100;
      put("queueLabel",(holdings[q.leg]?.name??"")+" · 前方剩餘 "+q.remaining+" / "+q.initial+" 股");
    }
  }
  function draw(desk,connected) {const now=Date.now();chart(desk,connected,now);slots(desk,connected,now);}
  function reset(){
    samples.length=0;
    for(const tile of tiles.values())tile.root.remove();
    tiles.clear();lastEvents="";
  }
  return {sample,draw,reset};
})();

const $=id=>document.getElementById(id), N=x=>Number(x)||0;
let market=null, polyWS=null, polyPing=null;
let signalState={stage:'WAIT',side:'WAIT',confidence:50,generatedAt:0,validUntil:0,potential:0,edge:0,reason:'Warming up'};
const historyEntries=[];
let storedEntries=[];
let calibration={bins:{},total:0,correct:0,brierSum:0};
let loggedEntryId='';
const binance={bid:0,ask:0,last:0,bidSize:0,askSize:0,ts:0,flow:0,bids:[],asks:[],depthTs:0};
const hist=[], trades=[], polyHist=[]; const books=new Map();
let selectedMode='precision', tradeMode='off', lastUpProb=.5, lastDownProb=.5, lastUpFlash=0, lastDownFlash=0;
let recentResults=[];
let sectionVisibility={binanceBody:true,edgeBody:true,splitBody:true};
const modeConfig={
 precision:{minConf:70,minEdge:.055,minMomentum:.055,minFlow:.050,minPoly:.020,maxVol:2.0,persist:900,cooldown:30000},
 balanced:{minConf:64,minEdge:.035,minMomentum:.040,minFlow:.035,minPoly:.015,maxVol:2.35,persist:700,cooldown:25000},
 momentum:{minConf:67,minEdge:.045,minMomentum:.065,minFlow:.030,minPoly:.010,maxVol:2.6,persist:650,cooldown:25000},
 liquidity:{minConf:68,minEdge:.045,minMomentum:.035,minFlow:.040,minPoly:.025,maxVol:2.25,persist:850,cooldown:30000},
 auto:{minConf:72,minEdge:.065,minMomentum:.060,minFlow:.055,minPoly:.025,maxVol:1.9,persist:1200,cooldown:45000},
 edge:{minConf:76,minEdge:.085,minMomentum:.070,minFlow:.060,minPoly:.035,maxVol:1.75,persist:1500,cooldown:60000}
};
const clamp=(x,a,b)=>Math.max(a,Math.min(b,x));
const pct=v=>`${(N(v)*100).toFixed(3)}%`, fmtPct=v=>`${N(v)>=0?'+':''}${(N(v)*100).toFixed(3)}%`, cents=v=>N(v)?`${(N(v)*100).toFixed(2)}¢`:'—';
function setMove(id,box,v){const e=$(id),b=$(box),n=N(v);e.textContent=pct(n);e.style.color=n>0?'#4be28f':n<0?'#ff8585':'';b.classList.remove('flashUp','flashDown');if(Math.abs(n)>0.000001){void b.offsetWidth;b.classList.add(n>0?'flashUp':'flashDown')}}
function openWS(url,onOpen,onMsg){let ws,delay=500;const go=()=>{try{ws=new WebSocket(url);ws.onopen=()=>{delay=500;onOpen?.(ws)};ws.onmessage=e=>{try{onMsg(JSON.parse(e.data))}catch{}};ws.onerror=()=>{};ws.onclose=()=>setTimeout(go,delay);delay=Math.min(10000,delay*2)}catch{setTimeout(go,delay);delay=Math.min(10000,delay*2)}};go();return()=>{try{ws?.close()}catch{}}}
function updateBinance(bid,ask,last,bidSize,askSize,ts=Date.now()){if(bid>0)binance.bid=bid;if(ask>0)binance.ask=ask;if(last>0){binance.last=last;hist.push({ts,p:last});while(hist.length>5000)hist.shift()}if(bidSize>=0)binance.bidSize=bidSize;if(askSize>=0)binance.askSize=askSize;binance.ts=ts}
function setDepth(bids,asks,ts=Date.now()){binance.bids=(bids||[]).map(x=>({p:N(x[0]),q:N(x[1])})).filter(x=>x.p>0&&x.q>0).sort((a,b)=>b.p-a.p).slice(0,20);binance.asks=(asks||[]).map(x=>({p:N(x[0]),q:N(x[1])})).filter(x=>x.p>0&&x.q>0).sort((a,b)=>a.p-b.p).slice(0,20);binance.depthTs=ts;if(binance.bids[0]&&binance.asks[0])updateBinance(binance.bids[0].p,binance.asks[0].p,0,binance.bids[0].q,binance.asks[0].q,ts)}
function startBinance(){openWS('wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@depth20@100ms/btcusdt@trade',null,m=>{const d=m.data||m;if(d.e==='bookTicker')updateBinance(N(d.b),N(d.a),0,N(d.B),N(d.A),N(d.E));else if(d.e==='depthUpdate')setDepth(d.b,d.a,N(d.E)||Date.now());else if(d.e==='trade'){const q=N(d.q);binance.flow+=((d.m?-1:1)*q);trades.push({ts:N(d.T)||Date.now(),flow:(d.m?-1:1)*q});while(trades.length>5000)trades.shift();updateBinance(0,0,N(d.p),-1,-1,N(d.T)||Date.now())}})}
function parseArr(v){if(Array.isArray(v))return v;if(typeof v==='string'){try{const x=JSON.parse(v);return Array.isArray(x)?x:[]}catch{}}return[]}
function tokenPair(m){const ids=parseArr(m.clobTokenIds??m.clob_token_ids),outs=parseArr(m.outcomes);let up=null,down=null;outs.forEach((o,i)=>{const z=String(o).toLowerCase().trim();if(/^up(?:\b|\s)/.test(z)&&ids[i])up=String(ids[i]);if(/^down(?:\b|\s)/.test(z)&&ids[i])down=String(ids[i])});if((!up||!down)&&ids.length===2){up=up||String(ids[0]||'');down=down||String(ids[1]||'')}return up&&down?{up,down}:null}
async function discoverDirect(){const now=Math.floor(Date.now()/1000),baseTs=now-now%300,candidates=[baseTs,baseTs-300,baseTs+300];for(const ts of candidates){const slug=`btc-updown-5m-${ts}`;for(const path of [`https://gamma-api.polymarket.com/events?slug=${encodeURIComponent(slug)}`,`https://gamma-api.polymarket.com/markets?slug=${encodeURIComponent(slug)}`]){try{const r=await fetch(path,{cache:'no-store'});if(!r.ok)continue;const j=await r.json();const arr=Array.isArray(j)?j:(j.data||[]),events=path.includes('/events')?arr:arr.map(x=>({markets:[x]}));for(const ev of events)for(const m of ev.markets||[]){const t=tokenPair(m),end=m.endDate||m.endDateIso||ev.endDate,endTs=Math.floor(new Date(end).getTime()/1000);if(t&&Number.isFinite(endTs)&&endTs>now&&m.closed!==true&&String(m.active)!=='false'){market={slug:m.slug||ev.slug||slug,question:m.question||ev.title||slug,upToken:t.up,downToken:t.down,endTs};connectPoly([t.up,t.down]);return}}}catch{}}}throw Error('BTC 5M market discovery failed')}
function setBook(id,bids,asks){books.set(String(id),{bids:(bids||[]).map(x=>({price:N(x.price),size:N(x.size)})).filter(x=>x.price>0&&x.size>0).sort((a,b)=>b.price-a.price),asks:(asks||[]).map(x=>({price:N(x.price),size:N(x.size)})).filter(x=>x.price>0&&x.size>0).sort((a,b)=>a.price-b.price),ts:Date.now()})}
function applyPolyDelta(x,root){const id=String(x.asset_id||x.assetId||root||'');if(!id)return;const q=books.get(id)||{bids:[],asks:[],ts:0},side=String(x.side||'').toUpperCase()==='BUY'?q.bids:q.asks,p=N(x.price),s=N(x.size);if(p<=0)return;const i=side.findIndex(z=>z.price===p);if(s<=0){if(i>=0)side.splice(i,1)}else if(i>=0)side[i].size=s;else side.push({price:p,size:s});q.bids.sort((a,b)=>b.price-a.price);q.asks.sort((a,b)=>a.price-b.price);q.ts=Date.now();books.set(id,q)}
function connectPoly(ids){try{polyWS?.close()}catch{}clearInterval(polyPing);polyWS=new WebSocket('wss://ws-subscriptions-clob.polymarket.com/ws/market');polyWS.onopen=()=>{polyWS.send(JSON.stringify({assets_ids:ids,type:'market',custom_feature_enabled:true,initial_dump:true}));polyPing=setInterval(()=>{try{polyWS.send('PING')}catch{}},10000)};polyWS.onmessage=e=>{try{const raw=JSON.parse(e.data),msgs=Array.isArray(raw)?raw:[raw];for(const m of msgs){const ev=String(m.event_type||'').toLowerCase(),root=String(m.asset_id||'');if(ev==='book'){setBook(root,m.bids,m.asks);continue}if(ev==='price_change'){for(const x of m.price_changes||[])applyPolyDelta(x,x.asset_id||root);continue}if(ev==='best_bid_ask'){const q=books.get(root)||{bids:[],asks:[],ts:0},bb=N(m.best_bid),aa=N(m.best_ask);if(bb>0)q.bids=[{price:bb,size:N(m.best_bid_size)||q.bids[0]?.size||0}];if(aa>0)q.asks=[{price:aa,size:N(m.best_ask_size)||q.asks[0]?.size||0}];q.ts=Date.now();books.set(root,q)}}}catch{}};polyWS.onclose=()=>{clearInterval(polyPing);setTimeout(()=>{if(market)connectPoly([market.upToken,market.downToken])},1000)};polyWS.onerror=()=>{try{polyWS.close()}catch{}}}
async function restPoly(token){try{const r=await fetch(`https://clob.polymarket.com/book?token_id=${encodeURIComponent(token)}`,{cache:'no-store'});if(r.ok){const x=await r.json();setBook(token,x.bids,x.asks)}}catch{}}
function sample(ms){const now=Date.now();for(let i=hist.length-1;i>=0;i--)if(now-hist[i].ts>=ms)return hist[i].p;return null}
function windowReturn(ms){const now=Date.now(),p=binance.last||(binance.bid+binance.ask)/2;if(!p)return 0;const q=sample(ms);return q?p/q-1:0}
function regressionSlope(points){if(points.length<5)return 0;const n=points.length,mx=(n-1)/2,my=points.reduce((a,x)=>a+x,0)/n;let num=0,den=0;for(let i=0;i<n;i++){num+=(i-mx)*(points[i]-my);den+=(i-mx)*(i-mx)}return den?num/den:0}
function depthLiquidity(){
 const p=binance.last||(binance.bid+binance.ask)/2;if(!p||!binance.bids.length||!binance.asks.length)return{imb:0,support:0,resistance:0,zone:'UNKNOWN',nearest:0};
 const band=p*.0008;let bidQ=0,askQ=0,support=0,resistance=0;
 for(const z of binance.bids){if(Math.abs(z.p-p)<=band){bidQ+=z.q;support=Math.max(support,z.q*(1-Math.abs(z.p-p)/band))}}
 for(const z of binance.asks){if(Math.abs(z.p-p)<=band){askQ+=z.q;resistance=Math.max(resistance,z.q*(1-Math.abs(z.p-p)/band))}}
 const imb=(bidQ+askQ)?(bidQ-askQ)/(bidQ+askQ):0;const zone=imb>.18?'SUPPORT':imb<-.18?'RESISTANCE':'BALANCED';return{imb,support,resistance,zone,nearest:Math.min(Math.abs((binance.bids[0]?.p||p)-p),Math.abs((binance.asks[0]?.p||p)-p))/p};
}
function bookStats(b){
 const bid=b?.bids?.[0],ask=b?.asks?.[0];
 const bidQ=(b?.bids||[]).slice(0,5).reduce((a,z)=>a+z.size,0),askQ=(b?.asks||[]).slice(0,5).reduce((a,z)=>a+z.size,0);
 const imb=(bidQ+askQ)?(bidQ-askQ)/(bidQ+askQ):0;
 const micro=bid&&ask&&(bid.size+ask.size)>0?((ask.price*bid.size)+(bid.price*ask.size))/(bid.size+ask.size):(bid?.price||ask?.price||0);
 const spread=bid&&ask?ask.price-bid.price:0;
 return {bid:bid?.price||0,ask:ask?.price||0,bidSize:bid?.size||0,askSize:ask?.size||0,bidQ,askQ,imb,micro,spread};
}
function polyBookPressure(upB,downB){
 const u=bookStats(upB),d=bookStats(downB);
 // UP bid pressure is bullish; DOWN bid pressure is bearish.
 return clamp((u.imb-d.imb)*.5,-1,1);
}
function calculate(){
 const now=Date.now(),p=binance.last||(binance.bid+binance.ask)/2;
 if(!p||now-binance.ts>2500)return{status:'warming',signal:'WAIT',confidence:50,upProbability:.5,downProbability:.5,potential:0,components:{}};
 const p1=sample(1000),p3=sample(3000),p5=sample(5000);
 const r1=p1?p/p1-1:0,r3=p3?p/p3-1:0,r5=p5?p/p5-1:0;
 const depth=depthLiquidity(),totalBook=binance.bidSize+binance.askSize,bookImb=totalBook?(binance.bidSize-binance.askSize)/totalBook:0;
 const nowTrades=trades.filter(t=>now-t.ts<=5000),recentFlow=nowTrades.reduce((a,t)=>a+t.flow,0);
 const flowNorm=clamp(recentFlow/Math.max(1,totalBook*2.5),-1,1);
 const upB=market?books.get(market.upToken):null,downB=market?books.get(market.downToken):null;
 const U=bookStats(upB),D=bookStats(downB);
 const upMid=U.bid&&U.ask?(U.bid+U.ask)/2:U.ask||U.bid||0,downMid=D.bid&&D.ask?(D.bid+D.ask)/2:D.bid||D.ask||0;
 const marketImplied=(upMid&&downMid)?clamp((upMid+(1-downMid))/2,.01,.99):(upMid||.5);
 const polyPressure=(upMid&&downMid)?clamp((upMid-(1-downMid))*.9,-.35,.35):0;
 const polyBook=polyBookPressure(upB,downB);
 const polyBidPressure=clamp((U.imb-D.imb)*.55,-1,1);
 const polyMicroProb=U.micro&&D.micro?clamp((U.micro+(1-D.micro))/2,.01,.99):marketImplied;
 const polyMicroDrift=clamp(polyMicroProb-marketImplied,-.25,.25);
 const polySpread=(U.spread&&D.spread)?(U.spread+D.spread)/2:.05;
 const polyLiquidity=clamp((U.bidQ+U.askQ+D.bidQ+D.askQ)/100,0,1);
 polyHist.push({ts:now,p:polyMicroProb});while(polyHist.length>2000)polyHist.shift();
 const polyPrev=polyHist.find(z=>now-z.ts>=1000);const polyR1=polyPrev?(polyMicroProb-polyPrev.p):0;
 const recentPrices=hist.filter(z=>now-z.ts<=5000).map(z=>z.p),slope=regressionSlope(recentPrices),slopeNorm=clamp(slope/Math.max(.01,p*.00002),-1,1);
 const prices1m=hist.filter(z=>now-z.ts<=60000).map(z=>z.p),ema=(arr,alpha)=>{if(!arr.length)return p;let e=arr[0];for(const v of arr.slice(1))e=alpha*v+(1-alpha)*e;return e};
 const emaFast=ema(prices1m,.22),emaSlow=ema(prices1m,.055),trendNorm=clamp((emaFast-emaSlow)/Math.max(.01,p*.0007),-1,1);
 const deltas=prices1m.slice(1).map((v,i)=>v/prices1m[i]-1).filter(Number.isFinite);let gains=0,losses=0;for(const d of deltas.slice(-30)){if(d>0)gains+=d;else losses-=d}const rsi=50+(gains+losses?50*(gains-losses)/(gains+losses):0),rsiNorm=clamp((rsi-50)/20,-1,1);
 const rets=hist.slice(-240).map((z,i,a)=>i?z.p/a[i-1].p-1:0).filter(Boolean),volatility=Math.sqrt(rets.reduce((a,b)=>a+b*b,0)/Math.max(1,rets.length)),volRegime=clamp(volatility/.00035,0,3);
 const volPenalty=volRegime>1.8?clamp((volRegime-1.8)*.08,0,.18):0;
 const momentum=clamp(r1*1150+r3*380+r5*170+slopeNorm*.08,-.5,.5);
 const orderFlow=clamp(bookImb*.16+flowNorm*.18+depth.imb*.08,-.38,.38);
 const trend=clamp(trendNorm*.09+rsiNorm*.045,-.14,.14);
 const poly=clamp(polyPressure*.16+polyBook*.08+polyBidPressure*.07+polyR1*1.8+polyMicroDrift*.20,-.16,.16);
 const liquidityZone=depth.zone, liquidityBias=clamp(depth.imb*.10,-.10,.10);
 const technicalScore=clamp(momentum+orderFlow+trend+liquidityBias,-.85,.85);
 const technicalProb=1/(1+Math.exp(-technicalScore*4.6));
 const marketDriftProb=clamp(.5+polyR1*2.8+polyMicroDrift*.8,-.15+.5,.85);
 const bidAskProb=clamp(.5+polyBidPressure*.16+polyBook*.10,-.01,.99);
 let blended=technicalProb*.48+marketImplied*.16+marketDriftProb*.10+bidAskProb*.11+(technicalProb+poly*.8)*.15;
 blended=.5+(blended-.5)*(1-volPenalty-clamp(polySpread/.08,0,1)*.05);
 const rawUpProbability=clamp(blended,.01,.99),up=calibratedProbability(rawUpProbability),down=1-up;
 const confidence=50+Math.min(49,Math.abs(up-.5)*190);const signal=up>=.5?'UP':'DOWN';
 const upEdge=U.ask?up-U.ask:0,downEdge=D.ask?down-D.ask:0,upBidEdge=U.bid?up-U.bid:0,downBidEdge=D.bid?down-D.bid:0;
 const edge=Math.max(upEdge,downEdge);
 const bestBidExit=Math.max(upBidEdge,downBidEdge);
 const potential=clamp((up-.5)*1.8,-.85,.85);
 const alignment=(Math.sign(momentum)===Math.sign(orderFlow)?1:0)+(Math.sign(momentum)===Math.sign(trend)?1:0)+(Math.sign(momentum)===Math.sign(poly)?1:0)+(Math.sign(momentum)===Math.sign(depth.imb)?1:0);
 const setupScore=(Math.abs(technicalScore)*100)+(Math.max(0,edge)*220)+(Math.abs(polyR1)*100)+(Math.abs(depth.imb)*8)+Math.abs(polyBidPressure)*12;
 return{price:p,return1s:r1,return3s:r3,return5s:r5,upProbability:up,downProbability:down,rawUpProbability,rawDownProbability:1-rawUpProbability,marketImplied,polyR1,signal,confidence,score:technicalScore*100,volatility,volRegime,components:{momentum,orderFlow,polymarketPressure:poly,trend,slopeNorm,bookImb,flowNorm,polyMid:marketImplied,technicalProb,marketImplied,polySpread,polyLiquidity,emaFast,emaSlow,rsi,polyBook,polyBidPressure,polyMicroProb,polyMicroDrift,liquidityZone,liquidityBias,depthSupport:depth.support,depthResistance:depth.resistance,alignment},upBook:upB,downBook:downB,upStats:U,downStats:D,potential,upEdge,downEdge,upBidEdge,downBidEdge,bestBidExit,edge,setupScore};
}

let candidateSince=0,lastCandidateKey='';
function stableSignal(x){
 const now=Date.now(),secs=market?market.endTs-now/1000:0,cfg=modeConfig[selectedMode]||modeConfig.precision,mode=selectedMode==='auto'?modeConfig.auto:cfg;
 const side=x.upProbability>=x.downProbability?'UP':'DOWN',ask=side==='UP'?x.upStats?.ask:side==='DOWN'?x.downStats?.ask:0,edge=side==='UP'?x.upEdge:x.downEdge;
 const m=Math.abs(x.components?.momentum||0),f=Math.abs(x.components?.orderFlow||0),pp=Math.abs(x.components?.polymarketPressure||0),a=x.components?.alignment||0;
 let stage='WATCH',reason='Waiting for aligned factors';const key=`${side}|${market?.slug||''}`;
 const directional=side==='UP'?1:-1;const factors=[x.components?.momentum||0,x.components?.orderFlow||0,x.components?.trend||0,x.components?.polymarketPressure||0,x.components?.bookImb||0,x.components?.polyBidPressure||0].map(v=>Math.sign(v)===directional);
 const agree=factors.filter(Boolean).length;
 const bidConfirm=side==='UP'?x.upStats?.bid>x.downStats?.ask*.95:x.downStats?.bid>x.upStats?.ask*.95;
 const quality=agree>=5&&a>=3&&edge>=mode.minEdge&&x.confidence>=mode.minConf&&m>=mode.minMomentum&&f>=mode.minFlow&&pp>=mode.minPoly&&x.volRegime<mode.maxVol&&Math.abs(x.polyR1||0)<.025&&bidConfirm;
 const setup=agree>=4&&a>=2&&edge>=mode.minEdge*.55&&x.confidence>=mode.minConf-7&&m>=mode.minMomentum*.6&&x.volRegime<mode.maxVol+.35;
 if(x.status==='warming')stage='WAIT',reason='Waiting for fresh Binance data';
 else if(secs<=60)stage='WAIT',reason='Last minute protection — no new sniper entry';
 else if(now-binance.ts>1200)stage='WAIT',reason='Binance feed stale';
 else if(!ask)stage='WATCH',reason='Polymarket executable ask unavailable';
 else if(quality){if(lastCandidateKey!==key){candidateSince=now;lastCandidateKey=key}if(now-candidateSince>=mode.persist)stage='ENTRY',reason=`${selectedMode.toUpperCase()} mode: ${agree}/6 factors + bid confirmation`;else stage='SETUP',reason=`Confirming setup ${Math.max(0,mode.persist-(now-candidateSince))}ms`;}
 else if(setup)stage='SETUP',reason=`Setup forming: ${agree}/6 factors aligned`;
 else stage='WATCH',reason=`Filtered: ${agree}/6 factors aligned`;
 const marketKey=market?.slug||String(market?.endTs||''),already=storedEntries.some(e=>e.stage==='ENTRY'&&e.marketKey===marketKey),lastEntry=storedEntries.filter(e=>e.stage==='ENTRY').reduce((a,e)=>Math.max(a,e.generatedAt||0),0);
 if(stage==='ENTRY'&&(already||now-lastEntry<mode.cooldown)){stage='SETUP';reason=already?'One entry already logged for this 5M market':'Entry cooldown active'}
 const valid=stage==='ENTRY'?Math.min(now+8000,(market?.endTs||now/1000)*1000-65000):now+2500;
 const lockedAsk=stage==='ENTRY'?ask:(signalState.lockedAsk||0);const newSignal={stage,side:stage==='WAIT'?'WAIT':side,confidence:x.confidence||50,generatedAt:now,validUntil:valid,potential:x.potential||0,edge,reason,lockedAsk,mode:selectedMode,liquidityZone:x.components?.liquidityZone||'UNKNOWN'};
 const changed=stage!==signalState.stage||newSignal.side!==signalState.side||newSignal.lockedAsk!==signalState.lockedAsk;
 if(changed||now-signalState.generatedAt>4000){signalState=newSignal;if(stage==='ENTRY'){const h={...newSignal,marketEnd:market?.endTs*1000||0,ask,upProb:x.upProbability,downProb:x.downProbability,marketKey,slug:market?.slug||''};if(entryKey(h)!==loggedEntryId){loggedEntryId=entryKey(h);persistEntry(h)}}}
 if(now>signalState.validUntil&&signalState.stage!=='WAIT')signalState={...signalState,stage:'WATCH',validUntil:now+2500,lockedAsk:0};return signalState;
}

function fmtTime(ms){return ms?new Date(ms).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'}):'—'}
function expiryText(){if(!market)return'EXP —';const s=Math.max(0,Math.ceil(market.endTs-Date.now()/1000));return `5M EXP ${s}s • ${fmtTime(market.endTs*1000)}`}
function render(){
 const x=calculate(),s=stableSignal(x),upB=x.upBook,downB=x.downBook,ua=x.upStats,da=x.downStats,now=Date.now(),secs=market?Math.max(0,Math.ceil(market.endTs-now/1000)):0;
 if(market){$('marketTitle').textContent=market.question||market.slug;$('openMarket').href=`https://polymarket.com/event/${encodeURIComponent(market.slug||'')}`;$('timeLeft').textContent=secs+'s';$('expiryTime').textContent=fmtTime(market.endTs*1000);$('expiryTime').classList.toggle('expiry-last',secs<=60);document.querySelectorAll('.expiryBadge').forEach(e=>{e.textContent=expiryText();e.classList.toggle('expiry-last',secs<=60)});$('timeLeft').parentElement.classList.toggle('expiry-last',secs<=60);$('expiryEntry2').classList.toggle('expiry-last',secs<=60);$('entryTimeframe').classList.toggle('expiry-last',secs<=60)}
 $('spotPrice').textContent=(binance.bid&&binance.ask)?((binance.bid+binance.ask)/2).toFixed(2):(x.price?x.price.toFixed(2):'—');$('detailBid').textContent=binance.bid?binance.bid.toFixed(2):'—';$('detailAsk').textContent=binance.ask?binance.ask.toFixed(2):'—';$('detailDepth').textContent=`${binance.bids.length}/${binance.asks.length}`;$('detailFeed').textContent=binance.ts?Math.max(0,now-binance.ts)+'ms':'—';$('lastPrice').textContent=binance.last?binance.last.toFixed(2):'—';$('spotSignal').textContent=x.signal||'WAIT';$('binanceBid').textContent=binance.bid?binance.bid.toFixed(2):'—';$('binanceAsk').textContent=binance.ask?binance.ask.toFixed(2):'—';$('binanceFeed').textContent=binance.ts?`${Math.max(0,now-binance.ts)}ms`:'—';
 $('upAsk').textContent=ua?.ask?cents(ua.ask):'—';$('downAsk').textContent=da?.ask?cents(da.ask):'—';$('upBid').textContent=ua?.bid?cents(ua.bid):'—';$('downBid').textContent=da?.bid?cents(da.bid):'—';$('upSize').textContent=ua?.askSize?`${ua.askSize.toFixed(2)} ask shares`:'—';$('downSize').textContent=da?.askSize?`${da.askSize.toFixed(2)} ask shares`:'—';
 const ages=[upB?.ts,downB?.ts].filter(Boolean);$('bookAge').textContent=ages.length?Math.max(0,now-Math.max(...ages))+'ms':'—';$('feedState').textContent=(upB&&downB)?'LIVE':'WAITING';
 $('upProb').textContent=(x.upProbability*100).toFixed(1)+'%';$('downProb').textContent=(x.downProbability*100).toFixed(1)+'%';$('probBarUp').style.width=(x.upProbability*100).toFixed(1)+'%';$('ret1s').textContent=fmtPct(x.return1s);$('ret3s').textContent=fmtPct(x.return3s);$('ret5s').textContent=fmtPct(x.return5s);$('factorMomentum').textContent=fmtPct(x.components?.momentum);$('factorFlow').textContent=fmtPct(x.components?.orderFlow);$('factorPoly').textContent=fmtPct(x.components?.polymarketPressure);$('factorRsi').textContent=x.components?.rsi!=null?x.components.rsi.toFixed(1):'—';$('modelScore').textContent=x.score!=null?x.score.toFixed(1):'—';$('volatility').textContent=x.volatility!=null?(x.volatility*100).toFixed(3)+'%':'—';$('rsiValue').textContent=x.components?.rsi!=null?x.components.rsi.toFixed(1):'—';$('depthZone').textContent=x.components?.liquidityZone||'—';$('alignment').textContent=(x.components?.alignment||0)+'/4';$('tradeCount').textContent=String(trades.length);$('bookSpread').textContent=((ua?.spread||0)+(da?.spread||0))?cents(((ua?.spread||0)+(da?.spread||0))/2):'—';$('bookLiquidity').textContent=x.components?.polyLiquidity!=null?(x.components.polyLiquidity*100).toFixed(0)+'%':'—';$('binanceDot').className=binance.ts&&now-binance.ts<2500?'live':'warn';$('polyDot').className=(upB&&downB)?'live':'warn';drawBTCChart();$('upModelHint').textContent=`${x.confidence.toFixed(1)}% confidence • ${x.components?.alignment||0}/4 agreement`;$('downModelHint').textContent=`${x.confidence.toFixed(1)}% confidence • ${x.components?.alignment||0}/4 agreement`;
 const upDelta=x.upProbability-lastUpProb,downDelta=x.downProbability-lastDownProb;
 if(Math.abs(upDelta)>=.008&&now-lastUpFlash>120){const card=$('upProbCard');card.classList.remove('probFlashUp');void card.offsetWidth;card.classList.add('probFlashUp');lastUpFlash=now}
 if(Math.abs(downDelta)>=.008&&now-lastDownFlash>120){const card=$('downProbCard');card.classList.remove('probFlashDown');void card.offsetWidth;card.classList.add('probFlashDown');lastDownFlash=now}
 lastUpProb=x.upProbability;lastDownProb=x.downProbability;
 const c=x.components||{},fmt=v=>v==null?'—':(N(v)>=0?'+':'')+(N(v)*100).toFixed(2)+'%';
 const liveAsk=s.side==='UP'?ua?.ask:da?.ask;
 $('sniperBox').className='sniper '+s.stage.toLowerCase();$('sniperText').textContent=`${s.stage==='ENTRY'?'⚡':s.stage==='SETUP'?'🎯':'👀'} ${s.stage}`;$('sniperSub').textContent=`5M • expires ${fmtTime(market?.endTs*1000)} • signal ${fmtTime(s.generatedAt)}`;if($('entryQuality'))$('entryQuality').textContent=s.stage==='ENTRY'?'HIGH-CONVICTION':s.stage==='SETUP'?'FORMING':'FILTERED';
 $('entryStage').textContent=s.stage;$('entrySide').textContent=s.side;$('entrySide').className=s.side==='UP'?'upText':s.side==='DOWN'?'downText':'';$('liveEntryAsk').textContent=liveAsk?cents(liveAsk):'—';
 $('signalTime').textContent=s.generatedAt?fmtTime(s.generatedAt):'—';$('venueDetail').textContent=`Binance BTC/USDT • ${binance.ts?Math.max(0,now-binance.ts):'—'}ms feed age • ${trades.length} buffered trades • ${binance.bids.length}/${binance.asks.length} depth levels`;$('status').textContent=binance.ts&&now-binance.ts<2500?`LIVE • Binance Spot + Polymarket • ${fmtTime(now)}`:'WAITING FOR BINANCE LIVE FEED';
 // Directional edge / hypothetical position guidance
 const buyUp=x.upEdge,buyDown=x.downEdge,sellUp=x.upBidEdge,sellDown=x.downBidEdge;
 let bestSide=buyUp>=buyDown?'UP':'DOWN',bestBuy=Math.max(buyUp,buyDown),bestSell=Math.max(sellUp,sellDown);
 $('oneDirSignal').textContent=bestBuy>.025?`BUY ${bestSide}`:'WAIT';$('oneDirSignal').className=bestBuy>.025?(bestSide==='UP'?'upText':'downText'):'';$('oneDirDetail').textContent=bestBuy>.025?`Model ${((bestSide==='UP'?x.upProbability:x.downProbability)*100).toFixed(1)}% vs ask ${cents(bestSide==='UP'?ua?.ask:da?.ask)} • edge ${fmt(bestBuy)}`:'No sufficient model-vs-ask edge';
 const exitSide=bestSell===sellUp?'UP':'DOWN';const exitEdge=bestSell;let action='HOLD';if(exitEdge>.03)action=`SELL ${exitSide}`;else if(bestBuy>.04)action=`HOLD / ADD ${bestSide}`;$('positionAction').textContent=action;$('positionAction').className=action.includes('SELL')?(exitSide==='UP'?'upText':'downText'):'';$('positionDetail').textContent=`Best bid edge ${fmt(exitEdge)} • current model ${bestSide==='UP'?(x.upProbability*100).toFixed(1):(x.downProbability*100).toFixed(1)}%`;
 $('bestEdge').textContent=fmt(Math.max(bestBuy,bestSell));$('bestEdgeDetail').textContent=`BUY ${fmt(bestBuy)} • SELL/EXIT ${fmt(bestSell)}`;$('bidEdge').textContent=fmt(bestSell);$('bidEdgeDetail').textContent=`UP bid ${cents(ua?.bid)} • DOWN bid ${cents(da?.bid)}`;
 // Split mode: allocation is a model suggestion, never an execution instruction
 const splitTotal=Math.max(x.upProbability+x.downProbability,0.0001),upAlloc=clamp(x.upProbability/splitTotal*100,0,100),downAlloc=100-upAlloc;
 $('upAlloc').textContent=tradeMode==='split'?upAlloc.toFixed(0)+'%':'—';$('downAlloc').textContent=tradeMode==='split'?downAlloc.toFixed(0)+'%':'—';
 let splitDecision='WAIT',holdSell='—';if(tradeMode==='split'){const spreadCost=(ua?.ask||0)+(da?.ask||0);if(spreadCost<.995)splitDecision='LOCKED-COST EDGE';else if(bestBuy>.035&&x.volRegime<2.2)splitDecision=`LEAN ${bestSide}`;else splitDecision='HOLD / WAIT';holdSell=bestSell>.03?`SELL ${exitSide}`:bestBuy>.035?'HOLD':'REDUCE RISK'}else if(tradeMode==='oneway'){splitDecision=bestBuy>.025?`BUY ${bestSide}`:'WAIT';holdSell=bestSell>.03?`SELL ${exitSide}`:'HOLD'}$('splitDecision').textContent=splitDecision;$('holdSell').textContent=holdSell;
 if($('calAccuracy'))$('calAccuracy').textContent=calibration.total?((calibration.correct/calibration.total)*100).toFixed(1)+'%':'—';if($('calBrier'))$('calBrier').textContent=brier()!=null?brier().toFixed(4):'—';if($('calSamples'))$('calSamples').textContent=String(calibration.total||0);
 const resolved=storedEntries.filter(e=>e.result==='UP'||e.result==='DOWN'),wins=resolved.filter(e=>e.correct===true).length,losses=resolved.filter(e=>e.correct===false).length,totalResolved=wins+losses,upPred=resolved.filter(e=>e.side==='UP'),downPred=resolved.filter(e=>e.side==='DOWN'),upCorrect=upPred.filter(e=>e.result==='UP').length,downCorrect=downPred.filter(e=>e.result==='DOWN').length,probPredCorrect=resolved.filter(e=>((e.upProb||0)>=(e.downProb||0)?'UP':'DOWN')===e.result).length;
 if($('sniperWins'))$('sniperWins').textContent=String(wins);if($('sniperLosses'))$('sniperLosses').textContent=String(losses);if($('sniperWinRate'))$('sniperWinRate').textContent=totalResolved?((wins/totalResolved)*100).toFixed(1)+'%':'—';if($('probAccuracy'))$('probAccuracy').textContent=totalResolved?((probPredCorrect/totalResolved)*100).toFixed(1)+'%':'—';if($('upProbAccuracy'))$('upProbAccuracy').textContent=upPred.length?`${upCorrect}/${upPred.length} • ${(upCorrect/upPred.length*100).toFixed(1)}%`:'—';if($('downProbAccuracy'))$('downProbAccuracy').textContent=downPred.length?`${downCorrect}/${downPred.length} • ${(downCorrect/downPred.length*100).toFixed(1)}%`:'—';
 const log=$('entryHistory');log.innerHTML=historyEntries.length?historyEntries.map(h=>`<div class="historyRow"><b>${h.stage} ${h.side}</b><span>${fmtTime(h.generatedAt)} → ${fmtTime(h.marketEnd)}</span><span>${h.result?`RESULT ${h.result} • ${h.correct?'✓ HIT':'✕ MISS'}`:'PENDING'}</span></div>`).join(''):'<div class="historyEmpty">No confirmed sniper entries yet.</div>';
 renderRecentResults();
}

function drawBTCChart(){
 const c=$('btcChart'); if(!c)return; const dpr=window.devicePixelRatio||1,rect=c.getBoundingClientRect(),w=Math.max(300,rect.width),h=150;
 if(c.width!==Math.round(w*dpr)||c.height!==Math.round(h*dpr)){c.width=Math.round(w*dpr);c.height=Math.round(h*dpr)}
 const ctx=c.getContext('2d');ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,w,h);
 const pts=hist.slice(-180).map(z=>z.p).filter(Number.isFinite);if(pts.length<2)return;
 const lo=Math.min(...pts),hi=Math.max(...pts),pad=Math.max((hi-lo)*.15,hi*.00001),min=lo-pad,max=hi+pad;
 ctx.strokeStyle='#172231';ctx.lineWidth=1;for(let i=1;i<4;i++){const y=i*h/4;ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(w,y);ctx.stroke()}
 const grad=ctx.createLinearGradient(0,0,0,h);grad.addColorStop(0,'rgba(53,229,138,.28)');grad.addColorStop(1,'rgba(53,229,138,0)');
 ctx.beginPath();pts.forEach((v,i)=>{const x=i/(pts.length-1)*w,y=h-(v-min)/(max-min)*h;i?ctx.lineTo(x,y):ctx.moveTo(x,y)});ctx.lineTo(w,h);ctx.lineTo(0,h);ctx.closePath();ctx.fillStyle=grad;ctx.fill();
 ctx.beginPath();pts.forEach((v,i)=>{const x=i/(pts.length-1)*w,y=h-(v-min)/(max-min)*h;i?ctx.lineTo(x,y):ctx.moveTo(x,y)});ctx.strokeStyle='#35e58a';ctx.lineWidth=2;ctx.stroke();
}
function resultFromMarket(m){
 const prices=parseArr(m?.outcomePrices||m?.outcome_prices).map(Number), outs=parseArr(m?.outcomes).map(x=>String(x).trim().toUpperCase());
 const winner=String(m?.winningOutcome||m?.winner||m?.resolution||'').toUpperCase();
 if(winner.includes('UP'))return'UP'; if(winner.includes('DOWN'))return'DOWN';
 if(prices.length===2){
  const idx=prices.findIndex(v=>v>=.999);
  if(idx>=0){const o=outs[idx]||'';return o.includes('DOWN')?'DOWN':'UP'}
  if(m?.closed===true&&Math.max(...prices)>=.98){const idx=prices[0]>prices[1]?0:1;return (outs[idx]||'').includes('DOWN')?'DOWN':'UP'}
 }
 return null;
}
async function fetchMarketBySlug(slug){
 for(const url of [`https://gamma-api.polymarket.com/markets/slug/${encodeURIComponent(slug)}`,`https://gamma-api.polymarket.com/markets?slug=${encodeURIComponent(slug)}`]){
  try{const r=await fetch(url,{cache:'no-store'});if(!r.ok)continue;const j=await r.json();const m=Array.isArray(j)?j[0]:(j.data||[])[0]||j;if(m)return m}catch{}
 }
 return null;
}
async function loadRecentResults(){
 const now=Math.floor(Date.now()/1000),base=now-now%300,arr=[];
 for(let i=1;i<=12&&arr.length<10;i++){
  const ts=base-i*300,slug=`btc-updown-5m-${ts}`,m=await fetchMarketBySlug(slug);if(!m)continue;
  const result=resultFromMarket(m);const end=Math.floor(new Date(m.endDate||m.endDateIso||0).getTime()/1000);if(result&&end&&end<=now)arr.push({result,end,slug});
 }
 recentResults=arr.slice(0,10);
}
function renderRecentResults(){const el=$('recentResults');if(!el)return;el.innerHTML=recentResults.length?recentResults.map(r=>`<span class="resultBall ${r.result==='UP'?'resultUp':'resultDown'}" title="${r.result} • ${fmtTime(r.end*1000)}"></span>`).join(''):'<span class="historyEmpty">No resolved 5M results yet.</span>';}
function entryKey(h){return `${h.marketEnd||0}|${h.generatedAt||0}|${h.side||''}`}
async function loadCalibration(){try{const z=await Promise.resolve(JSON.parse(localStorage.getItem('btc5mSniper')||'{}'));
 sectionVisibility={...sectionVisibility,...(z.sectionVisibility||{})};storedEntries=Array.isArray(z.sniperEntries)?z.sniperEntries:[];calibration=z.sniperCalibration||calibration;historyEntries.splice(0,historyEntries.length,...storedEntries.filter(x=>x.stage==='ENTRY').slice(-30).reverse());}catch{}}
async function saveCalibration(){try{await localStorage.setItem('btc5mSniper',JSON.stringify({...JSON.parse(localStorage.getItem('btc5mSniper')||'{}'),sniperEntries:storedEntries.slice(-200),sniperCalibration:calibration}))}catch{}}
async function saveVisibility(){try{await localStorage.setItem('btc5mSniper',JSON.stringify({...JSON.parse(localStorage.getItem('btc5mSniper')||'{}'),sectionVisibility}))}catch{}}
function probabilityBin(p){return Math.max(0,Math.min(9,Math.floor(Math.max(0,Math.min(.999,p))/.1)))}
function calibratedProbability(raw){const b=calibration.bins?.[probabilityBin(raw)];if(!b||b.n<5)return raw;const empirical=b.hits/b.n;const w=Math.min(.45,b.n/40*.45);return clamp(raw*(1-w)+empirical*w,.01,.99)}
function brier(){return calibration.total?calibration.brierSum/calibration.total:null}
async function settlePending(){const now=Date.now();let changed=false;for(const e of storedEntries){if(e.result||!e.marketEnd||e.marketEnd>now||!e.slug)continue;try{let m=null;for(const url of [`https://gamma-api.polymarket.com/markets/slug/${encodeURIComponent(e.slug)}`,`https://gamma-api.polymarket.com/markets?slug=${encodeURIComponent(e.slug)}`]){const r=await fetch(url,{cache:'no-store'});if(!r.ok)continue;const j=await r.json();m=Array.isArray(j)?j[0]:(j.data||[])[0]||j;if(m)break}if(!m)continue;let result=resultFromMarket(m);if(!result)continue;e.result=result;e.correct=e.side===result;const prob=e.side==='UP'?e.upProb:e.downProb,bin=probabilityBin(prob);calibration.bins[bin]=calibration.bins[bin]||{n:0,hits:0};calibration.bins[bin].n++;calibration.bins[bin].hits+=e.correct?1:0;calibration.total++;calibration.correct+=e.correct?1:0;calibration.brierSum+=(prob-(e.correct?1:0))**2;changed=true;}catch{}}if(changed){historyEntries.splice(0,historyEntries.length,...storedEntries.filter(x=>x.stage==='ENTRY').slice(-30).reverse());await saveCalibration();}}
async function persistEntry(h){const id=entryKey(h);if(storedEntries.some(x=>x.id===id))return;const row={id,stage:h.stage,side:h.side,confidence:h.confidence,upProb:h.upProb,downProb:h.downProb,generatedAt:h.generatedAt,marketEnd:h.marketEnd,slug:market?.slug||'',marketKey:h.marketKey||market?.slug||String(market?.endTs||''),ask:h.ask,potential:h.potential,edge:h.edge,result:null,correct:null};storedEntries.push(row);storedEntries=storedEntries.slice(-200);historyEntries.unshift(row);historyEntries.splice(30);await saveCalibration()}
async function ensureMarket(){if(market&&market.endTs>Date.now()/1000+2)return;try{await discoverDirect()}catch(e){$('status').textContent='Market discovery: '+e.message}}
$('refresh').onclick=async()=>{market=null;await ensureMarket();render()};$('clear').onclick=()=>{historyEntries.length=0;render()};
$('statsToggle').onclick=()=>{const e=$('statsDetails'),b=$('statsToggle');e.classList.toggle('show');b.textContent=e.classList.contains('show')?'HIDE STATS':'SHOW STATS'};
$('modeSelect').onchange=e=>{selectedMode=e.target.value;signalState={stage:'WATCH',side:'WAIT',confidence:50,generatedAt:Date.now(),validUntil:Date.now()+1000,potential:0,edge:0,reason:'Mode changed',lockedAsk:0}};
document.querySelectorAll('.hideToggle').forEach(btn=>btn.onclick=async()=>{const target=btn.dataset.target,el=$(target);el.classList.toggle('sectionHidden');const hidden=el.classList.contains('sectionHidden');btn.textContent=hidden?'SHOW':'HIDE';sectionVisibility[target]=!hidden;await saveVisibility()});
function applyVisibility(){Object.entries(sectionVisibility).forEach(([id,visible])=>{const el=$(id),btn=document.querySelector(`.hideToggle[data-target="${id}"]`);if(!el||!btn)return;el.classList.toggle('sectionHidden',visible===false);btn.textContent=visible===false?'SHOW':'HIDE'})}
(async()=>{await loadCalibration();applyVisibility();await settlePending();await loadRecentResults();startBinance();await ensureMarket();setInterval(()=>{ensureMarket();render()},250);setInterval(()=>{if(market){restPoly(market.upToken);restPoly(market.downToken)}},1000);setInterval(()=>{settlePending().then(render).catch(()=>{})},30000);setInterval(()=>{loadRecentResults().then(render).catch(()=>{})},60000)})();

$('tradeMode').onchange=e=>{tradeMode=e.target.value;render()};

const $=id=>document.getElementById(id);
const N=x=>Number(x)||0;

let market=null;
let polyWS=null;
let polyPing=null;

let signalState={
 stage:'WAIT',
 side:'WAIT',
 confidence:50,
 generatedAt:0,
 validUntil:0,
 potential:0,
 edge:0,
 lockedAsk:0,
 reason:'Warming up'
};

const historyEntries=[];
let storedEntries=[];

let calibration={
 bins:{},
 total:0,
 correct:0,
 brierSum:0
};

let loggedEntryId='';

const binance={
 bid:0,
 ask:0,
 last:0,
 bidSize:0,
 askSize:0,
 ts:0,
 flow:0,
 bids:[],
 asks:[],
 depthTs:0
};

const hist=[];
const trades=[];
const polyHist=[];
const books=new Map();

let selectedMode='precision';
let tradeMode='off';

let lastUpProb=.5;
let lastDownProb=.5;
let lastUpFlash=0;
let lastDownFlash=0;

let recentResults=[];

let sectionVisibility={
 binanceBody:true,
 edgeBody:true,
 splitBody:true
};

const comparisonHist=[];

let lastComparisonSnapshot=0;

let binanceWsLastBookTicker=0;
let binanceFallbackTimer=null;
let binanceFallbackMonitor=null;


/* =========================================================
   MODE CONFIG
   ========================================================= */

const modeConfig={

 precision:{
  minConf:70,
  minEdge:.055,
  minMomentum:.055,
  minFlow:.050,
  minPoly:.020,
  maxVol:2.0,
  persist:900,
  cooldown:30000
 },

 balanced:{
  minConf:64,
  minEdge:.035,
  minMomentum:.040,
  minFlow:.035,
  minPoly:.015,
  maxVol:2.35,
  persist:700,
  cooldown:25000
 },

 momentum:{
  minConf:67,
  minEdge:.045,
  minMomentum:.065,
  minFlow:.030,
  minPoly:.010,
  maxVol:2.6,
  persist:650,
  cooldown:25000
 },

 liquidity:{
  minConf:68,
  minEdge:.045,
  minMomentum:.035,
  minFlow:.040,
  minPoly:.025,
  maxVol:2.25,
  persist:850,
  cooldown:30000
 },

 auto:{
  minConf:72,
  minEdge:.065,
  minMomentum:.060,
  minFlow:.055,
  minPoly:.025,
  maxVol:1.9,
  persist:1200,
  cooldown:45000
 },

 edge:{
  minConf:76,
  minEdge:.085,
  minMomentum:.070,
  minFlow:.060,
  minPoly:.035,
  maxVol:1.75,
  persist:1500,
  cooldown:60000
 }

};


/* =========================================================
   HELPERS
   ========================================================= */

const clamp=(x,a,b)=>
 Math.max(
  a,
  Math.min(b,x)
 );

const pct=v=>
 `${(N(v)*100).toFixed(3)}%`;

const fmtPct=v=>
 `${N(v)>=0?'+':''}${(N(v)*100).toFixed(3)}%`;

const cents=v=>
 N(v)
  ?`${(N(v)*100).toFixed(2)}¢`
  :'—';


function setMove(id,box,v){

 const e=$(id);
 const b=$(box);
 const n=N(v);

 if(!e||!b)return;

 e.textContent=pct(n);

 e.style.color=
  n>0
   ?'#4be28f'
   :n<0
    ?'#ff8585'
    :'';

 b.classList.remove(
  'flashUp',
  'flashDown'
 );

 if(Math.abs(n)>0.000001){

  void b.offsetWidth;

  b.classList.add(
   n>0
    ?'flashUp'
    :'flashDown'
  );
 }
}


/* =========================================================
   GENERIC WEBSOCKET
   ========================================================= */

function openWS(url,onOpen,onMsg){

 let ws=null;
 let delay=500;
 let closedByUser=false;

 const go=()=>{

  if(closedByUser)
   return;

  try{

   ws=new WebSocket(url);

   ws.onopen=()=>{

    delay=500;

    try{
     onOpen?.(ws);
    }catch{}
   };

   ws.onmessage=e=>{

    try{

     const data=
      typeof e.data==='string'
       ?JSON.parse(e.data)
       :e.data;

     onMsg?.(data);

    }catch{}
   };

   ws.onerror=()=>{};

   ws.onclose=()=>{

    if(closedByUser)
     return;

    setTimeout(
     go,
     delay
    );

    delay=Math.min(
     10000,
     delay*2
    );
   };

  }catch{

   if(closedByUser)
    return;

   setTimeout(
    go,
    delay
   );

   delay=Math.min(
    10000,
    delay*2
   );
  }
 };

 go();

 return()=>{

  closedByUser=true;

  try{
   ws?.close();
  }catch{}
 };
}


/* =========================================================
   BINANCE DATA
   ========================================================= */

function updateBinance(
 bid,
 ask,
 last,
 bidSize,
 askSize,
 ts=Date.now()
){

 if(bid>0)
  binance.bid=bid;

 if(ask>0)
  binance.ask=ask;

 if(last>0){

  binance.last=last;

  hist.push({
   ts,
   p:last
  });

  while(hist.length>5000)
   hist.shift();
 }

 if(bidSize>=0)
  binance.bidSize=bidSize;

 if(askSize>=0)
  binance.askSize=askSize;

 binance.ts=ts;
}


function setDepth(
 bids,
 asks,
 ts=Date.now()
){

 binance.bids=(bids||[])
  .map(x=>({
   p:N(x[0]),
   q:N(x[1])
  }))
  .filter(
   x=>x.p>0&&x.q>0
  )
  .sort(
   (a,b)=>b.p-a.p
  )
  .slice(0,20);

 binance.asks=(asks||[])
  .map(x=>({
   p:N(x[0]),
   q:N(x[1])
  }))
  .filter(
   x=>x.p>0&&x.q>0
  )
  .sort(
   (a,b)=>a.p-b.p
  )
  .slice(0,20);

 binance.depthTs=ts;

 if(
  binance.bids[0]&&
  binance.asks[0]
 ){

  updateBinance(
   binance.bids[0].p,
   binance.asks[0].p,
   0,
   binance.bids[0].q,
   binance.asks[0].q,
   ts
  );
 }
}


/* =========================================================
   BINANCE REST FALLBACK
   ========================================================= */

async function pollBinanceFallback(){

 try{

  const r=await fetch(
   'https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT',
   {
    cache:'no-store',
    headers:{
     Accept:'application/json'
    }
   }
  );

  if(!r.ok)
   return;

  const x=await r.json();

  const bid=N(x.bidPrice);
  const ask=N(x.askPrice);

  if(
   bid>0&&
   ask>0
  ){

   updateBinance(
    bid,
    ask,
    (bid+ask)/2,
    N(x.bidQty),
    N(x.askQty),
    Date.now()
   );
  }

 }catch{}
}


function startBinanceFallback(){

 if(binanceFallbackTimer)
  return;

 pollBinanceFallback();

 binanceFallbackTimer=
  setInterval(()=>{

   if(
    !binanceWsLastBookTicker||
    Date.now()-
     binanceWsLastBookTicker>
     2500
   ){

    pollBinanceFallback();
   }

  },1000);
}


function stopBinanceFallback(){

 if(!binanceFallbackTimer)
  return;

 clearInterval(
  binanceFallbackTimer
 );

 binanceFallbackTimer=null;
}


/* =========================================================
   BINANCE LIVE CONNECTION
   ========================================================= */

function startBinance(){

 openWS(
  'wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@depth20@100ms/btcusdt@trade',
  null,
  m=>{

   const d=m?.data||m;

   if(!d)
    return;

   if(d.e==='bookTicker'){

    binanceWsLastBookTicker=
     Date.now();

    stopBinanceFallback();

    updateBinance(
     N(d.b),
     N(d.a),
     0,
     N(d.B),
     N(d.A),
     N(d.E)||Date.now()
    );

    return;
   }

   if(d.e==='depthUpdate'){

    setDepth(
     d.b,
     d.a,
     N(d.E)||Date.now()
    );

    return;
   }

   if(d.e==='trade'){

    const q=N(d.q);

    const flow=
     (d.m?-1:1)*q;

    binance.flow+=flow;

    trades.push({
     ts:N(d.T)||Date.now(),
     flow
    });

    while(trades.length>5000)
     trades.shift();

    updateBinance(
     0,
     0,
     N(d.p),
     -1,
     -1,
     N(d.T)||Date.now()
    );
   }
  }
 );

 if(!binanceFallbackMonitor){

  binanceFallbackMonitor=
   setInterval(()=>{

    const age=
     binanceWsLastBookTicker
      ?Date.now()-
       binanceWsLastBookTicker
      :Infinity;

    if(age>2500)
     startBinanceFallback();
    else
     stopBinanceFallback();

   },1000);
 }
}


/* =========================================================
   POLYMARKET
   ========================================================= */

function parseArr(v){

 if(Array.isArray(v))
  return v;

 if(typeof v==='string'){

  try{

   const x=JSON.parse(v);

   return Array.isArray(x)
    ?x
    :[];

  }catch{}
 }

 return[];
}


function tokenPair(m){

 const ids=
  parseArr(
   m?.clobTokenIds??
   m?.clob_token_ids
  );

 const outs=
  parseArr(m?.outcomes);

 let up=null;
 let down=null;

 outs.forEach((o,i)=>{

  const z=
   String(o)
    .toLowerCase()
    .trim();

  if(
   /^up(?:\b|\s)/.test(z)&&
   ids[i]
  )
   up=String(ids[i]);

  if(
   /^down(?:\b|\s)/.test(z)&&
   ids[i]
  )
   down=String(ids[i]);
 });

 if(
  (!up||!down)&&
  ids.length===2
 ){

  up=up||String(ids[0]||'');
  down=down||String(ids[1]||'');
 }

 return(
  up&&down
   ?{up,down}
   :null
 );
}


async function discoverDirect(){

 const now=
  Math.floor(
   Date.now()/1000
  );

 const baseTs=
  now-
  now%300;

 const candidates=[
  baseTs,
  baseTs-300,
  baseTs+300
 ];

 for(const ts of candidates){

  const slug=
   `btc-updown-5m-${ts}`;

  const urls=[

   `https://gamma-api.polymarket.com/events?slug=${encodeURIComponent(slug)}`,

   `https://gamma-api.polymarket.com/markets?slug=${encodeURIComponent(slug)}`

  ];

  for(const path of urls){

   try{

    const r=
     await fetch(
      path,
      {
       cache:'no-store',
       headers:{
        Accept:'application/json'
       }
      }
     );

    if(!r.ok)
     continue;

    const j=await r.json();

    const arr=
     Array.isArray(j)
      ?j
      :(j?.data||[]);

    const events=
     path.includes('/events')
      ?arr
      :arr.map(x=>({
       markets:[x],
       slug:x.slug
      }));

    for(const ev of events){

     for(
      const m of
      ev?.markets||[]
     ){

      const t=tokenPair(m);

      if(!t)
       continue;

      const end=
       m.endDate||
       m.endDateIso||
       ev.endDate;

      const endMs=
       new Date(end).getTime();

      const endTs=
       Math.floor(
        endMs/1000
       );

      const active=
       m.active!==false&&
       String(m.active)!=='false';

      if(
       endTs>now&&
       !m.closed&&
       active
      ){

       const nextMarket={
        slug:
         m.slug||
         ev.slug||
         slug,

        question:
         m.question||
         ev.title||
         slug,

        upToken:t.up,
        downToken:t.down,

        endTs
       };

       const changed=
        !market||
        market.slug!==nextMarket.slug;

       market=nextMarket;

       if(changed){

        books.clear();

        connectPoly([
         t.up,
         t.down
        ]);

        comparisonHist.length=0;
        lastComparisonSnapshot=0;
       }

       return;
      }
     }
    }

   }catch{}
  }
 }

 throw Error(
  'BTC 5M market discovery failed'
 );
}


function setBook(
 id,
 bids,
 asks
){

 if(!id)
  return;

 books.set(
  String(id),
  {
   bids:(bids||[])
    .map(x=>({
     price:N(x.price),
     size:N(x.size)
    }))
    .filter(
     x=>x.price>0&&x.size>0
    )
    .sort(
     (a,b)=>b.price-a.price
    ),

   asks:(asks||[])
    .map(x=>({
     price:N(x.price),
     size:N(x.size)
    }))
    .filter(
     x=>x.price>0&&x.size>0
    )
    .sort(
     (a,b)=>a.price-b.price
    ),

   ts:Date.now()
  }
 );
}


function applyPolyDelta(
 x,
 root
){

 const id=
  String(
   x?.asset_id||
   x?.assetId||
   root||
   ''
  );

 if(!id)
  return;

 const q=
  books.get(id)||{
   bids:[],
   asks:[],
   ts:0
  };

 const side=
  String(
   x?.side||''
  ).toUpperCase()==='BUY'
   ?q.bids
   :q.asks;

 const p=N(x?.price);
 const s=N(x?.size);

 if(p<=0)
  return;

 const i=
  side.findIndex(
   z=>z.price===p
  );

 if(s<=0){

  if(i>=0)
   side.splice(i,1);

 }else if(i>=0){

  side[i].size=s;

 }else{

  side.push({
   price:p,
   size:s
  });
 }

 q.bids.sort(
  (a,b)=>b.price-a.price
 );

 q.asks.sort(
  (a,b)=>a.price-b.price
 );

 q.ts=Date.now();

 books.set(
  id,
  q
 );
}


function connectPoly(ids){

 try{
  polyWS?.close();
 }catch{}

 clearInterval(polyPing);

 polyPing=null;

 const wanted=
  (ids||[])
   .map(String)
   .filter(Boolean);

 if(!wanted.length)
  return;

 polyWS=
  new WebSocket(
   'wss://ws-subscriptions-clob.polymarket.com/ws/market'
  );

 polyWS.onopen=()=>{

  try{

   polyWS.send(
    JSON.stringify({
     assets_ids:wanted,
     type:'market',
     custom_feature_enabled:true,
     initial_dump:true
    })
   );

  }catch{}

  polyPing=
   setInterval(()=>{

    try{
     polyWS?.send('PING');
    }catch{}

   },10000);
 };

 polyWS.onmessage=e=>{

  try{

   const raw=
    typeof e.data==='string'
     ?JSON.parse(e.data)
     :e.data;

   const msgs=
    Array.isArray(raw)
     ?raw
     :[raw];

   for(const m of msgs){

    if(!m||typeof m!=='object')
     continue;

    const ev=
     String(
      m.event_type||''
     ).toLowerCase();

    const root=
     String(
      m.asset_id||''
     );

    if(ev==='book'){

     setBook(
      root,
      m.bids,
      m.asks
     );

     continue;
    }

    if(ev==='price_change'){

     for(
      const x of
      m.price_changes||[]
     ){

      applyPolyDelta(
       x,
       x.asset_id||root
      );
     }

     continue;
    }

    if(ev==='best_bid_ask'){

     if(!root)
      continue;

     const q=
      books.get(root)||{
       bids:[],
       asks:[],
       ts:0
      };

     const bb=N(m.best_bid);
     const aa=N(m.best_ask);

     if(bb>0){

      q.bids=[{
       price:bb,
       size:
        N(m.best_bid_size)||
        q.bids[0]?.size||
        0
      }];
     }

     if(aa>0){

      q.asks=[{
       price:aa,
       size:
        N(m.best_ask_size)||
        q.asks[0]?.size||
        0
      }];
     }

     q.ts=Date.now();

     books.set(
      root,
      q
     );
    }
   }

  }catch{}
 };

 polyWS.onclose=()=>{

  clearInterval(polyPing);
  polyPing=null;

  setTimeout(()=>{

   if(
    market&&
    market.upToken&&
    market.downToken
   ){

    connectPoly([
     market.upToken,
     market.downToken
    ]);
   }

  },1000);
 };

 polyWS.onerror=()=>{

  try{
   polyWS?.close();
  }catch{}
 };
}


async function restPoly(token){

 if(!token)
  return;

 try{

  const r=
   await fetch(
    `https://clob.polymarket.com/book?token_id=${encodeURIComponent(token)}`,
    {
     cache:'no-store',
     headers:{
      Accept:'application/json'
     }
    }
   );

  if(!r.ok)
   return;

  const x=await r.json();

  setBook(
   token,
   x?.bids,
   x?.asks
  );

 }catch{}
}


/* =========================================================
   CALCULATION ENGINE
   ========================================================= */

function sample(ms){

 const now=Date.now();

 for(
  let i=hist.length-1;
  i>=0;
  i--
 ){

  if(
   now-
   hist[i].ts>=ms
  ){

   return hist[i].p;
  }
 }

 return null;
}


function windowReturn(ms){

 const p=
  binance.last||
  (binance.bid+binance.ask)/2;

 if(!p)
  return 0;

 const q=sample(ms);

 return q
  ?p/q-1
  :0;
}


function regressionSlope(points){

 if(points.length<5)
  return 0;

 const n=points.length;

 const mx=(n-1)/2;

 const my=
  points.reduce(
   (a,x)=>a+x,
   0
  )/n;

 let num=0;
 let den=0;

 for(let i=0;i<n;i++){

  num+=
   (i-mx)*
   (points[i]-my);

  den+=
   (i-mx)*
   (i-mx);
 }

 return den
  ?num/den
  :0;
}


function depthLiquidity(){

 const p=
  binance.last||
  (binance.bid+binance.ask)/2;

 if(
  !p||
  !binance.bids.length||
  !binance.asks.length
 ){

  return{
   imb:0,
   support:0,
   resistance:0,
   zone:'UNKNOWN',
   nearest:0
  };
 }

 const band=
  p*.0008;

 let bidQ=0;
 let askQ=0;
 let support=0;
 let resistance=0;

 for(const z of binance.bids){

  if(
   Math.abs(z.p-p)<=band
  ){

   bidQ+=z.q;

   support=Math.max(
    support,
    z.q*
    (
     1-
     Math.abs(z.p-p)/band
    )
   );
  }
 }

 for(const z of binance.asks){

  if(
   Math.abs(z.p-p)<=band
  ){

   askQ+=z.q;

   resistance=Math.max(
    resistance,
    z.q*
    (
     1-
     Math.abs(z.p-p)/band
    )
   );
  }
 }

 const imb=
  (bidQ+askQ)
   ?(bidQ-askQ)/(bidQ+askQ)
   :0;

 const zone=
  imb>.18
   ?'SUPPORT'
   :imb<-.18
    ?'RESISTANCE'
    :'BALANCED';

 return{
  imb,
  support,
  resistance,
  zone,
  nearest:
   Math.min(
    Math.abs(
     (binance.bids[0]?.p||p)-p
    ),
    Math.abs(
     (binance.asks[0]?.p||p)-p
    )
   )/p
 };
}


function bookStats(b){

 const bid=b?.bids?.[0];
 const ask=b?.asks?.[0];

 const bidQ=
  (b?.bids||[])
   .slice(0,5)
   .reduce(
    (a,z)=>a+z.size,
    0
   );

 const askQ=
  (b?.asks||[])
   .slice(0,5)
   .reduce(
    (a,z)=>a+z.size,
    0
   );

 const imb=
  (bidQ+askQ)
   ?(bidQ-askQ)/(bidQ+askQ)
   :0;

 const micro=
  bid&&ask&&
  (bid.size+ask.size)>0
   ?(
    (ask.price*bid.size)+
    (bid.price*ask.size)
   )/
   (bid.size+ask.size)
   :(bid?.price||
     ask?.price||
     0);

 const spread=
  bid&&ask
   ?ask.price-bid.price
   :0;

 return{
  bid:bid?.price||0,
  ask:ask?.price||0,
  bidSize:bid?.size||0,
  askSize:ask?.size||0,
  bidQ,
  askQ,
  imb,
  micro,
  spread
 };
}


function polyBookPressure(
 upB,
 downB
){

 const u=bookStats(upB);
 const d=bookStats(downB);

 return clamp(
  (u.imb-d.imb)*.5,
  -1,
  1
 );
}


function calculate(){

 const now=Date.now();

 const p=
  binance.last||
  (binance.bid+binance.ask)/2;

 if(
  !p||
  !binance.ts||
  now-binance.ts>2500
 ){

  return{
   status:'warming',
   signal:'WAIT',
   confidence:50,
   upProbability:.5,
   downProbability:.5,
   potential:0,
   edge:0,
   return1s:0,
   return3s:0,
   return5s:0,
   volatility:0,
   volRegime:0,
   polyR1:0,
   components:{}
  };
 }

 const p1=sample(1000);
 const p3=sample(3000);
 const p5=sample(5000);

 const r1=
  p1?p/p1-1:0;

 const r3=
  p3?p/p3-1:0;

 const r5=
  p5?p/p5-1:0;

 const depth=
  depthLiquidity();

 const totalBook=
  binance.bidSize+
  binance.askSize;

 const bookImb=
  totalBook
   ?(
    binance.bidSize-
    binance.askSize
   )/totalBook
   :0;

 const nowTrades=
  trades.filter(
   t=>now-t.ts<=5000
  );

 const recentFlow=
  nowTrades.reduce(
   (a,t)=>a+t.flow,
   0
  );

 const flowNorm=
  clamp(
   recentFlow/
   Math.max(
    1,
    totalBook*2.5
   ),
   -1,
   1
  );

 const upB=
  market
   ?books.get(market.upToken)
   :null;

 const downB=
  market
   ?books.get(market.downToken)
   :null;

 const U=bookStats(upB);
 const D=bookStats(downB);

 const upMid=
  U.bid&&U.ask
   ?(U.bid+U.ask)/2
   :U.ask||U.bid||0;

 const downMid=
  D.bid&&D.ask
   ?(D.bid+D.ask)/2
   :D.bid||D.ask||0;

 const marketImplied=
  upMid&&downMid
   ?clamp(
    (
     upMid+
     (1-downMid)
    )/2,
    .01,
    .99
   )
   :(upMid||.5);

 const polyPressure=
  upMid&&downMid
   ?clamp(
    (
     upMid-
     (1-downMid)
    )*.9,
    -.35,
    .35
   )
   :0;

 const polyBook=
  polyBookPressure(
   upB,
   downB
  );

 const polyBidPressure=
  clamp(
   (U.imb-D.imb)*.55,
   -1,
   1
  );

 const polyMicroProb=
  U.micro&&D.micro
   ?clamp(
    (
     U.micro+
     (1-D.micro)
    )/2,
    .01,
    .99
   )
   :marketImplied;

 const polyMicroDrift=
  clamp(
   polyMicroProb-
   marketImplied,
   -.25,
   .25
  );

 const polySpread=
  U.spread&&D.spread
   ?(
    U.spread+D.spread
   )/2
   :.05;

 const polyLiquidity=
  clamp(
   (
    U.bidQ+
    U.askQ+
    D.bidQ+
    D.askQ
   )/100,
   0,
   1
  );

 polyHist.push({
  ts:now,
  p:polyMicroProb
 });

 while(polyHist.length>2000)
  polyHist.shift();


 /* FIX:
    Find the newest sample that is at least 1 second old.
    The old code used .find() from the beginning,
    which selected a very stale sample.
 */

 let polyPrev=null;

 for(
  let i=polyHist.length-1;
  i>=0;
  i--
 ){

  if(
   now-
   polyHist[i].ts>=1000
  ){

   polyPrev=polyHist[i];
   break;
  }
 }

 const polyR1=
  polyPrev
   ?polyMicroProb-polyPrev.p
   :0;

 const recentPrices=
  hist
   .filter(
    z=>now-z.ts<=5000
   )
   .map(z=>z.p);

 const slope=
  regressionSlope(
   recentPrices
  );

 const slopeNorm=
  clamp(
   slope/
   Math.max(
    .01,
    p*.00002
   ),
   -1,
   1
  );

 const prices1m=
  hist
   .filter(
    z=>now-z.ts<=60000
   )
   .map(z=>z.p);

 const ema=(arr,alpha)=>{

  if(!arr.length)
   return p;

  let e=arr[0];

  for(
   const v of arr.slice(1)
  ){

   e=
    alpha*v+
    (1-alpha)*e;
  }

  return e;
 };

 const emaFast=
  ema(prices1m,.22);

 const emaSlow=
  ema(prices1m,.055);

 const trendNorm=
  clamp(
   (
    emaFast-emaSlow
   )/
   Math.max(
    .01,
    p*.0007
   ),
   -1,
   1
  );

 const deltas=
  prices1m
   .slice(1)
   .map(
    (v,i)=>
     v/prices1m[i]-1
   )
   .filter(Number.isFinite);

 let gains=0;
 let losses=0;

 for(
  const d of deltas.slice(-30)
 ){

  if(d>0)
   gains+=d;
  else
   losses-=d;
 }

 const rsi=
  50+
  (
   gains+losses
    ?50*(gains-losses)/
     (gains+losses)
    :0
  );

 const rsiNorm=
  clamp(
   (rsi-50)/20,
   -1,
   1
  );

 const rets=
  hist
   .slice(-240)
   .map(
    (z,i,a)=>
     i
      ?z.p/a[i-1].p-1
      :0
   )
   .filter(Boolean);

 const volatility=
  Math.sqrt(
   rets.reduce(
    (a,b)=>a+b*b,
    0
   )/
   Math.max(
    1,
    rets.length
   )
  );

 const volRegime=
  clamp(
   volatility/.00035,
   0,
   3
  );

 const volPenalty=
  volRegime>1.8
   ?clamp(
    (volRegime-1.8)*.08,
    0,
    .18
   )
   :0;

 const momentum=
  clamp(
   r1*1150+
   r3*380+
   r5*170+
   slopeNorm*.08,
   -.5,
   .5
  );

 const orderFlow=
  clamp(
   bookImb*.16+
   flowNorm*.18+
   depth.imb*.08,
   -.38,
   .38
  );

 const trend=
  clamp(
   trendNorm*.09+
   rsiNorm*.045,
   -.14,
   .14
  );

 const poly=
  clamp(
   polyPressure*.16+
   polyBook*.08+
   polyBidPressure*.07+
   polyR1*1.8+
   polyMicroDrift*.20,
   -.16,
   .16
  );

 const liquidityZone=
  depth.zone;

 const liquidityBias=
  clamp(
   depth.imb*.10,
   -.10,
   .10
  );

 const technicalScore=
  clamp(
   momentum+
   orderFlow+
   trend+
   liquidityBias,
   -.85,
   .85
  );

 const technicalProb=
  1/
  (
   1+
   Math.exp(
    -technicalScore*4.6
   )
  );

 const marketDriftProb=
  clamp(
   .5+
   polyR1*2.8+
   polyMicroDrift*.8,
   .35,
   .85
  );

 const bidAskProb=
  clamp(
   .5+
   polyBidPressure*.16+
   polyBook*.10,
   .01,
   .99
  );

 let blended=
  technicalProb*.48+
  marketImplied*.16+
  marketDriftProb*.10+
  bidAskProb*.11+
  (
   technicalProb+
   poly*.8
  )*.15;

 blended=
  .5+
  (blended-.5)*
  (
   1-
   volPenalty-
   clamp(
    polySpread/.08,
    0,
    1
   )*.05
  );

 const rawUpProbability=
  clamp(
   blended,
   .01,
   .99
  );

 const up=
  calibratedProbability(
   rawUpProbability
  );

 const down=
  1-up;

 const confidence=
  50+
  Math.min(
   49,
   Math.abs(up-.5)*190
  );

 const signal=
  up>=.5
   ?'UP'
   :'DOWN';

 const upEdge=
  U.ask
   ?up-U.ask
   :0;

 const downEdge=
  D.ask
   ?down-D.ask
   :0;

 const upBidEdge=
  U.bid
   ?up-U.bid
   :0;

 const downBidEdge=
  D.bid
   ?down-D.bid
   :0;

 const edge=
  Math.max(
   upEdge,
   downEdge
  );

 const bestBidExit=
  Math.max(
   upBidEdge,
   downBidEdge
  );

 const potential=
  clamp(
   (up-.5)*1.8,
   -.85,
   .85
  );

 const alignment=
  (
   Math.sign(momentum)===
   Math.sign(orderFlow)
   ?1:0
  )+
  (
   Math.sign(momentum)===
   Math.sign(trend)
   ?1:0
  )+
  (
   Math.sign(momentum)===
   Math.sign(poly)
   ?1:0
  )+
  (
   Math.sign(momentum)===
   Math.sign(depth.imb)
   ?1:0
  );

 const setupScore=
  Math.abs(technicalScore)*100+
  Math.max(0,edge)*220+
  Math.abs(polyR1)*100+
  Math.abs(depth.imb)*8+
  Math.abs(polyBidPressure)*12;

 return{

  price:p,

  return1s:r1,
  return3s:r3,
  return5s:r5,

  upProbability:up,
  downProbability:down,

  rawUpProbability,
  rawDownProbability:
   1-rawUpProbability,

  marketImplied,
  polyR1,

  signal,
  confidence,

  score:
   technicalScore*100,

  volatility,
  volRegime,

  components:{

   momentum,
   orderFlow,
   polymarketPressure:poly,
   trend,

   slopeNorm,
   bookImb,
   flowNorm,

   polyMid:marketImplied,
   technicalProb,
   marketImplied,

   polySpread,
   polyLiquidity,

   emaFast,
   emaSlow,
   rsi,

   polyBook,
   polyBidPressure,

   polyMicroProb,
   polyMicroDrift,

   liquidityZone,
   liquidityBias,

   depthSupport:depth.support,
   depthResistance:depth.resistance,

   alignment
  },

  upBook:upB,
  downBook:downB,

  upStats:U,
  downStats:D,

  potential,

  upEdge,
  downEdge,

  upBidEdge,
  downBidEdge,

  bestBidExit,

  edge,
  setupScore
 };
}


/* =========================================================
   STABLE SIGNAL
   ========================================================= */

let candidateSince=0;
let lastCandidateKey='';


function stableSignal(x){

 const now=Date.now();

 const secs=
  market
   ?market.endTs-
    now/1000
   :0;

 const cfg=
  modeConfig[selectedMode]||
  modeConfig.precision;

 const mode=
  selectedMode==='auto'
   ?modeConfig.auto
   :cfg;

 const side=
  x.upProbability>=
  x.downProbability
   ?'UP'
   :'DOWN';

 const ask=
  side==='UP'
   ?x.upStats?.ask
   :side==='DOWN'
    ?x.downStats?.ask
    :0;

 const edge=
  side==='UP'
   ?x.upEdge
   :x.downEdge;

 const m=
  Math.abs(
   x.components?.momentum||0
  );

 const f=
  Math.abs(
   x.components?.orderFlow||0
  );

 const pp=
  Math.abs(
   x.components?.polymarketPressure||0
  );

 const a=
  x.components?.alignment||0;

 const key=
  `${side}|${market?.slug||''}`;

 let stage='WATCH';

 let reason=
  'Waiting for aligned factors';

 const directional=
  side==='UP'
   ?1
   :-1;

 const factors=[
  x.components?.momentum||0,
  x.components?.orderFlow||0,
  x.components?.trend||0,
  x.components?.polymarketPressure||0,
  x.components?.bookImb||0,
  x.components?.polyBidPressure||0
 ].map(
  v=>
   Math.sign(v)===
   directional
 );

 const agree=
  factors.filter(Boolean).length;

 const bidConfirm=
  side==='UP'
   ?(
    N(x.upStats?.bid)>
    N(x.downStats?.ask)*.95
   )
   :(
    N(x.downStats?.bid)>
    N(x.upStats?.ask)*.95
   );

 const quality=
  agree>=5&&
  a>=3&&
  edge>=mode.minEdge&&
  x.confidence>=mode.minConf&&
  m>=mode.minMomentum&&
  f>=mode.minFlow&&
  pp>=mode.minPoly&&
  x.volRegime<mode.maxVol&&
  Math.abs(x.polyR1||0)<.025&&
  bidConfirm;

 const setup=
  agree>=4&&
  a>=2&&
  edge>=mode.minEdge*.55&&
  x.confidence>=mode.minConf-7&&
  m>=mode.minMomentum*.6&&
  x.volRegime<
   mode.maxVol+.35;

 if(x.status==='warming'){

  stage='WAIT';

  reason=
   'Waiting for fresh Binance data';

 }else if(secs<=60){

  stage='WAIT';

  reason=
   'Last minute protection — no new sniper entry';

 }else if(
  !binance.ts||
  now-binance.ts>1200
 ){

  stage='WAIT';

  reason=
   'Binance feed stale';

 }else if(!ask){

  stage='WATCH';

  reason=
   'Polymarket executable ask unavailable';

 }else if(quality){

  if(
   lastCandidateKey!==key
  ){

   candidateSince=now;
   lastCandidateKey=key;
  }

  const elapsed=
   now-candidateSince;

  if(
   elapsed>=mode.persist
  ){

   stage='ENTRY';

   reason=
    `${selectedMode.toUpperCase()} mode: ${agree}/6 factors + bid confirmation`;

  }else{

   stage='SETUP';

   reason=
    `Confirming setup ${Math.max(
     0,
     mode.persist-elapsed
    )}ms`;
  }

 }else if(setup){

  stage='SETUP';

  reason=
   `Setup forming: ${agree}/6 factors aligned`;

 }else{

  stage='WATCH';

  reason=
   `Filtered: ${agree}/6 factors aligned`;
 }

 const marketKey=
  market?.slug||
  String(
   market?.endTs||''
  );

 const already=
  storedEntries.some(
   e=>
    e.stage==='ENTRY'&&
    e.marketKey===marketKey
  );

 const lastEntry=
  storedEntries
   .filter(
    e=>e.stage==='ENTRY'
   )
   .reduce(
    (a,e)=>
     Math.max(
      a,
      e.generatedAt||0
     ),
    0
   );

 if(
  stage==='ENTRY'&&
  (
   already||
   now-lastEntry<
   mode.cooldown
  )
 ){

  stage='SETUP';

  reason=
   already
    ?'One entry already logged for this 5M market'
    :'Entry cooldown active';
 }

 const valid=
  stage==='ENTRY'
   ?Math.min(
    now+8000,
    (
     market?.endTs||
     now/1000
    )*1000-65000
   )
   :now+2500;

 const lockedAsk=
  stage==='ENTRY'
   ?N(ask)
   :N(signalState.lockedAsk);

 const newSignal={
  stage,

  side:
   stage==='WAIT'
    ?'WAIT'
    :side,

  confidence:
   x.confidence||50,

  generatedAt:now,

  validUntil:valid,

  potential:
   x.potential||0,

  edge:N(edge),

  reason,

  lockedAsk,

  mode:selectedMode,

  liquidityZone:
   x.components?.liquidityZone||
   'UNKNOWN'
 };

 const changed=
  stage!==signalState.stage||
  newSignal.side!==signalState.side||
  newSignal.lockedAsk!==signalState.lockedAsk;

 if(
  changed||
  now-signalState.generatedAt>4000
 ){

  signalState=newSignal;

  if(stage==='ENTRY'){

   const h={
    ...newSignal,

    marketEnd:
     market?.endTs*1000||0,

    ask:N(ask),

    upProb:
     x.upProbability,

    downProb:
     x.downProbability,

    marketKey,

    slug:
     market?.slug||''
   };

   const id=entryKey(h);

   if(id!==loggedEntryId){

    loggedEntryId=id;

    persistEntry(h);
   }
  }
 }

 if(
  now>signalState.validUntil&&
  signalState.stage!=='WAIT'
 ){

  signalState={
   ...signalState,
   stage:'WATCH',
   validUntil:now+2500,
   lockedAsk:0
  };
 }

 return signalState;
}


/* =========================================================
   PRICE COMPARISON
   ========================================================= */

function comparisonSnapshot(){

 const now=Date.now();

 /*
   Four snapshots per second were unnecessary.
   Keep the comparison engine smooth while reducing
   local memory churn.
 */

 if(
  now-lastComparisonSnapshot<250
 )
  return;

 lastComparisonSnapshot=now;

 const spot=
  binance.bid&&binance.ask
   ?(
    binance.bid+
    binance.ask
   )/2
   :binance.last||0;

 const upBook=
  market
   ?books.get(market.upToken)
   :null;

 const downBook=
  market
   ?books.get(market.downToken)
   :null;

 const upStats=
  bookStats(upBook);

 const downStats=
  bookStats(downBook);

 const up=
  upStats.ask||
  upStats.bid||
  0;

 const down=
  downStats.ask||
  downStats.bid||
  0;

 if(
  !spot&&
  !up&&
  !down
 )
  return;

 comparisonHist.push({
  ts:now,
  spot,
  up,
  down
 });

 while(
  comparisonHist.length>3600
 )
  comparisonHist.shift();
}


function comparisonChange(
 key,
 ms
){

 const now=Date.now();

 const cur=
  comparisonHist[
   comparisonHist.length-1
  ];

 if(
  !cur||
  !N(cur[key])
 )
  return 0;

 let old=null;

 for(
  let i=
   comparisonHist.length-1;
  i>=0;
  i--
 ){

  const row=
   comparisonHist[i];

  if(
   now-row.ts>=ms
  ){

   old=row;
   break;
  }
 }

 if(
  !old||
  !N(old[key])
 )
  return 0;

 return(
  cur[key]/
  old[key]
 )-1;
}


function renderComparison(
 ua,
 da
){

 if(!$('compareBinancePrice'))
  return;

 const now=Date.now();

 const spot=
  binance.bid&&binance.ask
   ?(
    binance.bid+
    binance.ask
   )/2
   :binance.last||0;

 const up=
  ua?.ask||
  ua?.bid||
  0;

 const down=
  da?.ask||
  da?.bid||
  0;

 $('compareBinancePrice')
  .textContent=
   spot
    ?spot.toFixed(2)
    :'—';

 $('compareUpPrice')
  .textContent=
   up
    ?cents(up)
    :'—';

 $('compareDownPrice')
  .textContent=
   down
    ?cents(down)
    :'—';

 const set=(id,v)=>{

  const el=$(id);

  if(!el)
   return;

  const n=N(v);

  el.textContent=
   fmtPct(n);

  el.style.color=
   n>0
    ?'var(--up)'
    :n<0
     ?'var(--down)'
     :'var(--muted)';
 };

 set(
  'compareBinance1s',
  comparisonChange(
   'spot',
   1000
  )
 );

 set(
  'compareUp1s',
  comparisonChange(
   'up',
   1000
  )
 );

 set(
  'compareDown1s',
  comparisonChange(
   'down',
   1000
  )
 );

 set(
  'compareBinance5s',
  comparisonChange(
   'spot',
   5000
  )
 );

 set(
  'compareUp5s',
  comparisonChange(
   'up',
   5000
  )
 );

 set(
  'compareDown5s',
  comparisonChange(
   'down',
   5000
  )
 );

 set(
  'compareBinance15s',
  comparisonChange(
   'spot',
   15000
  )
 );

 set(
  'compareUp15s',
  comparisonChange(
   'up',
   15000
  )
 );

 set(
  'compareDown15s',
  comparisonChange(
   'down',
   15000
  )
 );

 const spot5=
  comparisonChange(
   'spot',
   5000
  );

 const u5=
  comparisonChange(
   'up',
   5000
  );

 const d5=
  comparisonChange(
   'down',
   5000
  );

 set(
  'compareSpotUpMove',
  u5-spot5
 );

 set(
  'compareSpotDownMove',
  d5-spot5
 );

 if(up&&down){

  const combined=
   up+down;

  $('compareCombined')
   .textContent=
    (
     combined*100
    ).toFixed(2)+'¢';

  $('compareCombined')
   .style.color=
    Math.abs(
     combined-1
    )<.02
     ?'var(--up)'
     :'var(--gold)';

 }else{

  $('compareCombined')
   .textContent='—';
 }

 if(market){

  $('compareMarketTime')
   .textContent=
    Math.max(
     0,
     Math.ceil(
      market.endTs-
      now/1000
     )
    )+'s';

 }else{

  $('compareMarketTime')
   .textContent='—';
 }

 const age=
  binance.ts
   ?now-binance.ts
   :99999;

 if($('comparisonFeed')){

  $('comparisonFeed')
   .textContent=
    age<2500
     ?'LIVE'
     :'STALE';

  $('comparisonFeed')
   .style.color=
    age<2500
     ?'var(--up)'
     :'var(--down)';
 }
}


/* =========================================================
   DISPLAY HELPERS
   ========================================================= */

function fmtTime(ms){

 return ms
  ?new Date(ms)
   .toLocaleTimeString(
    [],
    {
     hour:'2-digit',
     minute:'2-digit',
     second:'2-digit'
    }
   )
  :'—';
}


function expiryText(){

 if(!market)
  return'EXP —';

 const s=
  Math.max(
   0,
   Math.ceil(
    market.endTs-
    Date.now()/1000
   )
  );

 return `5M EXP ${s}s • ${fmtTime(
  market.endTs*1000
 )}`;
}


/* =========================================================
   MAIN RENDER
   ========================================================= */

function render(){

 const x=calculate();

 const s=stableSignal(x);

 const upB=x.upBook;
 const downB=x.downBook;

 const ua=x.upStats;
 const da=x.downStats;

 const now=Date.now();

 comparisonSnapshot();
 renderComparison(
  ua,
  da
 );

 const secs=
  market
   ?Math.max(
    0,
    Math.ceil(
     market.endTs-
     now/1000
    )
   )
   :0;

 if(market){

  if($('marketTitle'))
   $('marketTitle').textContent=
    market.question||
    market.slug;

  if($('openMarket'))
   $('openMarket').href=
    `https://polymarket.com/event/${encodeURIComponent(
     market.slug||''
    )}`;

  if($('timeLeft'))
   $('timeLeft').textContent=
    secs+'s';

  if($('expiryTime'))
   $('expiryTime').textContent=
    fmtTime(
     market.endTs*1000
    );

  if($('expiryTime'))
   $('expiryTime').classList.toggle(
    'expiry-last',
    secs<=60
   );

  document
   .querySelectorAll(
    '.expiryBadge'
   )
   .forEach(e=>{

    e.textContent=
     expiryText();

    e.classList.toggle(
     'expiry-last',
     secs<=60
    );
   });

  if(
   $('timeLeft')&&
   $('timeLeft').parentElement
  ){

   $('timeLeft')
    .parentElement
    .classList.toggle(
     'expiry-last',
     secs<=60
    );
  }

  if($('expiryEntry2'))
   $('expiryEntry2')
    .classList.toggle(
     'expiry-last',
     secs<=60
    );

  if($('entryTimeframe'))
   $('entryTimeframe')
    .classList.toggle(
     'expiry-last',
     secs<=60
    );
 }

 const spot=
  binance.bid&&binance.ask
   ?(
    binance.bid+
    binance.ask
   )/2
   :x.price||0;

 if($('spotPrice'))
  $('spotPrice').textContent=
   spot
    ?spot.toFixed(2)
    :'—';

 if($('detailBid'))
  $('detailBid').textContent=
   binance.bid
    ?binance.bid.toFixed(2)
    :'—';

 if($('detailAsk'))
  $('detailAsk').textContent=
   binance.ask
    ?binance.ask.toFixed(2)
    :'—';

 if($('detailDepth'))
  $('detailDepth').textContent=
   `${binance.bids.length}/${binance.asks.length}`;

 if($('detailFeed'))
  $('detailFeed').textContent=
   binance.ts
    ?Math.max(
     0,
     now-binance.ts
    )+'ms'
    :'—';

 if($('lastPrice'))
  $('lastPrice').textContent=
   binance.last
    ?binance.last.toFixed(2)
    :'—';

 if($('spotSignal'))
  $('spotSignal').textContent=
   x.signal||'WAIT';

 if($('binanceBid'))
  $('binanceBid').textContent=
   binance.bid
    ?binance.bid.toFixed(2)
    :'—';

 if($('binanceAsk'))
  $('binanceAsk').textContent=
   binance.ask
    ?binance.ask.toFixed(2)
    :'—';

 if($('binanceFeed'))
  $('binanceFeed').textContent=
   binance.ts
    ?`${Math.max(
      0,
      now-binance.ts
     )}ms`
    :'—';

 if($('upAsk'))
  $('upAsk').textContent=
   ua?.ask
    ?cents(ua.ask)
    :'—';

 if($('downAsk'))
  $('downAsk').textContent=
   da?.ask
    ?cents(da.ask)
    :'—';

 if($('upBid'))
  $('upBid').textContent=
   ua?.bid
    ?cents(ua.bid)
    :'—';

 if($('downBid'))
  $('downBid').textContent=
   da?.bid
    ?cents(da.bid)
    :'—';

 if($('upSize'))
  $('upSize').textContent=
   ua?.askSize
    ?`${ua.askSize.toFixed(2)} ask shares`
    :'—';

 if($('downSize'))
  $('downSize').textContent=
   da?.askSize
    ?`${da.askSize.toFixed(2)} ask shares`
    :'—';

 const ages=[
  upB?.ts,
  downB?.ts
 ].filter(Boolean);

 if($('bookAge'))
  $('bookAge').textContent=
   ages.length
    ?Math.max(
     0,
     now-Math.max(...ages)
    )+'ms'
    :'—';

 if($('feedState'))
  $('feedState').textContent=
   upB&&downB
    ?'LIVE'
    :'WAITING';

 if($('upProb'))
  $('upProb').textContent=
   (
    x.upProbability*100
   ).toFixed(1)+'%';

 if($('downProb'))
  $('downProb').textContent=
   (
    x.downProbability*100
   ).toFixed(1)+'%';

 if($('probBarUp'))
  $('probBarUp').style.width=
   (
    x.upProbability*100
   ).toFixed(1)+'%';

 if($('ret1s'))
  $('ret1s').textContent=
   fmtPct(x.return1s);

 if($('ret3s'))
  $('ret3s').textContent=
   fmtPct(x.return3s);

 if($('ret5s'))
  $('ret5s').textContent=
   fmtPct(x.return5s);

 if($('factorMomentum'))
  $('factorMomentum').textContent=
   fmtPct(
    x.components?.momentum
   );

 if($('factorFlow'))
  $('factorFlow').textContent=
   fmtPct(
    x.components?.orderFlow
   );

 if($('factorPoly'))
  $('factorPoly').textContent=
   fmtPct(
    x.components?.polymarketPressure
   );

 if($('factorRsi'))
  $('factorRsi').textContent=
   x.components?.rsi!=null
    ?x.components.rsi.toFixed(1)
    :'—';

 if($('modelScore'))
  $('modelScore').textContent=
   x.score!=null
    ?x.score.toFixed(1)
    :'—';

 if($('volatility'))
  $('volatility').textContent=
   x.volatility!=null
    ?(
     x.volatility*100
    ).toFixed(3)+'%'
    :'—';

 if($('rsiValue'))
  $('rsiValue').textContent=
   x.components?.rsi!=null
    ?x.components.rsi.toFixed(1)
    :'—';

 if($('depthZone'))
  $('depthZone').textContent=
   x.components?.liquidityZone||
   '—';

 if($('alignment'))
  $('alignment').textContent=
   (
    x.components?.alignment||0
   )+'/4';

 if($('tradeCount'))
  $('tradeCount').textContent=
   String(trades.length);

 const avgSpread=
  (
   N(ua?.spread)+
   N(da?.spread)
  )/2;

 if($('bookSpread'))
  $('bookSpread').textContent=
   avgSpread
    ?cents(avgSpread)
    :'—';

 if($('bookLiquidity'))
  $('bookLiquidity').textContent=
   x.components?.polyLiquidity!=null
    ?(
     x.components.polyLiquidity*100
    ).toFixed(0)+'%'
    :'—';

 if($('binanceDot'))
  $('binanceDot').className=
   binance.ts&&
   now-binance.ts<2500
    ?'live'
    :'warn';

 if($('polyDot'))
  $('polyDot').className=
   upB&&downB
    ?'live'
    :'warn';

 drawBTCChart();

 if($('upModelHint'))
  $('upModelHint').textContent=
   `${x.confidence.toFixed(1)}% confidence • ${
    x.components?.alignment||0
   }/4 agreement`;

 if($('downModelHint'))
  $('downModelHint').textContent=
   `${x.confidence.toFixed(1)}% confidence • ${
    x.components?.alignment||0
   }/4 agreement`;

 const upDelta=
  x.upProbability-
  lastUpProb;

 const downDelta=
  x.downProbability-
  lastDownProb;

 if(
  Math.abs(upDelta)>=.008&&
  now-lastUpFlash>120&&
  $('upProbCard')
 ){

  const card=$('upProbCard');

  card.classList.remove(
   'probFlashUp'
  );

  void card.offsetWidth;

  card.classList.add(
   'probFlashUp'
  );

  lastUpFlash=now;
 }

 if(
  Math.abs(downDelta)>=.008&&
  now-lastDownFlash>120&&
  $('downProbCard')
 ){

  const card=$('downProbCard');

  card.classList.remove(
   'probFlashDown'
  );

  void card.offsetWidth;

  card.classList.add(
   'probFlashDown'
  );

  lastDownFlash=now;
 }

 lastUpProb=
  x.upProbability;

 lastDownProb=
  x.downProbability;

 const c=x.components||{};

 const fmt=v=>
  v==null
   ?'—'
   :(N(v)>=0?'+':'')+
    (N(v)*100).toFixed(2)+'%';

 const liveAsk=
  s.side==='UP'
   ?ua?.ask
   :s.side==='DOWN'
    ?da?.ask
    :0;

 if($('sniperBox'))
  $('sniperBox').className=
   'sniper '+
   String(
    s.stage||'WAIT'
   ).toLowerCase();

 if($('sniperText'))
  $('sniperText').textContent=
   `${
    s.stage==='ENTRY'
     ?'⚡'
     :s.stage==='SETUP'
      ?'🎯'
      :'👀'
   } ${s.stage}`;

 if($('sniperSub'))
  $('sniperSub').textContent=
   `5M • expires ${
    fmtTime(
     market?.endTs*1000
    )
   } • signal ${
    fmtTime(
     s.generatedAt
    )
   }`;

 if($('entryQuality'))
  $('entryQuality').textContent=
   s.stage==='ENTRY'
    ?'HIGH-CONVICTION'
    :s.stage==='SETUP'
     ?'FORMING'
     :'FILTERED';

 if($('entryStage'))
  $('entryStage').textContent=
   s.stage;

 if($('entrySide'))
  $('entrySide').textContent=
   s.side;

 if($('entrySide'))
  $('entrySide').className=
   s.side==='UP'
    ?'upText'
    :s.side==='DOWN'
     ?'downText'
     :'';

 if($('liveEntryAsk'))
  $('liveEntryAsk').textContent=
   liveAsk
    ?cents(liveAsk)
    :'—';

 if($('signalTime'))
  $('signalTime').textContent=
   s.generatedAt
    ?fmtTime(s.generatedAt)
    :'—';

 if($('venueDetail'))
  $('venueDetail').textContent=
   `Binance BTC/USDT • ${
    binance.ts
     ?Math.max(
      0,
      now-binance.ts
     )
     :'—'
   }ms feed age • ${
    trades.length
   } buffered trades • ${
    binance.bids.length
   }/${
    binance.asks.length
   } depth levels`;

 if($('status'))
  $('status').textContent=
   binance.ts&&
   now-binance.ts<2500
    ?`LIVE • Binance Spot + Polymarket • ${
      fmtTime(now)
     }`
    :'WAITING FOR BINANCE LIVE FEED';


 /* =======================================================
    DIRECTIONAL EDGE
    ======================================================= */

 const buyUp=x.upEdge;
 const buyDown=x.downEdge;

 const sellUp=x.upBidEdge;
 const sellDown=x.downBidEdge;

 const bestSide=
  buyUp>=buyDown
   ?'UP'
   :'DOWN';

 const bestBuy=
  Math.max(
   buyUp,
   buyDown
  );

 const bestSell=
  Math.max(
   sellUp,
   sellDown
  );

 if($('oneDirSignal'))
  $('oneDirSignal').textContent=
   bestBuy>.025
    ?`BUY ${bestSide}`
    :'WAIT';

 if($('oneDirSignal'))
  $('oneDirSignal').className=
   bestBuy>.025
    ?(
     bestSide==='UP'
      ?'upText'
      :'downText'
    )
    :'';

 if($('oneDirDetail'))
  $('oneDirDetail').textContent=
   bestBuy>.025
    ?`Model ${
     (
      (
       bestSide==='UP'
        ?x.upProbability
        :x.downProbability
      )*100
     ).toFixed(1)
    }% vs ask ${
     cents(
      bestSide==='UP'
       ?ua?.ask
       :da?.ask
     )
    } • edge ${
     fmt(bestBuy)
    }`
    :'No sufficient model-vs-ask edge';

 const exitSide=
  bestSell===sellUp
   ?'UP'
   :'DOWN';

 const exitEdge=
  bestSell;

 let action='HOLD';

 if(exitEdge>.03){

  action=
   `SELL ${exitSide}`;

 }else if(bestBuy>.04){

  action=
   `HOLD / ADD ${bestSide}`;
 }

 if($('positionAction'))
  $('positionAction').textContent=
   action;

 if($('positionAction'))
  $('positionAction').className=
   action.includes('SELL')
    ?(
     exitSide==='UP'
      ?'upText'
      :'downText'
    )
    :'';

 if($('positionDetail'))
  $('positionDetail').textContent=
   `Best bid edge ${fmt(exitEdge)} • current model ${
    bestSide==='UP'
     ?(
      x.upProbability*100
     ).toFixed(1)
     :(
      x.downProbability*100
     ).toFixed(1)
   }%`;

 if($('bestEdge'))
  $('bestEdge').textContent=
   fmt(
    Math.max(
     bestBuy,
     bestSell
    )
   );

 if($('bestEdgeDetail'))
  $('bestEdgeDetail').textContent=
   `BUY ${fmt(bestBuy)} • SELL/EXIT ${fmt(bestSell)}`;

 if($('bidEdge'))
  $('bidEdge').textContent=
   fmt(bestSell);

 if($('bidEdgeDetail'))
  $('bidEdgeDetail').textContent=
   `UP bid ${cents(ua?.bid)} • DOWN bid ${cents(da?.bid)}`;


 /* =======================================================
    SPLIT TRADING
    ======================================================= */

 const splitTotal=
  Math.max(
   x.upProbability+
   x.downProbability,
   .0001
  );

 const upAlloc=
  clamp(
   x.upProbability/
   splitTotal*100,
   0,
   100
  );

 const downAlloc=
  100-upAlloc;

 if($('upAlloc'))
  $('upAlloc').textContent=
   tradeMode==='split'
    ?upAlloc.toFixed(0)+'%'
    :'—';

 if($('downAlloc'))
  $('downAlloc').textContent=
   tradeMode==='split'
    ?downAlloc.toFixed(0)+'%'
    :'—';

 let splitDecision='WAIT';
 let holdSell='—';

 if(tradeMode==='split'){

  const spreadCost=
   N(ua?.ask)+
   N(da?.ask);

  if(spreadCost<.995)
   splitDecision=
    'LOCKED-COST EDGE';

  else if(
   bestBuy>.035&&
   x.volRegime<2.2
  )
   splitDecision=
    `LEAN ${bestSide}`;

  else
   splitDecision=
    'HOLD / WAIT';

  holdSell=
   bestSell>.03
    ?`SELL ${exitSide}`
    :bestBuy>.035
     ?'HOLD'
     :'REDUCE RISK';

 }else if(tradeMode==='oneway'){

  splitDecision=
   bestBuy>.025
    ?`BUY ${bestSide}`
    :'WAIT';

  holdSell=
   bestSell>.03
    ?`SELL ${exitSide}`
    :'HOLD';
 }

 if($('splitDecision'))
  $('splitDecision').textContent=
   splitDecision;

 if($('holdSell'))
  $('holdSell').textContent=
   holdSell;


 /* =======================================================
    CALIBRATION
    ======================================================= */

 if($('calAccuracy'))
  $('calAccuracy').textContent=
   calibration.total
    ?(
     calibration.correct/
     calibration.total*
     100
    ).toFixed(1)+'%'
    :'—';

 if($('calBrier'))
  $('calBrier').textContent=
   brier()!=null
    ?brier().toFixed(4)
    :'—';

 if($('calSamples'))
  $('calSamples').textContent=
   String(
    calibration.total||0
   );


 /* =======================================================
    STATISTICS
    ======================================================= */

 const resolved=
  storedEntries.filter(
   e=>
    e.result==='UP'||
    e.result==='DOWN'
  );

 const wins=
  resolved.filter(
   e=>e.correct===true
  ).length;

 const losses=
  resolved.filter(
   e=>e.correct===false
  ).length;

 const totalResolved=
  wins+losses;

 const upPred=
  resolved.filter(
   e=>e.side==='UP'
  );

 const downPred=
  resolved.filter(
   e=>e.side==='DOWN'
  );

 const upCorrect=
  upPred.filter(
   e=>e.result==='UP'
  ).length;

 const downCorrect=
  downPred.filter(
   e=>e.result==='DOWN'
  ).length;

 const probPredCorrect=
  resolved.filter(
   e=>(
    (
     e.upProb||0
    )>=(
     e.downProb||0
    )
     ?'UP'
     :'DOWN'
   )===e.result
  ).length;

 if($('sniperWins'))
  $('sniperWins').textContent=
   String(wins);

 if($('sniperLosses'))
  $('sniperLosses').textContent=
   String(losses);

 if($('sniperWinRate'))
  $('sniperWinRate').textContent=
   totalResolved
    ?(
     wins/
     totalResolved*
     100
    ).toFixed(1)+'%'
    :'—';

 if($('probAccuracy'))
  $('probAccuracy').textContent=
   totalResolved
    ?(
     probPredCorrect/
     totalResolved*
     100
    ).toFixed(1)+'%'
    :'—';

 if($('upProbAccuracy'))
  $('upProbAccuracy').textContent=
   upPred.length
    ?`${upCorrect}/${upPred.length} • ${
     (
      upCorrect/
      upPred.length*
      100
     ).toFixed(1)
    }%`
    :'—';

 if($('downProbAccuracy'))
  $('downProbAccuracy').textContent=
   downPred.length
    ?`${downCorrect}/${downPred.length} • ${
     (
      downCorrect/
      downPred.length*
      100
     ).toFixed(1)
    }%`
    :'—';


 /* =======================================================
    ENTRY HISTORY
    ======================================================= */

 const log=$('entryHistory');

 if(log){

  log.innerHTML=
   historyEntries.length
    ?historyEntries.map(
     h=>`
      <div class="historyRow">
       <b>${h.stage} ${h.side}</b>
       <span>${fmtTime(h.generatedAt)} → ${fmtTime(h.marketEnd)}</span>
       <span>${
        h.result
         ?`RESULT ${h.result} • ${
          h.correct
           ?'✓ HIT'
           :'✕ MISS'
         }`
         :'PENDING'
       }</span>
      </div>
     `
    ).join('')
    :'<div class="historyEmpty">No confirmed sniper entries yet.</div>';
 }

 renderRecentResults();
}


/* =========================================================
   BTC CHART
   ========================================================= */

function drawBTCChart(){

 const c=$('btcChart');

 if(!c)
  return;

 const dpr=
  window.devicePixelRatio||1;

 const rect=
  c.getBoundingClientRect();

 const w=
  Math.max(
   300,
   rect.width||300
  );

 const h=150;

 if(
  c.width!==Math.round(w*dpr)||
  c.height!==Math.round(h*dpr)
 ){

  c.width=
   Math.round(w*dpr);

  c.height=
   Math.round(h*dpr);
 }

 const ctx=
  c.getContext('2d');

 if(!ctx)
  return;

 ctx.setTransform(
  dpr,
  0,
  0,
  dpr,
  0,
  0
 );

 ctx.clearRect(
  0,
  0,
  w,
  h
 );

 const pts=
  hist
   .slice(-180)
   .map(z=>z.p)
   .filter(Number.isFinite);

 if(pts.length<2)
  return;

 const lo=Math.min(...pts);
 const hi=Math.max(...pts);

 const pad=
  Math.max(
   (hi-lo)*.15,
   hi*.00001
  );

 const min=lo-pad;
 const max=hi+pad;

 ctx.strokeStyle='#172231';
 ctx.lineWidth=1;

 for(let i=1;i<4;i++){

  const y=i*h/4;

  ctx.beginPath();
  ctx.moveTo(0,y);
  ctx.lineTo(w,y);
  ctx.stroke();
 }

 const grad=
  ctx.createLinearGradient(
   0,
   0,
   0,
   h
  );

 grad.addColorStop(
  0,
  'rgba(53,229,138,.28)'
 );

 grad.addColorStop(
  1,
  'rgba(53,229,138,0)'
 );

 ctx.beginPath();

 pts.forEach((v,i)=>{

  const x=
   i/
   (pts.length-1)*
   w;

  const y=
   h-
   (v-min)/
   (max-min)*
   h;

  if(i)
   ctx.lineTo(x,y);
  else
   ctx.moveTo(x,y);
 });

 ctx.lineTo(w,h);
 ctx.lineTo(0,h);
 ctx.closePath();

 ctx.fillStyle=grad;
 ctx.fill();

 ctx.beginPath();

 pts.forEach((v,i)=>{

  const x=
   i/
   (pts.length-1)*
   w;

  const y=
   h-
   (v-min)/
   (max-min)*
   h;

  if(i)
   ctx.lineTo(x,y);
  else
   ctx.moveTo(x,y);
 });

 ctx.strokeStyle='#35e58a';
 ctx.lineWidth=2;
 ctx.stroke();
}


/* =========================================================
   MARKET RESULTS
   ========================================================= */

function resultFromMarket(m){

 const prices=
  parseArr(
   m?.outcomePrices||
   m?.outcome_prices
  ).map(Number);

 const outs=
  parseArr(
   m?.outcomes
  ).map(
   x=>
    String(x)
     .trim()
     .toUpperCase()
  );

 const winner=
  String(
   m?.winningOutcome||
   m?.winner||
   m?.resolution||
   ''
  ).toUpperCase();

 if(winner.includes('UP'))
  return'UP';

 if(winner.includes('DOWN'))
  return'DOWN';

 if(prices.length===2){

  const idx=
   prices.findIndex(
    v=>v>=.999
   );

  if(idx>=0){

   const o=
    outs[idx]||'';

   return o.includes('DOWN')
    ?'DOWN'
    :'UP';
  }

  if(
   m?.closed===true&&
   Math.max(...prices)>=.98
  ){

   const winnerIndex=
    prices[0]>prices[1]
     ?0
     :1;

   return(
    outs[winnerIndex]||''
   ).includes('DOWN')
    ?'DOWN'
    :'UP';
  }
 }

 return null;
}


async function fetchMarketBySlug(slug){

 const urls=[

  `https://gamma-api.polymarket.com/markets/slug/${encodeURIComponent(slug)}`,

  `https://gamma-api.polymarket.com/markets?slug=${encodeURIComponent(slug)}`

 ];

 for(const url of urls){

  try{

   const r=
    await fetch(
     url,
     {
      cache:'no-store',
      headers:{
       Accept:'application/json'
      }
     }
    );

   if(!r.ok)
    continue;

   const j=await r.json();

   const m=
    Array.isArray(j)
     ?j[0]
     :(j?.data||[])[0]||j;

   if(m)
    return m;

  }catch{}
 }

 return null;
}


async function loadRecentResults(){

 const now=
  Math.floor(
   Date.now()/1000
  );

 const base=
  now-
  now%300;

 const arr=[];

 for(
  let i=1;
  i<=12&&arr.length<10;
  i++
 ){

  const ts=
   base-i*300;

  const slug=
   `btc-updown-5m-${ts}`;

  const m=
   await fetchMarketBySlug(slug);

  if(!m)
   continue;

  const result=
   resultFromMarket(m);

  const end=
   Math.floor(
    new Date(
     m.endDate||
     m.endDateIso||
     0
    ).getTime()/1000
   );

  if(
   result&&
   end&&
   end<=now
  ){

   arr.push({
    result,
    end,
    slug
   });
  }
 }

 recentResults=
  arr.slice(0,10);
}


function renderRecentResults(){

 const el=$('recentResults');

 if(!el)
  return;

 el.innerHTML=
  recentResults.length
   ?recentResults.map(
    r=>`
     <span
      class="resultBall ${
       r.result==='UP'
        ?'resultUp'
        :'resultDown'
      }"
      title="${
       r.result
      } • ${
       fmtTime(r.end*1000)
      }"
     ></span>
    `
   ).join('')
   :'<span class="historyEmpty">No resolved 5M results yet.</span>';
}


/* =========================================================
   LOCAL STORAGE / CALIBRATION
   ========================================================= */

function entryKey(h){

 return `${
  h.marketEnd||0
 }|${
  h.generatedAt||0
 }|${
  h.side||''
 }`;
}


async function loadCalibration(){

 try{

  const raw=
   localStorage.getItem(
    'btc5mSniper'
   )||'{}';

  const z=
   JSON.parse(raw);

  sectionVisibility={
   ...sectionVisibility,
   ...(z.sectionVisibility||{})
  };

  storedEntries=
   Array.isArray(
    z.sniperEntries
   )
    ?z.sniperEntries
    :[];

  if(
   z.sniperCalibration&&
   typeof z.sniperCalibration==='object'
  ){

   calibration={
    bins:{},
    total:0,
    correct:0,
    brierSum:0,
    ...z.sniperCalibration
   };

   if(
    !calibration.bins||
    typeof calibration.bins!=='object'
   )
    calibration.bins={};
  }

  historyEntries.splice(
   0,
   historyEntries.length,
   ...storedEntries
    .filter(
     x=>x.stage==='ENTRY'
    )
    .slice(-30)
    .reverse()
  );

 }catch{}
}


async function saveCalibration(){

 try{

  const existing=
   JSON.parse(
    localStorage.getItem(
     'btc5mSniper'
    )||'{}'
   );

  localStorage.setItem(
   'btc5mSniper',
   JSON.stringify({
    ...existing,

    sniperEntries:
     storedEntries.slice(-200),

    sniperCalibration:
     calibration
   })
  );

 }catch{}
}


async function saveVisibility(){

 try{

  const existing=
   JSON.parse(
    localStorage.getItem(
     'btc5mSniper'
    )||'{}'
   );

  localStorage.setItem(
   'btc5mSniper',
   JSON.stringify({
    ...existing,
    sectionVisibility
   })
  );

 }catch{}
}


function probabilityBin(p){

 return Math.max(
  0,
  Math.min(
   9,
   Math.floor(
    clamp(
     N(p),
     0,
     .999
    )/.1
   )
  )
 );
}


function calibratedProbability(raw){

 const b=
  calibration.bins?.[
   probabilityBin(raw)
  ];

 if(
  !b||
  b.n<5
 )
  return raw;

 const empirical=
  b.hits/
  b.n;

 const w=
  Math.min(
   .45,
   b.n/40*.45
  );

 return clamp(
  raw*(1-w)+
  empirical*w,
  .01,
  .99
 );
}


function brier(){

 return calibration.total
  ?calibration.brierSum/
   calibration.total
  :null;
}


/* =========================================================
   SETTLE PENDING
   ========================================================= */

async function settlePending(){

 const now=Date.now();

 let changed=false;

 for(const e of storedEntries){

  if(
   e.result||
   !e.marketEnd||
   e.marketEnd>now||
   !e.slug
  )
   continue;

  try{

   const m=
    await fetchMarketBySlug(
     e.slug
    );

   if(!m)
    continue;

   const result=
    resultFromMarket(m);

   if(!result)
    continue;

   e.result=result;

   e.correct=
    e.side===result;

   const prob=
    e.side==='UP'
     ?N(e.upProb)
     :N(e.downProb);

   const bin=
    probabilityBin(prob);

   calibration.bins[bin]=
    calibration.bins[bin]||
    {
     n:0,
     hits:0
    };

   calibration.bins[bin].n++;

   calibration.bins[bin].hits+=
    e.correct?1:0;

   calibration.total++;

   calibration.correct+=
    e.correct?1:0;

   calibration.brierSum+=
    (
     prob-
     (e.correct?1:0)
    )**2;

   changed=true;

  }catch{}
 }

 if(changed){

  historyEntries.splice(
   0,
   historyEntries.length,
   ...storedEntries
    .filter(
     x=>x.stage==='ENTRY'
    )
    .slice(-30)
    .reverse()
  );

  await saveCalibration();
 }
}


async function persistEntry(h){

 const id=entryKey(h);

 if(
  storedEntries.some(
   x=>x.id===id
  )
 )
  return;

 const row={
  id,

  stage:h.stage,
  side:h.side,

  confidence:h.confidence,

  upProb:N(h.upProb),
  downProb:N(h.downProb),

  generatedAt:h.generatedAt,
  marketEnd:h.marketEnd,

  slug:
   h.slug||
   market?.slug||
   '',

  marketKey:
   h.marketKey||
   market?.slug||
   String(
    market?.endTs||''
   ),

  ask:N(h.ask),
  potential:N(h.potential),
  edge:N(h.edge),

  result:null,
  correct:null
 };

 storedEntries.push(row);

 storedEntries=
  storedEntries.slice(-200);

 historyEntries.unshift(row);

 historyEntries.splice(30);

 await saveCalibration();
}


/* =========================================================
   MARKET / UI CONTROLS
   ========================================================= */

let marketDiscoveryPromise=null;

async function ensureMarket(){

 if(
  market&&
  market.endTs>
   Date.now()/1000+2
 )
  return market;

 if(marketDiscoveryPromise)
  return marketDiscoveryPromise;

 marketDiscoveryPromise=
  (async()=>{

   try{

    await discoverDirect();

    return market;

   }catch(e){

    if($('status'))
     $('status').textContent=
      'Market discovery: '+
      (
       e?.message||
       'failed'
      );

    return null;

   }finally{

    marketDiscoveryPromise=null;
   }

  })();

 return marketDiscoveryPromise;
}


if($('refresh'))
 $('refresh').onclick=async()=>{

  market=null;

  books.clear();

  comparisonHist.length=0;

  lastComparisonSnapshot=0;

  signalState={
   stage:'WAIT',
   side:'WAIT',
   confidence:50,
   generatedAt:Date.now(),
   validUntil:Date.now()+1500,
   potential:0,
   edge:0,
   lockedAsk:0,
   reason:'Refreshing market'
  };

  await ensureMarket();

  render();
 };


if($('clear'))
 $('clear').onclick=()=>{

  historyEntries.length=0;

  render();
 };


if($('statsToggle'))
 $('statsToggle').onclick=()=>{

  const e=$('statsDetails');
  const b=$('statsToggle');

  if(!e||!b)
   return;

  e.classList.toggle('show');

  b.textContent=
   e.classList.contains('show')
    ?'HIDE STATS'
    :'SHOW STATS';
 };


if($('modeSelect'))
 $('modeSelect').onchange=e=>{

  selectedMode=
   e.target.value;

  candidateSince=0;
  lastCandidateKey='';

  signalState={
   stage:'WATCH',
   side:'WAIT',
   confidence:50,
   generatedAt:Date.now(),
   validUntil:Date.now()+1000,
   potential:0,
   edge:0,
   lockedAsk:0,
   reason:'Mode changed'
  };

  render();
 };


document
 .querySelectorAll(
  '.hideToggle'
 )
 .forEach(btn=>
  btn.onclick=async()=>{

   const target=
    btn.dataset.target;

   const el=$(target);

   if(!el)
    return;

   el.classList.toggle(
    'sectionHidden'
   );

   const hidden=
    el.classList.contains(
     'sectionHidden'
    );

   btn.textContent=
    hidden
     ?'SHOW'
     :'HIDE';

   sectionVisibility[target]=
    !hidden;

   await saveVisibility();
  }
 );


function applyVisibility(){

 Object.entries(
  sectionVisibility
 ).forEach(
  ([id,visible])=>{

   const el=$(id);

   const btn=
    document.querySelector(
     `.hideToggle[data-target="${id}"]`
    );

   if(!el||!btn)
    return;

   el.classList.toggle(
    'sectionHidden',
    visible===false
   );

   btn.textContent=
    visible===false
     ?'SHOW'
     :'HIDE';
  }
 );
}


/* =========================================================
   INITIALIZATION
   ========================================================= */

(async()=>{

 await loadCalibration();

 applyVisibility();

 await settlePending();

 await loadRecentResults();

 startBinance();

 await ensureMarket();

 render();

 setInterval(()=>{

  ensureMarket()
   .then(()=>{
    render();
   })
   .catch(()=>{
    render();
   });

 },250);

 setInterval(()=>{

  if(
   market?.upToken&&
   market?.downToken
  ){

   restPoly(
    market.upToken
   );

   restPoly(
    market.downToken
   );
  }

 },1000);

 setInterval(()=>{

  settlePending()
   .then(render)
   .catch(()=>{});

 },30000);

 setInterval(()=>{

  loadRecentResults()
   .then(render)
   .catch(()=>{});

 },60000);

})();


if($('tradeMode'))
 $('tradeMode').onchange=e=>{

  tradeMode=e.target.value;

  render();
 };

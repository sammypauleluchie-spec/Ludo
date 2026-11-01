const http=require('http'),fs=require('fs'),path=require('path');
const {WebSocketServer}=require('ws');
const page=fs.readFileSync(path.join(__dirname,'index.html'));
const server=http.createServer((q,r)=>{r.writeHead(200,{'content-type':'text/html; charset=utf-8'});r.end(page)});
const wss=new WebSocketServer({server});
const rooms=new Map();
const list=room=>[...room].map(w=>({peer:w.id,presence:w.presence}));
const bcast=(room,msg)=>{const s=JSON.stringify(msg);for(const w of room)if(w.readyState===1)w.send(s)};
wss.on('connection',ws=>{
  ws.id=Math.random().toString(36).slice(2,10);ws.presence={};ws.room=null;ws.code=null;ws.on('error',()=>{});
  ws.on('message',raw=>{
    let m;try{m=JSON.parse(raw)}catch{return}
    if(m.t==='join'&&!ws.room){
      ws.code=String(m.room||'').slice(0,20);
      if(!rooms.has(ws.code))rooms.set(ws.code,new Set());
      ws.room=rooms.get(ws.code);
            if(ws.room.size>=8)return ws.close();
      const cid=String(m.cid||'').replace(/[^a-z0-9]/gi,'').slice(0,16);
      if(cid){ws.id=cid;for(const w of [...ws.room])if(w.id===cid){ws.room.delete(w);w.room=null;try{w.close()}catch{}}}
      ws.room.add(ws);
      ws.send(JSON.stringify({t:'hi',peer:ws.id}));
      bcast(ws.room,{t:'peers',peers:list(ws.room)});
    }else if(!ws.room)return;
    else if(m.t==='presence'&&m.patch&&typeof m.patch==='object'){
      ws.presence={...ws.presence,...m.patch};
      bcast(ws.room,{t:'peers',peers:list(ws.room)});
    }else if(m.t==='emit'&&typeof m.topic==='string'){
      bcast(ws.room,{t:'msg',topic:m.topic,data:m.data,peer:ws.id});
    }
  });
  ws.on('close',()=>{
    if(!ws.room)return;
    ws.room.delete(ws);
    if(!ws.room.size)rooms.delete(ws.code);else bcast(ws.room,{t:'peers',peers:list(ws.room)});
  });
});
server.listen(process.env.PORT||3000,()=>console.log('Ludo server running'));

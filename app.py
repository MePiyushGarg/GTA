import asyncio, json, os, sqlite3, time
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import core
app = FastAPI()
os.makedirs("data/out", exist_ok=True)
app.mount("/media", StaticFiles(directory="data/out"), name="media")
SECRET = ("KEY", "SECRET", "TOKEN")

@app.on_event("startup")
async def up():
    core.init(); asyncio.create_task(core.supervisor()); asyncio.create_task(core.worker())

@app.get("/")
def index(): return FileResponse("static/index.html")

@app.get("/api/channels")
def chans(): return core.q("select * from channels")
@app.post("/api/channels")
async def add(r: Request):
    d = await r.json()
    core.q("insert into channels(name,platform,handle,url,enabled) values(?,?,?,?,1)", (d["name"], d["platform"], d["handle"], d["url"]))
    return {"ok": 1}
@app.put("/api/channels/{i}")
async def edit(i: int, r: Request):
    d = await r.json()
    core.q("update channels set name=?,platform=?,handle=?,url=?,enabled=? where id=?", (d["name"], d["platform"], d["handle"], d["url"], int(d["enabled"]), i))
    return {"ok": 1}
@app.delete("/api/channels/{i}")
def rm(i: int): core.q("delete from channels where id=?", (i,)); return {"ok": 1}

@app.get("/api/state")
def state():
    day = time.time() - 86400
    return {"jobs": core.q("select id,channel,stage,status,progress,error,data,updated from jobs order by id desc limit 30"),
            "live": core.LIVE, "stats": {
        "today": core.q("select count(*) c from jobs where stage='Published' and updated>?", (day,))[0]["c"],
        "total": core.q("select count(*) c from jobs where stage='Published'")[0]["c"],
        "queued": core.q("select count(*) c from jobs where status in ('queued','retry')")[0]["c"],
        "live": len(core.LIVE)}}

@app.get("/api/settings")
def gs():
    out = {}
    for k in core.KEYS:
        v = core.S(k); out[k] = ("•••• set" if v else "") if k.endswith(SECRET) else v
    return out
@app.post("/api/settings")
async def ss(r: Request):
    for k, v in (await r.json()).items():
        if k in core.KEYS and not str(v).startswith("••••"): core.q("insert or replace into settings values(?,?)", (k, v))
    return {"ok": 1}

@app.get("/api/layout")
def gl(channel_id: int = 0):
    if channel_id:
        row = core.q("select layout from channels where id=?", (channel_id,))
        if row and row[0]["layout"]: return json.loads(row[0]["layout"])
    return core.layout(None)
@app.post("/api/layout")
async def sl(r: Request):
    d = await r.json()
    if d.get("channel_id"): core.q("update channels set layout=? where id=?", (json.dumps(d["layout"]), d["channel_id"]))
    else: core.q("insert or replace into settings values('layout',?)", (json.dumps(d["layout"]),))
    return {"ok": 1}

@app.get("/events")
async def events():
    async def gen():
        qu = asyncio.Queue(); core.SUBS.append(qu)
        try:
            for e in core.RECENT[-50:]: yield f"data: {json.dumps(e)}\n\n"
            while True: yield f"data: {json.dumps(await qu.get())}\n\n"
        finally: core.SUBS.remove(qu)
    return StreamingResponse(gen(), media_type="text/event-stream")

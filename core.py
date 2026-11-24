import asyncio, base64, collections, glob, json, os, re, shlex, sqlite3, subprocess, time, httpx
KEYS = ["PUBLIC_BASE_URL","ANTHROPIC_API_KEY","AI_MODEL","WHISPER_MODEL","YT_CLIENT_ID","YT_CLIENT_SECRET","YT_REFRESH_TOKEN","IG_USER_ID","IG_ACCESS_TOKEN","TT_CLIENT_KEY","TT_CLIENT_SECRET","TT_REFRESH_TOKEN","TT_PRIVACY","TWITCH_CLIENT_ID","HYPE_THRESHOLD","CLIP_PRE","CLIP_POST","CLIP_COOLDOWN","POLL_SECONDS","UPLOAD_GAP_SECONDS"]
STAGES = ["Monitoring","Live Detected","Hype Found","Clipping","AI Analyzing","Rendering 9:16","Uploading","Published"]
DEF_LAYOUT = {"split":0.667,"game":{"x":0,"y":0,"w":1,"h":1},"face":{"x":0.7,"y":0.62,"w":0.3,"h":0.38},
 "title":{"font":"DejaVu Sans","size":72,"color":"#FFFFFF","box":True,"x":0.5,"y":0.08},
 "cap":{"font":"DejaVu Sans","size":84,"hl":"#FFE600","stroke":"#000000","outline":6,"x":0.5,"y":0.62}}
SUBS, RECENT, LIVE, CYCLES = [], [], {}, [0]
class Defer(Exception):
    def __init__(s, secs): s.secs = secs

def conn():
    c = sqlite3.connect("data/db.sqlite", timeout=30); c.row_factory = sqlite3.Row; return c
def q(sql, a=()):
    with conn() as c:
        cur = c.execute(sql, a); return [dict(r) for r in cur.fetchall()] if cur.description else []
def init():
    os.makedirs("data/buf", exist_ok=True)
    q("create table if not exists channels(id integer primary key,name,platform,handle,url,enabled,layout)")
    q("create table if not exists jobs(id integer primary key,channel_id,channel,stage,status,progress,error,data,attempts default 0,next_at default 0,updated)")
    q("create table if not exists settings(k primary key,v)")
    q("update jobs set status='queued' where status='running'")  # crash recovery
def S(k, d=""):
    r = q("select v from settings where k=?", (k,))
    return (r[0]["v"] if r and r[0]["v"] != "" else os.environ.get(k, d))
def layout(cid):
    r = q("select v from settings where k='layout'")
    return json.loads(r[0]["v"]) if r else DEF_LAYOUT
def log(msg, kind="info"):
    e = {"t": time.strftime("%H:%M:%S"), "kind": kind, "msg": msg}; RECENT.append(e); del RECENT[:-300]
    print(f"{e['t']} [{kind}] {msg}", flush=True)
    for s in SUBS: s.put_nowait(e)
def jset(i, **kw):
    kw["updated"] = time.time()
    q(f"update jobs set {','.join(k+'=?' for k in kw)} where id=?", (*kw.values(), i))

# ---------- live monitoring ----------
async def sh(cmd, timeout=None):
    p = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    o, e = await asyncio.wait_for(p.communicate(), timeout); return p.returncode, o.decode(), e.decode()
async def is_live(ch):
    try:
        rc, o, _ = await sh(f"streamlink --json {shlex.quote(ch['url'])}", 40)
        return rc == 0 and '"streams"' in o
    except Exception: return False

async def supervisor():
    while True:
        try:
            for ch in q("select * from channels where enabled=1"):
                if ch["id"] not in LIVE and await is_live(ch):
                    LIVE[ch["id"]] = {"name": ch["name"], "since": time.time(), "hype": 0}
                    log(f"🔴 {ch['name']} is LIVE on {ch['platform']} — attaching recorder + chat", "live")
                    asyncio.create_task(watch(ch))
        except Exception as e: log(f"supervisor error: {e}", "err")
        CYCLES[0] += 1
        await asyncio.sleep(int(S("POLL_SECONDS", "45")))

async def watch(ch):
    d = f"data/buf/{ch['id']}"; os.makedirs(d, exist_ok=True)
    rec = await asyncio.create_subprocess_shell(
        f"streamlink -O {shlex.quote(ch['url'])} best | ffmpeg -loglevel error -i - -c copy -f segment -segment_time 6 -strftime 1 -reset_timestamps 1 {d}/%s.ts")
    msgs = collections.deque(); audio = collections.deque(maxlen=30); last_clip = [0]
    def on_msg(t): msgs.append((time.time(), t))
    chat = asyncio.create_task(chat_listener(ch, on_msg))
    try:
        while rec.returncode is None:
            await asyncio.sleep(2); now = time.time()
            while msgs and msgs[0][0] < now - 10: msgs.popleft()
            txt = [m[1] for m in msgs]; n = len(txt)
            kw = sum(len(re.findall(r"clip it|\bW\b|no way|crazy|lmao|wtf|pog|omg|insane", t, re.I)) for t in txt)
            caps = sum(1 for t in txt if len(t) > 4 and sum(c.isupper() for c in t) / len(t) > .7)
            score = (n + 2 * kw + caps) / 10
            segs = sorted(glob.glob(f"{d}/*.ts"))
            for old in segs[:-60]: os.remove(old)  # ~6 min rolling buffer
            if len(segs) > 2:
                rc, _, e = await sh(f"ffmpeg -i {segs[-2]} -af volumedetect -f null - 2>&1 | grep mean_volume")
                m = re.search(r"mean_volume: (-?[\d.]+)", e + _)
                if m:
                    v = float(m.group(1))
                    if len(audio) > 5 and v > sum(audio) / len(audio) + 6: score += 2
                    audio.append(v)
            LIVE[ch["id"]]["hype"] = round(score, 2)
            if score >= float(S("HYPE_THRESHOLD", "3")) and now - last_clip[0] > int(S("CLIP_COOLDOWN", "90")):
                last_clip[0] = now
                log(f"🔥 Hype spike {score:.1f} on {ch['name']} ({n} msgs/10s)", "hype")
                q("insert into jobs(channel_id,channel,stage,status,progress,data,updated) values(?,?,?,?,?,?,?)",
                  (ch["id"], ch["name"], "Hype Found", "queued", 10, json.dumps({"peak": now}), now))
    finally:
        chat.cancel(); LIVE.pop(ch["id"], None); log(f"⚫ {ch['name']} went offline", "live")

async def chat_listener(ch, on_msg):
    while True:
        try:
            if ch["platform"].lower() == "twitch":
                import websockets
                async with websockets.connect("wss://irc-ws.chat.twitch.tv:443") as ws:
                    await ws.send(f"NICK justinfan{int(time.time())%99999}\r\nJOIN #{ch['handle'].lstrip('@').lower()}")
                    async for m in ws:
                        for line in m.split("\r\n"):
                            if line.startswith("PING"): await ws.send("PONG :tmi.twitch.tv")
                            elif "PRIVMSG" in line: on_msg(line.split("PRIVMSG", 1)[1].split(":", 1)[1])
            elif ch["platform"].lower() == "youtube":
                import pytchat
                vid = (await sh(f"yt-dlp --get-id {shlex.quote(ch['url'])}", 40))[1].strip()
                c = pytchat.create(video_id=vid)
                while c.is_alive():
                    for it in c.get().sync_items(): on_msg(it.message)
                    await asyncio.sleep(1)
            else: await asyncio.sleep(3600)  # Kick: audio-only hype (no stable public chat API)
        except asyncio.CancelledError: raise
        except Exception: await asyncio.sleep(10)

# ---------- job worker ----------
async def worker():
    while True:
        j = q("select * from jobs where status in ('queued','retry') and next_at<=? order by id limit 1", (time.time(),))
        if not j: await asyncio.sleep(2); continue
        j = j[0]; jset(j["id"], status="running")
        try: await run_job(j)
        except Defer as d:
            jset(j["id"], status="retry", next_at=time.time() + d.secs); log(f"⏳ job {j['id']} deferred {d.secs}s (rate limit/spacing)", "warn")
        except Exception as e:
            a = j["attempts"] + 1
            if a >= 6: jset(j["id"], status="failed", error=str(e)[:300], attempts=a); log(f"❌ job {j['id']} failed: {e}", "err")
            else: jset(j["id"], status="retry", error=str(e)[:300], attempts=a, next_at=time.time() + min(30 * 2 ** a, 1800)); log(f"↻ job {j['id']} retry {a}: {e}", "warn")

async def run_job(j):
    i, d = j["id"], json.loads(j["data"]); ch = q("select * from channels where id=?", (j["channel_id"],))[0]
    st = lambda s, p: (jset(i, stage=s, progress=p, data=json.dumps(d)), log(f"[{ch['name']}] {s}", "step"))
    w = f"data/work/{i}"; os.makedirs(w, exist_ok=True)
    if "clip" not in d:
        st("Clipping", 20)
        await asyncio.sleep(max(0, d["peak"] + int(S("CLIP_POST", "15")) + 8 - time.time()))
        lo, hi = d["peak"] - int(S("CLIP_PRE", "30")), d["peak"] + int(S("CLIP_POST", "15"))
        segs = [s for s in sorted(glob.glob(f"data/buf/{ch['id']}/*.ts")) if lo - 6 <= int(os.path.basename(s)[:-3]) <= hi]
        if not segs: raise RuntimeError("buffer empty")
        open(f"{w}/l.txt", "w").write("".join(f"file '{os.path.abspath(s)}'\n" for s in segs))
        rc, _, e = await sh(f"ffmpeg -y -loglevel error -f concat -safe 0 -i {w}/l.txt -c copy {w}/clip.mp4")
        if rc: raise RuntimeError(e)
        d["clip"] = f"{w}/clip.mp4"
    if "meta" not in d:
        st("AI Analyzing", 40); d["words"] = await asyncio.to_thread(transcribe, d["clip"])
        d["meta"] = await asyncio.to_thread(analyze, d["clip"], d["words"], ch, w)
    if "final" not in d:
        st("Rendering 9:16", 65)
        L = json.loads(ch["layout"]) if ch["layout"] else layout(None)
        open(f"{w}/c.ass", "w").write(make_ass(d["words"], d["meta"]["hook"], L))
        H1 = round(1920 * L["split"] / 2) * 2; H2 = 1920 - H1; g, f = L["game"], L["face"]
        cr = lambda r: f"crop=iw*{r['w']}:ih*{r['h']}:iw*{r['x']}:ih*{r['y']}"
        vf = (f"[0:v]split[a][b];[a]{cr(g)},scale=1080:{H1}:force_original_aspect_ratio=increase,crop=1080:{H1}[g];"
              f"[b]{cr(f)},scale=1080:{H2}:force_original_aspect_ratio=increase,crop=1080:{H2}[f];[g][f]vstack,fps=60,ass={w}/c.ass[v]")
        out = f"data/out/{i}.mp4"
        rc, _, e = await sh(f'ffmpeg -y -loglevel error -i {d["clip"]} -filter_complex "{vf}" -map "[v]" -map 0:a? -c:v libx264 -preset veryfast -crf 20 -pix_fmt yuv420p -c:a aac -b:a 160k {out}')
        if rc: raise RuntimeError(e[-300:])
        d["final"] = out
    st("Uploading", 85); d.setdefault("pub", {})
    credit = f"Credit: {ch['handle']} on {ch['platform']} — {ch['url']}"
    tags = " ".join(d["meta"]["hashtags"])
    full = f"{d['meta']['description']}\n\n{credit}\n{tags}"
    assert credit in full  # mandatory credit guard
    gap = int(S("UPLOAD_GAP_SECONDS", "600"))
    for name, fn, ready in [("youtube", up_yt, S("YT_REFRESH_TOKEN")), ("instagram", up_ig, S("IG_ACCESS_TOKEN")), ("tiktok", up_tt, S("TT_REFRESH_TOKEN"))]:
        if name in d["pub"] or not ready: continue
        last = q("select v from settings where k=?", ("last_" + name,)); wait = gap - (time.time() - float(last[0]["v"])) if last else 0
        if wait > 0: raise Defer(int(wait) + 1)
        d["pub"][name] = await fn(d["final"], d["meta"], full, i)
        q("insert or replace into settings values(?,?)", ("last_" + name, str(time.time()))); jset(i, data=json.dumps(d))
        log(f"📤 {name}: {d['pub'][name]}", "up")
    st("Published", 100); jset(i, status="done")

# ---------- AI ----------
_wm = None
def transcribe(p):
    global _wm
    from faster_whisper import WhisperModel
    _wm = _wm or WhisperModel(S("WHISPER_MODEL", "small"), compute_type="int8")
    segs, _ = _wm.transcribe(p, word_timestamps=True)
    return [{"w": w.word.strip(), "s": w.start, "e": w.end} for s in segs for w in s.words]
def analyze(p, words, ch, w):
    import anthropic
    subprocess.run(f"ffmpeg -y -loglevel error -i {p} -vf fps=1/6,scale=640:-1 -frames:v 5 {w}/f%d.jpg", shell=True)
    imgs = [{"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": base64.b64encode(open(f, "rb").read()).decode()}} for f in sorted(glob.glob(f"{w}/f*.jpg"))]
    txt = " ".join(x["w"] for x in words)[:3000]
    r = anthropic.Anthropic(api_key=S("ANTHROPIC_API_KEY")).messages.create(model=S("AI_MODEL", "claude-sonnet-5-5"), max_tokens=800, messages=[{"role": "user", "content": imgs + [{"type": "text", "text":
        f"GTA 6 stream clip from {ch['name']}. Transcript: {txt}\nReturn ONLY JSON: {{\"title\":high-CTR <90 chars,\"description\":2 sentences,\"hook\":<=6 word on-screen hook,\"hashtags\":[8 trending GTA6 hashtags]}}. Describe only what you actually see/hear."}]}])
    m = json.loads(re.search(r"\{.*\}", r.content[0].text, re.S).group(0)); m["caption"] = m["title"]; return m

def make_ass(words, hook, L):
    col = lambda h: "&H00" + h[5:7] + h[3:5] + h[1:3] + "&"
    t, c = L["title"], L["cap"]; ts = lambda x: f"0:{int(x)//60:02d}:{x%60:05.2f}"
    o = ["[Script Info]", "PlayResX: 1080", "PlayResY: 1920", "[V4+ Styles]",
         "Format: Name,Fontname,Fontsize,PrimaryColour,OutlineColour,BackColour,Bold,BorderStyle,Outline,Shadow,Alignment,MarginL,MarginR,MarginV",
         f"Style: T,{t['font']},{t['size']},{col(t['color'])},&H00000000&,&H99000000&,1,{3 if t['box'] else 1},{8 if t['box'] else 3},0,5,60,60,0",
         f"Style: C,{c['font']},{c['size']},&H00FFFFFF&,{col(c['stroke'])},&H00000000&,1,1,{c['outline']},2,5,60,60,0",
         "[Events]", "Format: Layer,Start,End,Style,Name,MarginL,MarginR,MarginV,Effect,Text",
         f"Dialogue: 0,{ts(0)},{ts(3.5)},T,,0,0,0,,{{\\pos({int(t['x']*1080)},{int(t['y']*1920)})}}{hook.upper()}"]
    for g in range(0, len(words), 4):
        grp = words[g:g + 4]
        for k, wd in enumerate(grp):
            end = grp[k + 1]["s"] if k + 1 < len(grp) else wd["e"] + .15
            txt = " ".join((f"{{\\c{col(c['hl'])}\\fscx115\\fscy115}}{x['w'].upper()}{{\\c&H00FFFFFF&\\fscx100\\fscy100}}" if n == k else x["w"].upper()) for n, x in enumerate(grp))
            o.append(f"Dialogue: 0,{ts(wd['s'])},{ts(end)},C,,0,0,0,,{{\\pos({int(c['x']*1080)},{int(c['y']*1920)})}}{txt}")
    return "\n".join(o)

# ---------- uploaders ----------
async def up_yt(path, m, desc, i):
    def go():
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
        from googleapiclient.errors import HttpError
        cr = Credentials(None, refresh_token=S("YT_REFRESH_TOKEN"), client_id=S("YT_CLIENT_ID"), client_secret=S("YT_CLIENT_SECRET"), token_uri="https://oauth2.googleapis.com/token")
        try:
            r = build("youtube", "v3", credentials=cr).videos().insert(part="snippet,status", body={"snippet": {"title": (m["title"] + " #Shorts")[:100], "description": desc, "categoryId": "20"}, "status": {"privacyStatus": "public", "selfDeclaredMadeForKids": False}}, media_body=MediaFileUpload(path, resumable=True)).execute()
            return "https://youtube.com/shorts/" + r["id"]
        except HttpError as e:
            if e.resp.status in (403, 429) and b"uota" in e.content: raise Defer(3600)
            raise
    return await asyncio.to_thread(go)
async def up_ig(path, m, desc, i):
    base, tok, uid = "https://graph.facebook.com/v21.0", S("IG_ACCESS_TOKEN"), S("IG_USER_ID")
    async with httpx.AsyncClient(timeout=120) as c:
        r = (await c.post(f"{base}/{uid}/media", data={"media_type": "REELS", "video_url": media_url(i, path), "caption": f"{m['hook']}\n\n{desc}"[:2200], "access_token": tok})).json()
        if "id" not in r: raise RuntimeError(f"IG: {r}")
        for _ in range(40):
            s = (await c.get(f"{base}/{r['id']}", params={"fields": "status_code", "access_token": tok})).json()
            if s.get("status_code") == "FINISHED": break
            if s.get("status_code") == "ERROR": raise RuntimeError(f"IG processing: {s}")
            await asyncio.sleep(10)
        p = (await c.post(f"{base}/{uid}/media_publish", data={"creation_id": r["id"], "access_token": tok})).json()
        if "id" not in p:
            if p.get("error", {}).get("code") in (4, 17, 32): raise Defer(1800)
            raise RuntimeError(f"IG publish: {p}")
        return (await c.get(f"{base}/{p['id']}", params={"fields": "permalink", "access_token": tok})).json().get("permalink", p["id"])
async def up_tt(path, m, desc, i):
    async with httpx.AsyncClient(timeout=300) as c:
        t = (await c.post("https://open.tiktokapis.com/v2/oauth/token/", data={"client_key": S("TT_CLIENT_KEY"), "client_secret": S("TT_CLIENT_SECRET"), "grant_type": "refresh_token", "refresh_token": S("TT_REFRESH_TOKEN")})).json()
        if "access_token" not in t: raise RuntimeError(f"TT token: {t}")
        if t.get("refresh_token"): q("insert or replace into settings values('TT_REFRESH_TOKEN',?)", (t["refresh_token"],))
        h = {"Authorization": "Bearer " + t["access_token"]}; data = open(path, "rb").read(); n = len(data)
        r = (await c.post("https://open.tiktokapis.com/v2/post/publish/video/init/", headers=h, json={"post_info": {"title": f"{m['hook']} {desc}"[:2200], "privacy_level": S("TT_PRIVACY", "SELF_ONLY")}, "source_info": {"source": "FILE_UPLOAD", "video_size": n, "chunk_size": n, "total_chunk_count": 1}})).json()
        if r.get("error", {}).get("code") not in ("ok", None): raise Defer(1800) if "rate" in str(r).lower() else RuntimeError(f"TT: {r}")
        await c.put(r["data"]["upload_url"], content=data, headers={"Content-Type": "video/mp4", "Content-Range": f"bytes 0-{n-1}/{n}"})
        return "tiktok publish_id " + r["data"]["publish_id"]

def media_url(i, path):
    if os.environ.get("GITHUB_ACTIONS"):  # no public server on a runner -> host the file as a release asset (repo must be public)
        repo, tag = os.environ["GITHUB_REPOSITORY"], f"clip-{i}-{int(time.time())}"
        subprocess.run(["gh", "release", "create", tag, path, "--repo", repo, "--title", tag, "--notes", "auto-clip"], check=True)
        return f"https://github.com/{repo}/releases/download/{tag}/{os.path.basename(path)}"
    return f"{S('PUBLIC_BASE_URL')}/media/{i}.mp4"

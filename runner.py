"""Scheduled mode: check channels once; if nobody is live, exit. If live, record/clip/publish until the stream ends and jobs drain."""
import asyncio, json, os, time, core
async def main():
    core.init()
    core.q("update jobs set status='failed',error='runner restarted (clip files not persisted)' where status in ('queued','retry','running')")
    for c in json.load(open("channels.json")):
        if c.get("enabled", True) and not core.q("select 1 from channels where url=?", (c["url"],)):
            core.q("insert into channels(name,platform,handle,url,enabled) values(?,?,?,?,1)", (c["name"], c["platform"], c["handle"], c["url"]))
    for ch in core.q("select * from channels"):  # keep DB in sync with channels.json
        if not any(x["url"] == ch["url"] and x.get("enabled", True) for x in json.load(open("channels.json"))): core.q("update channels set enabled=0 where id=?", (ch["id"],))
    deadline = time.time() + float(os.environ.get("MAX_MINUTES", "340")) * 60
    tasks = [asyncio.create_task(core.supervisor()), asyncio.create_task(core.worker())]
    core.log("⏱ scheduled check started", "info")
    while core.CYCLES[0] < 1: await asyncio.sleep(2)
    while time.time() < deadline:
        busy = core.LIVE or core.q("select 1 from jobs where status in ('queued','retry','running')")
        if not busy: break
        await asyncio.sleep(20)
    core.log("✅ nothing live / nothing pending — exiting until next scheduled check" if time.time() < deadline else "⏱ time budget reached — handing over to next run", "info")
    for t in tasks: t.cancel()
asyncio.run(main())

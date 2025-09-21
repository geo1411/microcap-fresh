#!/usr/bin/env python3
import os, sys, time, argparse, pathlib, fcntl
from typing import List, Dict, Any
import requests, pandas as pd
from string import Template

DEX = "https://api.dexscreener.com/latest/dex"

def fetch_pairs_for_chain(query: str, limit: int = 2000, pause: float = 0.6) -> List[Dict[str,Any]]:
    time.sleep(pause)
    r = requests.get(f"{DEX}/search", params={"q": query}, timeout=30)
    r.raise_for_status()
    return (r.json().get("pairs") or [])[:limit]

def pick_created_ms(pair: dict):
    info = pair.get("info") or {}
    liq  = pair.get("liquidity") or {}
    for k in ("pairCreatedAt","createdAt"):
        if pair.get(k): return float(pair[k])
    for k in ("createdAt","listedAt"):
        v = info.get(k)
        if v: return float(v)
    v = liq.get("createdAt")
    return float(v) if v else None

def age_hours_from_ms(ms):
    if ms is None: return None
    now_ms = time.time()*1000
    age_h = (now_ms - float(ms))/3_600_000
    return None if age_h > 24*365*5 else max(0.0, age_h)

def dex_name(dex_id: str, chain_id: str) -> str:
    d=(dex_id or "").lower(); c=(chain_id or "").lower()
    m={"raydium":"Raydium (Solana)","orca":"Orca (Solana)","meteora":"Meteora (Solana)","lifinity":"Lifinity (Solana)","phoenix":"Phoenix (Solana)",
       "uniswapv3":"Uniswap v3","aerodrome":"Aerodrome (Base)","baseswap":"BaseSwap (Base)","pancakeswap-v3":"PancakeSwap v3"}
    if d in m: return m[d]
    if "solana" in c: return "Solana DEX"
    if "base"   in c: return "Base DEX"
    if "eth"    in c: return "Ethereum DEX"
    return "DEX"

def explorer_url(chain_id: str, addr: str | None) -> str:
    if not addr: return ""
    c=(chain_id or "").lower()
    if "solana" in c: return f"https://solscan.io/token/{addr}"
    if "base"   in c: return f"https://basescan.org/token/{addr}"
    if "eth"    in c: return f"https://etherscan.io/token/{addr}"
    if "bsc"    in c: return f"https://bscscan.com/token/{addr}"
    if "arbitrum" in c: return f"https://arbiscan.io/token/{addr}"
    if "polygon" in c: return f"https://polygonscan.com/token/{addr}"
    if "optimism" in c: return f"https://optimistic.etherscan.io/token/{addr}"
    if "avax" in c or "avalanche" in c: return f"https://snowtrace.io/token/{addr}"
    return ""

def socials(info: dict):
    tw=tg=web=""
    if isinstance(info,dict):
        for s in (info.get("socials") or []):
            t=(s.get("type") or "").lower(); u=s.get("url") or ""
            if t=="twitter" and not tw: tw=u
            if t=="telegram" and not tg: tg=u
        webs=info.get("websites") or []
        if isinstance(webs,list) and webs: web=webs[0].get("url") or ""
    return tw,tg,web

def row_from_pair(p: dict) -> dict:
    base = p.get("baseToken") or {}
    info = p.get("info") or {}
    liq  = p.get("liquidity") or {}
    txns = (p.get("txns") or {}).get("h24") or {}
    vol24 = (p.get("volume") or {}).get("h24") or 0
    chg24 = (p.get("priceChange") or {}).get("h24")
    buys  = int(txns.get("buys",0)); sells = int(txns.get("sells",0))
    traders = buys + sells
    age_h = age_hours_from_ms(pick_created_ms(p))
    tw,tg,web = socials(info)
    price = float(p.get("priceUsd") or 0.0)
    fdv   = float(p.get("fdv") or 0.0)
    lp    = float((liq or {}).get("usd") or 0.0)
    return {
        "token_symbol": base.get("symbol") or "",
        "chain": p.get("chainId") or "",
        "contract_address": base.get("address"),
        "pair_address": p.get("pairAddress"),
        "price_usd": price,
        "fdv_usd": fdv,
        "lp_usd": lp,
        "volume_24h_usd": float(vol24 or 0),
        "unique_traders_24h": traders,
        "buys_24h": buys, "sells_24h": sells,
        "chg24_pct": float(chg24) if chg24 not in (None,"") else None,
        "dex": dex_name(p.get("dexId"), p.get("chainId")),
        "trade_url": p.get("url") or "",
        "explorer_url": explorer_url(p.get("chainId"), base.get("address")),
        "twitter_url": tw, "telegram_url": tg, "website_url": web,
        "age_hours": None if age_h is None else round(age_h,2)
    }

def pass_gates(r: dict, cfg: dict):
    age = r.get("age_hours")
    if age is not None and age > cfg["max_age_h"]:
        return False, f"age>{cfg['max_age_h']}h"
    lp = r["lp_usd"]
    if not (cfg["min_lp"] <= lp <= cfg["max_lp"]):
        return False, f"lp${lp:.0f} outside [{cfg['min_lp']},{cfg['max_lp']}]"
    fdv = r["fdv_usd"]
    if fdv>0 and fdv > cfg["max_fdv"]:
        return False, f"fdv>{cfg['max_fdv']}"
    traders = r["unique_traders_24h"]
    if traders < cfg["min_traders_24h"]:
        return False, f"traders24h<{cfg['min_traders_24h']}"
    vliq = (r["volume_24h_usd"]/(lp+1e-9)) if lp>0 else 0.0
    if vliq < cfg["min_vliq"]:
        return False, f"vliq<{cfg['min_vliq']:.2f}"
    if cfg["require_social"] and not (r["twitter_url"] or r["telegram_url"] or r["website_url"]):
        return False, "no_socials"
    return True, ""

def score(r: dict) -> float:
    lp=r["lp_usd"]; vol=r["volume_24h_usd"]; traders=r["unique_traders_24h"]
    fdv=r["fdv_usd"]; age=r.get("age_hours")
    vliq = min(vol/(lp+1e-9), 5.0)/5.0 if lp>0 else 0.0
    tnorm= min(traders/1500.0, 1.0)
    fdv_ok = 1.0 if (0<fdv<=3_000_000) else (0.85 if fdv<=6_000_000 else 0.7 if fdv<=10_000_000 else 0.5 if fdv<=15_000_000 else 0.0)
    age_ok = 1.0 if (age is None) else (1.0 if age<=24 else 0.75 if age<=72 else 0.5 if age<=240 else 0.3)
    s = 100*(0.46*vliq + 0.29*tnorm + 0.15*fdv_ok + 0.10*age_ok)
    buys, sells = r["buys_24h"], r["sells_24h"]
    if buys + sells >= 10 and buys > max(1, sells)*1.05:
        s += 2.0
    return round(s,2)

def compute_stop_loss(price: float, vliq: float, traders: int) -> float:
    if price <= 0: return 0.0
    sl_pct = 0.25
    if vliq >= 0.30 and traders >= 20: sl_pct = 0.22
    if vliq < 0.15 or traders < 8:     sl_pct = 0.28
    return round(price*(1.0 - sl_pct), 12)

def fib_levels(price: float, chg24_pct) -> dict:
    if price <= 0:
        return {"fib_dn_0382":"","fib_dn_0618":"","fib_up_1618":"","fib_up_2618":""}
    if chg24_pct is None:
        swing = 0.10*price
    else:
        try:
            open_ = price / (1.0 + chg24_pct/100.0)
            swing = abs(price - open_)
        except Exception:
            swing = 0.10*price
    dn_382 = max(price - 0.382*swing, 0.0)
    dn_618 = max(price - 0.618*swing, 0.0)
    up_1618 = price + 1.618*swing
    up_2618 = price + 2.618*swing
    return {
        "fib_dn_0382": round(dn_382, 12),
        "fib_dn_0618": round(dn_618, 12),
        "fib_up_1618": round(up_1618, 12),
        "fib_up_2618": round(up_2618, 12),
    }

def add_exit_prices(r: dict, tp_multipliers, fdv_targets):
    price = float(r.get("price_usd") or 0)
    fdv   = float(r.get("fdv_usd") or 0)
    out = {}
    for m in tp_multipliers:
        out[f"tp{int(m)}x_price"] = round(price*m, 12) if price>0 else ""
    if price>0 and fdv>0:
        for T in fdv_targets:
            mult = T/ fdv
            out[f"fdv{int(T/1_000_000)}m_price"] = round(price*mult, 12)
    else:
        for T in fdv_targets:
            out[f"fdv{int(T/1_000_000)}m_price"] = ""
    vliq = (r["volume_24h_usd"]/(r["lp_usd"]+1e-9)) if r["lp_usd"]>0 else 0.0
    out["stop_loss_price"] = compute_stop_loss(price, vliq, int(r.get("unique_traders_24h",0)))
    out.update(fib_levels(price, r.get("chg24_pct")))
    return out

# defaults
DISCOVERY = dict(max_age_h=2160, min_lp=3000, max_lp=2_000_000, max_fdv=20_000_000,
                 min_traders_24h=4, min_vliq=0.10, require_social=False)
REFINE    = dict(max_age_h=504,  min_lp=6000, max_lp=1_500_000, max_fdv=12_000_000,
                 min_traders_24h=8, min_vliq=0.15, require_social=True)

def run_once(rows, cfg, target, outdir, tag, tp_multipliers, fdv_targets):
    items, rejects = [], []
    for r in rows:
        ok, why = pass_gates(r, cfg)
        if ok:
            rr=dict(r); rr["score"]=score(r)
            rr.update(add_exit_prices(rr, tp_multipliers, fdv_targets))
            items.append(rr)
        else:
            rr=dict(r); rr["reject_reason"]=why; rejects.append(rr)
    items = sorted(items, key=lambda x: x["score"], reverse=True)[:target]
    for i, it in enumerate(items, start=1):
        it["rank"]=i
    outdir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(items).to_csv(outdir/f"candidates_{tag}.csv", index=False)
    pd.DataFrame(rejects).to_csv(outdir/f"rejects_{tag}.csv", index=False)
    def lnk(u,t): return f'<a href="{u}" target="_blank">{t}</a>' if u else ""
    html_rows=[]
    for i, r in enumerate(items, start=1):
        top = (i==1)
        style = ' style="background:#fff9d6;"' if top else ""
        html_rows.append(
            f"<tr{style}>"
            f"<td>{r['rank']}</td>"
            f"<td>{r['token_symbol']}</td>"
            f"<td>{r['chain']}</td>"
            f"<td>{r['age_hours']}</td>"
            f"<td>{r['score']}</td>"
            f"<td>${r['price_usd']}</td>"
            f"<td>${int(r['fdv_usd'])}</td>"
            f"<td>${int(r['lp_usd'])}</td>"
            f"<td>${int(r['volume_24h_usd'])}</td>"
            f"<td>{r['unique_traders_24h']}</td>"
            f"<td>{r['dex']}</td>"
            f"<td>{lnk(r['trade_url'],'Trade')}</td>"
            f"<td>{lnk(r['explorer_url'],'Explorer')}</td>"
            f"<td>{lnk(r['twitter_url'],'Twitter')}</td>"
            f"<td>{lnk(r['telegram_url'],'Telegram')}</td>"
            f"<td>{lnk(r['website_url'],'Website')}</td>"
            f"<td>{r.get('stop_loss_price','')}</td>"
            f"<td>{r.get('tp2x_price','')}</td>"
            f"<td>{r.get('tp5x_price','')}</td>"
            f"<td>{r.get('tp10x_price','')}</td>"
            f"<td>{r.get('fib_dn_0382','')}</td>"
            f"<td>{r.get('fib_dn_0618','')}</td>"
            f"<td>{r.get('fib_up_1618','')}</td>"
            f"<td>{r.get('fib_up_2618','')}</td>"
            f"<td>{r.get('fdv25m_price','')}</td>"
            f"<td>{r.get('fdv50m_price','')}</td>"
            "</tr>"
        )
    tpl = Template(
        "<!doctype html><meta charset='utf-8'><title>NewCoin Hunter</title>"
        "<style>body{font-family:system-ui,Arial;margin:24px}table{border-collapse:collapse;width:100%}"
        "th,td{border:1px solid #eee;padding:6px 8px;font-size:14px}th{background:#f7f7f7}</style>"
        "<h1>NewCoin Hunter — $tag</h1>"
        "<p>Generated: $ts</p>"
        "<table><tr>"
        "<th>#</th><th>Token</th><th>Chain</th><th>Age(h)</th><th>Score</th>"
        "<th>Price</th><th>FDV</th><th>LP</th><th>Vol24h</th><th>Traders</th><th>DEX</th>"
        "<th>Trade</th><th>Explorer</th><th>Twitter</th><th>Telegram</th><th>Website</th>"
        "<th>Stop-Loss</th><th>TP2×</th><th>TP5×</th><th>TP10×</th>"
        "<th>Fib 0.382↓</th><th>Fib 0.618↓</th><th>Fib 1.618↑</th><th>Fib 2.618↑</th>"
        "<th>FDV25M</th><th>FDV50M</th>"
        "</tr>$rows</table>"
    )
    html = tpl.substitute(tag=tag, ts=time.strftime("%Y-%m-%d %H:%M:%S"), rows="".join(html_rows))
    (outdir/f"candidates_{tag}.html").write_text(html, encoding="utf-8")
    return items

def run(chains: list, limit_per_chain: int, target_disc: int, target_ref: int, outdir: pathlib.Path,
        tp_multipliers, fdv_targets, disc_cfg, ref_cfg):
    rows=[]
    for q in chains:
        try:
            rows.extend(row_from_pair(p) for p in fetch_pairs_for_chain(q, limit=limit_per_chain))
        except Exception as e:
            print("Dex pull failed for", q, "->", e)
    disc = run_once(rows, disc_cfg, target_disc, outdir, "discovery", tp_multipliers, fdv_targets)
    refined_input = disc if disc else rows
    run_once(refined_input, ref_cfg, target_ref, outdir, "refined", tp_multipliers, fdv_targets)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chains", default="base,solana,eth,arbitrum,polygon,bsc,optimism,avalanche,blast,mantle,linea,scroll,zksync,fantom")
    ap.add_argument("--limit_per_chain", type=int, default=2000)
    ap.add_argument("--target_disc", type=int, default=200)
    ap.add_argument("--target_ref", type=int, default=90)
    ap.add_argument("--out", default="out_test2")
    ap.add_argument("--tp", default="2,5,10")
    ap.add_argument("--fdv_targets", default="25000000,50000000")

    # discovery overrides
    ap.add_argument("--max_age_h_disc", type=float)
    ap.add_argument("--min_lp_disc", type=float)
    ap.add_argument("--max_lp_disc", type=float)
    ap.add_argument("--max_fdv_disc", type=float)
    ap.add_argument("--min_traders_disc", type=int)
    ap.add_argument("--min_vliq_disc", type=float)
    ap.add_argument("--require_social_disc", choices=["true","false"])

    # refine overrides
    ap.add_argument("--max_age_h_refine", type=float)
    ap.add_argument("--min_lp_refine", type=float)
    ap.add_argument("--max_lp_refine", type=float)
    ap.add_argument("--max_fdv_refine", type=float)
    ap.add_argument("--min_traders_refine", type=int)
    ap.add_argument("--min_vliq_refine", type=float)
    ap.add_argument("--require_social_refine", choices=["true","false"])

    args = ap.parse_args()

    # parse exits
    tp_multipliers = [float(x) for x in args.tp.split(",") if x.strip()]
    fdv_targets = [float(x) for x in args.fdv_targets.split(",") if x.strip()]

    # start from defaults; apply overrides if any
    disc_cfg = DISCOVERY.copy()
    ref_cfg  = REFINE.copy()
    for k, v in dict(max_age_h=args.max_age_h_disc, min_lp=args.min_lp_disc, max_lp=args.max_lp_disc,
                     max_fdv=args.max_fdv_disc, min_traders_24h=args.min_traders_disc,
                     min_vliq=args.min_vliq_disc).items():
        if v is not None: disc_cfg[k]=v
    if args.require_social_disc is not None: disc_cfg["require_social"]=(args.require_social_disc=="true")
    for k, v in dict(max_age_h=args.max_age_h_refine, min_lp=args.min_lp_refine, max_lp=args.max_lp_refine,
                     max_fdv=args.max_fdv_refine, min_traders_24h=args.min_traders_refine,
                     min_vliq=args.min_vliq_refine).items():
        if v is not None: ref_cfg[k]=v
    if args.require_social_refine is not None: ref_cfg["require_social"]=(args.require_social_refine=="true")

    lock_path = pathlib.Path(".newcoin_hunter.lock")
    with open(lock_path, "w") as lf:
        try:
            fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Another run is active. Exiting to avoid conflicts."); sys.exit(0)

        chains = [c.strip() for c in args.chains.split(",") if c.strip()]
        outdir = pathlib.Path(args.out)
        run(chains, args.limit_per_chain, args.target_disc, args.target_ref, outdir,
            tp_multipliers, fdv_targets, disc_cfg, ref_cfg)

if __name__ == "__main__":
    main()

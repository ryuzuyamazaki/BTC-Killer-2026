# BTC 5M Sniper — Standalone Web App

Converted from BTC 5M Sniper V24 UI + History.

## Run locally

Use a local HTTP server (do not open `index.html` directly with `file://`):

```bash
python3 -m http.server 8080
```

Then open `http://localhost:8080`.

## Deploy

This is a static web app. Upload the contents of this folder to Vercel, Netlify, Cloudflare Pages, GitHub Pages, or another static host.

## Browser storage

Sniper history, calibration, and panel visibility use `localStorage` in the browser instead of Chrome extension storage.

## Live feeds

- Binance BTC/USDT WebSocket
- Polymarket CLOB WebSocket
- Polymarket Gamma/CLOB REST fallbacks

The app contains no order execution, private-key handling, or trading API credentials.

# 🚀 Deploying Valorant Analytics to Production

This directory contains the production-ready Evidence.dev dashboard for the Valorant Analytics platform. The dashboard connects directly to your Databricks SQL Warehouse and serves live metrics across Team Overview, Weapons, Maps, Players, and Agent Meta.

---

## 🛠️ Production Architecture

```text
Databricks SQL Warehouse (Unity Catalog: valorant.gold.*)
                     ▲
                     │ (Encrypted SQL over HTTPS / Port 443)
                     │
         [ Production Cloud Container ]
           evidencedev/serve:latest
                     │
                     ▼
         Public Dashboard URL (HTTPS)
   (Vercel / Railway / Render / Evidence Cloud)
```

---

## ⚡ Deployment Options

### Option A: Deploy on Railway (Easiest — Recommended)

Railway offers free container hosting with instant GitHub integration.

1. **Push your code to GitHub** (commit `Dockerfile`, `connection.yaml.prod`, and the `pages/` directory).
2. Go to **[railway.app](https://railway.app)** and click **New Project** → **Deploy from GitHub repo**.
3. Select your repository (`valo analytics`).
4. In the service settings:
   * **Root Directory:** Set to `dashboards/evidence`
   * **Custom Build Command (if prompted):** Copy `connection.yaml.prod` to `connection.yaml`:
     ```bash
     cp connection.yaml.prod connection.yaml
     ```
5. In **Variables (Environment Variables)**, add:
   * `DATABRICKS_TOKEN`: `<your-databricks-pat-token>`
   * `EVIDENCE_AUTH_DISABLED`: `true` *(or set `EVIDENCE_BASIC_USER` & `EVIDENCE_BASIC_PASSWORD` for password protection)*
6. In **Settings** → **Networking**, click **Generate Domain** to get a public URL (e.g., `https://valo-analytics.up.railway.app`).

---

### Option B: Deploy on Vercel

Vercel supports Evidence via container images on Vercel Functions.

1. In `dashboards/evidence/`:
   ```bash
   cp connection.yaml.prod connection.yaml
   ```
2. In `.gitignore`, remove `connection.yaml` since it only contains `${DATABRICKS_TOKEN}` (the secret itself is never committed).
3. Commit and push to GitHub.
4. Go to **[vercel.com/new](https://vercel.com/new)** and import the GitHub repo.
5. In **Environment Variables**, add:
   * `DATABRICKS_TOKEN`: `<your-databricks-pat-token>`
   * `EVIDENCE_AUTH_DISABLED`: `true`
6. Click **Deploy**.

---

### Option C: Deploy to Evidence Studio Cloud (1-Click Managed)

Evidence provides an official managed cloud with custom domains and GitHub PR previews.

1. In your local terminal inside `dashboards/evidence/`, run:
   ```bash
   evidence launch
   ```
2. Follow the prompt to log in and authorize GitHub.
3. Evidence will create a hosted cloud project at `https://your-team.evidence.app`.

---

## 🔒 Security Best Practices in Production

1. **Never commit personal access tokens.** Always use `${DATABRICKS_TOKEN}` in production configuration.
2. **Databricks Auto-Stop:** Ensure your Databricks SQL Warehouse has `Auto-stop` enabled (e.g., 10 or 15 minutes) to conserve Free Edition compute credits when nobody is browsing the dashboard.
3. **Password Protection:** If you want to keep the dashboard private to your squad, simply set:
   * `EVIDENCE_BASIC_USER=squad`
   * `EVIDENCE_BASIC_PASSWORD=your_secure_password`
   in your host's environment variables.

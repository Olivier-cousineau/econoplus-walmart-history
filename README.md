# econoplus-walmart-history

Repo secondaire pour stocker les snapshots Walmart par date et générer des index consommables par EconoPlus.

## Structure

- `snapshots/YYYY-MM-DD/<store_slug>.json` : snapshots bruts par magasin.
- `indexes/history_store/<store_slug>.json` : historique des prix (fenêtre glissante).
- `indexes/deals_daily/YYYY-MM-DD/<store_slug>.json` : deals détectés par chute de prix.
- `scripts/split_by_store.py` : split `walmart_all.json` en fichiers par magasin.
- `scripts/build_indexes.py` : construit l'historique et les deals.

## Consommation côté EconoPlus

> **Important :** EconoPlus ne doit pas lire `snapshots/` (trop volumineux).
> Lire uniquement les index :
>
> - `indexes/deals_daily/...`
> - `indexes/history_store/...`

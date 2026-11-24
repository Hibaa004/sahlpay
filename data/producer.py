#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
producer.py - Générateur de transactions SahlPay.ma (entreprise fictive)

Trois modes :
  python producer.py --mode merchants                 -> crée merchants.json (300 lignes, JSON Lines)
  python producer.py --mode file --n 50000            -> crée transactions_history.jsonl
  python producer.py --mode kafka --rate 20           -> publie dans le topic Kafka "transactions"

Cas réels simulés :
  - ~5 % d'événements en retard (event_time reculé de 1 à 15 minutes)
  - ~2 % de doublons (même tx_id, même status, réémis quelques secondes plus tard)
  - scénarios de fraude injectés (vérité terrain écrite dans fraud_ground_truth.json) :
      1. appareil partagé par 25 comptes (D-SHARED01)
      2. transferts circulaires A -> B -> C -> A (5 cycles)
      3. comptes collecteurs recevant de nombreux petits virements (2 collecteurs)
  - un peu de "bruit" normal (appareils partagés par 2 comptes, comptes populaires)
    pour produire de vrais faux positifs lors de l'analyse de graphes.
"""
import argparse
import hashlib
import heapq
import json
import math
import os
import random
import sys
import time
from datetime import datetime, timezone

# --------------------------------------------------------------------------
# Référentiel
# --------------------------------------------------------------------------
# ville: (latitude, longitude, poids)
CITIES = {
    "Casablanca": (33.5731, -7.5898, 0.28),
    "Rabat":      (34.0209, -6.8416, 0.15),
    "Marrakech":  (31.6295, -7.9811, 0.12),
    "Tanger":     (35.7595, -5.8340, 0.10),
    "Fes":        (34.0181, -5.0078, 0.09),
    "Agadir":     (30.4278, -9.5981, 0.07),
    "Meknes":     (33.8935, -5.5473, 0.05),
    "Kenitra":    (34.2610, -6.5802, 0.05),
    "Tetouan":    (35.5785, -5.3684, 0.05),
    "Oujda":      (34.6814, -1.9086, 0.04),
}
# categorie: (libellé de nom, multiplicateur du montant médian)
CATEGORIES = {
    "epicerie":    ("Epicerie",      0.6),
    "carburant":   ("Station",       1.5),
    "telecom":     ("Telecom",       0.6),
    "restauration": ("Restaurant",   0.8),
    "pharmacie":   ("Pharmacie",     1.0),
    "e-commerce":  ("Boutique en ligne", 2.0),
    "vetements":   ("Boutique",      1.4),
    "transport":   ("Transport",     0.7),
}


def weighted_city(rng):
    names = list(CITIES)
    return rng.choices(names, weights=[CITIES[c][2] for c in names])[0]


def make_merchants(seed=42):
    rng = random.Random(seed)
    cats = list(CATEGORIES)
    out = []
    for i in range(1, 301):
        city = weighted_city(rng)
        cat = rng.choice(cats)
        lat, lon, _ = CITIES[city]
        out.append({
            "merchant_id": f"M-{i:03d}",
            "name": f"{CATEGORIES[cat][0]} {city} {i:03d}",
            "city": city,
            "category": cat,
            "latitude": round(lat + rng.uniform(-0.05, 0.05), 5),
            "longitude": round(lon + rng.uniform(-0.05, 0.05), 5),
        })
    return out


def dev(wallet, k=0):
    return "D-" + hashlib.md5(f"{wallet}-{k}".encode()).hexdigest()[:6]


# --------------------------------------------------------------------------
# Monde simulé
# --------------------------------------------------------------------------
class World:
    SHARED_DEVICE = "D-SHARED01"

    def __init__(self, merchants, seed=42):
        rng = random.Random(seed + 1)
        self.merchants = merchants
        # les premiers commerçants sont plus populaires (loi de type Zipf)
        self.m_weights = [1 / (i + 1) ** 0.7 for i in range(len(merchants))]

        self.wallets = [f"W-{10000 + i}" for i in range(1000)]
        self.popular = self.wallets[:5]  # comptes "populaires" légitimes
        self.shared_wallets = [f"W-{90001 + i}" for i in range(25)]
        self.cycles = [[f"W-{91000 + 3 * k + j}" for j in (1, 2, 3)] for k in range(5)]
        self.collectors = {
            "W-92000": [f"W-{92001 + i}" for i in range(40)],
            "W-92100": [f"W-{92101 + i}" for i in range(40)],
        }

        all_fraud = list(self.shared_wallets)
        for c in self.cycles:
            all_fraud += c
        for col, payers in self.collectors.items():
            all_fraud += [col] + payers
        self.fraud_wallets = set(all_fraud)

        self.home = {w: weighted_city(rng) for w in self.wallets + all_fraud}
        self.devices = {}
        for w in self.wallets:
            self.devices[w] = [dev(w, 0)] + ([dev(w, 1)] if rng.random() < 0.2 else [])
        # bruit : 10 couples de comptes légitimes (famille) partagent un appareil
        for i in range(10):
            w1, w2 = self.wallets[100 + 2 * i], self.wallets[101 + 2 * i]
            self.devices[w2] = [self.devices[w1][0]]
        for w in all_fraud:
            self.devices[w] = [dev(w, 0)]

    def ground_truth(self):
        return {
            "shared_device": {"device_id": self.SHARED_DEVICE, "wallets": self.shared_wallets},
            "circular_transfers": self.cycles,
            "collector_accounts": {c: len(p) for c, p in self.collectors.items()},
            "collector_payers": self.collectors,
            "benign_noise": {
                "family_shared_devices": [
                    [self.wallets[100 + 2 * i], self.wallets[101 + 2 * i]] for i in range(10)
                ],
                "popular_wallets": self.popular,
            },
        }

    # ---- montant -----------------------------------------------------------
    @staticmethod
    def amount(rng, median, sigma=1.1):
        a = rng.lognormvariate(math.log(median), sigma)
        return round(min(max(a, 1.0), 20000.0), 2)

    # ---- transactions normales ---------------------------------------------
    def normal_spec(self, rng):
        payer = rng.choice(self.wallets)
        if rng.random() < 0.75:
            m = rng.choices(self.merchants, weights=self.m_weights)[0]
            mult = CATEGORIES[m["category"]][1]
            return dict(type="P2M", payer=payer, payee=m["merchant_id"],
                        device=rng.choice(self.devices[payer]), city=m["city"],
                        channel=rng.choices(["QR", "APP", "USSD"], [0.6, 0.3, 0.1])[0],
                        amount=self.amount(rng, 150 * mult), fraud=False)
        if rng.random() < 0.08:
            payee = rng.choice(self.popular)
        else:
            payee = rng.choice(self.wallets)
        if payee == payer:
            payee = self.wallets[(self.wallets.index(payer) + 1) % len(self.wallets)]
        return dict(type="P2P", payer=payer, payee=payee,
                    device=rng.choice(self.devices[payer]), city=self.home[payer],
                    channel=rng.choices(["APP", "USSD"], [0.7, 0.3])[0],
                    amount=self.amount(rng, 300, 1.0), fraud=False)

    # ---- scénarios de fraude -----------------------------------------------
    def fraud_spec(self, rng):
        s = rng.choices(["shared", "cycle", "collector"], [0.4, 0.35, 0.25])[0]
        if s == "shared":
            payer = rng.choice(self.shared_wallets)
            m = rng.choice(self.merchants)
            return dict(type="P2M", payer=payer, payee=m["merchant_id"],
                        device=self.SHARED_DEVICE, city=m["city"], channel="APP",
                        amount=round(rng.uniform(100, 900), 2), fraud=True)
        if s == "cycle":
            cyc = rng.choice(self.cycles)
            i = rng.randrange(3)
            payer, payee = cyc[i], cyc[(i + 1) % 3]
            return dict(type="P2P", payer=payer, payee=payee, device=self.devices[payer][0],
                        city=self.home[payer], channel="APP",
                        amount=round(rng.uniform(4400, 4800), 2), fraud=True)
        col = rng.choice(list(self.collectors))
        payer = rng.choice(self.collectors[col])
        return dict(type="P2P", payer=payer, payee=col, device=self.devices[payer][0],
                    city=self.home[payer], channel="APP",
                    amount=round(rng.uniform(20, 80), 2), fraud=True)


# --------------------------------------------------------------------------
# Cycle de vie d'une transaction + flux d'événements
# --------------------------------------------------------------------------
def iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def lifecycle(rng, spec):
    if spec["fraud"]:
        return ["INITIATED", "AUTHORIZED", "COMPLETED"]
    seq = ["INITIATED"]
    if rng.random() < 0.92:
        seq.append("AUTHORIZED")
        if rng.random() < 0.90:
            seq.append("COMPLETED")
            if rng.random() < 0.01:
                seq.append("REVERSED")
        else:
            seq.append("REJECTED")
    else:
        seq.append("REJECTED")
    return seq


def event_stream(world, rng, t_start, tx_interval, fraud_rate=0.06,
                 late_rate=0.05, dup_rate=0.02):
    """Générateur infini de (instant_d_émission, événement), trié par instant d'émission."""
    heap, counter, tx_n = [], 0, 0
    next_new = t_start
    while True:
        while heap and heap[0][0] <= next_new:
            t, _, ev = heapq.heappop(heap)
            yield t, ev
        tx_n += 1
        spec = world.fraud_spec(rng) if rng.random() < fraud_rate else world.normal_spec(rng)
        tx_id = f"TX-{tx_n:08d}"
        t = next_new
        for status in lifecycle(rng, spec):
            ev_time = t
            if rng.random() < late_rate:                 # événement en retard
                ev_time = t - rng.uniform(60, 900)
            ev = {
                "tx_id": tx_id, "type": spec["type"], "payer_wallet": spec["payer"],
                "payee_id": spec["payee"], "device_id": spec["device"], "city": spec["city"],
                "channel": spec["channel"], "status": status,
                "amount_mad": spec["amount"], "event_time": iso(ev_time),
            }
            counter += 1
            heapq.heappush(heap, (t, counter, ev))
            if rng.random() < dup_rate:                  # doublon exact
                counter += 1
                heapq.heappush(heap, (t + rng.uniform(0.5, 5), counter, dict(ev)))
            t += rng.uniform(1, 5)
        next_new += rng.expovariate(1.0 / tx_interval)


# --------------------------------------------------------------------------
# Modes
# --------------------------------------------------------------------------
def load_merchants(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def write_ground_truth(world, out):
    p = os.path.join(out, "fraud_ground_truth.json")
    with open(p, "w", encoding="utf-8") as f:
        json.dump(world.ground_truth(), f, indent=2, ensure_ascii=False)
    print(f"Vérité terrain de la fraude : {p}")


def main():
    ap = argparse.ArgumentParser(description="Générateur de transactions SahlPay")
    ap.add_argument("--mode", choices=["merchants", "file", "kafka"], required=True)
    ap.add_argument("--n", type=int, default=50000, help="nombre d'événements (mode file)")
    ap.add_argument("--rate", type=float, default=20, help="événements/seconde (mode kafka)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=".", help="dossier de sortie / de lecture")
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--topic", default="transactions")
    ap.add_argument("--start", default="2026-11-14T00:00:00Z",
                    help="début de la période simulée (mode file)")
    ap.add_argument("--days", type=float, default=2.0, help="durée simulée (mode file)")
    a = ap.parse_args()

    os.makedirs(a.out, exist_ok=True)
    mpath = os.path.join(a.out, "merchants.json")

    if a.mode == "merchants":
        with open(mpath, "w", encoding="utf-8") as f:
            for m in make_merchants(a.seed):
                f.write(json.dumps(m, ensure_ascii=False) + "\n")
        print(f"{mpath} créé (300 commerçants, une ligne JSON par commerçant)")
        return

    if not os.path.exists(mpath):
        with open(mpath, "w", encoding="utf-8") as f:
            for m in make_merchants(a.seed):
                f.write(json.dumps(m, ensure_ascii=False) + "\n")
    world = World(load_merchants(mpath), a.seed)
    rng = random.Random(a.seed)
    write_ground_truth(world, a.out)
    EV_PER_TX = 3.0  # nombre moyen d'événements par transaction

    if a.mode == "file":
        t0 = datetime.strptime(a.start, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
        interval = a.days * 86400 / (a.n / EV_PER_TX)
        path = os.path.join(a.out, "transactions_history.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            for i, (_, ev) in enumerate(event_stream(world, rng, t0, interval)):
                if i >= a.n:
                    break
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
        print(f"{path} créé : {a.n} événements")
        return

    # ---- mode kafka -----------------------------------------------------------
    try:
        from kafka import KafkaProducer
    except ImportError:
        sys.exit("Installez kafka-python :  pip install kafka-python")
    producer = KafkaProducer(
        bootstrap_servers=a.bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=20,
    )
    interval = EV_PER_TX / a.rate
    sent, last = 0, time.time()
    print(f"Publication dans '{a.topic}' sur {a.bootstrap} à ~{a.rate} événements/s (Ctrl+C pour arrêter)")
    try:
        for t, ev in event_stream(world, rng, time.time(), interval):
            wait = t - time.time()
            if wait > 0:
                time.sleep(wait)
            producer.send(a.topic, key=ev["tx_id"], value=ev)
            sent += 1
            if time.time() - last >= 5:
                print(f"  {sent} événements envoyés")
                last = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        print(f"Arrêt. Total envoyé : {sent}")


if __name__ == "__main__":
    main()

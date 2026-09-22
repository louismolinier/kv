# Benchmarks et itérations

Mesures locales du 22 septembre 2026 sur macOS arm64 avec Apple Clang 21,
`-O2`, quatre threads et I/O bufferisées. Aucun `fsync` n'est effectué par
opération. Les résultats YCSB sont des runs uniques et restent sensibles au
cache du système et à la charge de la machine.

## Campagne YCSB A–F

Chaque workload a chargé **100 000 records**, puis exécuté **100 000
opérations** dans un nouveau fichier. Commande :

```sh
./bench_all.sh 100000 100000 4
```

| Workload | Mix principal | Load (ops/s) | Run (ops/s) |
|---|---|---:|---:|
| A | 50 % read, 50 % update | 63 245 | 123 884 |
| B | 95 % read, 5 % update | 63 918 | 316 134 |
| C | 100 % read | 63 983 | 463 835 |
| D | 95 % read, 5 % insert | 63 359 | 368 957 |
| E | 95 % scan, 5 % insert | 64 932 | 12 726 |
| F | 50 % read, 50 % read-modify-write | 64 385 | 100 051 |

Le scan E reste le plus coûteux : il lit les valeurs de la plage demandée sur
disque et sa première opération construit l'index trié en RAM. Le temps de run
E pour ces 100 000 opérations était de 7,86 s.

## Comparaisons avant/après aux mêmes tailles

Les trois premiers workloads ci-dessous utilisent les mêmes paramètres avant
et après : A/C ont 50 000 records et 50 000 opérations, E a 5 000 records et
1 000 opérations. E a été réduit dans la référence initiale car son ancien
scan complet rendait une charge plus grande impraticable.

| Mesure | Premier jet | Version actuelle | Gain |
|---|---:|---:|---:|
| Load A | 46 470 ops/s | 66 720 ops/s | ×1,44 |
| Run A | 86 408 ops/s | 131 236 ops/s | ×1,52 |
| Run C | 148 268 ops/s | 496 882 ops/s | ×3,35 |
| Run E | 98 ops/s | 11 808 ops/s | ×120 |

Étapes gardées après mesure :

1. Écrire en un `pwrite` par record. Sur 20 000 inserts profilés, les appels
   d'écriture sont passés de 60 001 à 20 001 et leur temps cumulé de 222 ms à
   106 ms.
2. Construire l'index trié au premier scan. Sur 300 opérations E profilées,
   les appels de lecture sont passés de 4,27 millions à environ 38 000.
3. Autoriser plusieurs lecteurs en parallèle avec `std::shared_mutex`.
4. Lire en une fois l'en-tête et la clé courte lors d'une recherche ponctuelle.
5. Indexer les régions libres par taille. Sur 30 000 inserts, puis 15 000
   suppressions et 15 000 réinsertions, la dernière phase est passée d'environ
   0,208 s à 0,035 s (deux runs de chaque version).

Le benchmark de churn est reproductible avec :

```sh
make build/churn_bench
./build/churn_bench 30000
```

Les workloads YCSB A–F utilisent ici des valeurs de taille stable et ne
mesurent presque pas l'allocateur. Le test de churn le sollicite séparément.

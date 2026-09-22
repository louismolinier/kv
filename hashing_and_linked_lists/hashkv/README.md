# HashKV — prototype autonome

Ce dossier est à côté de `YCSB-cpp`. Le binaire YCSB dédié est construit ici
en liant l'adaptateur HashKV aux sources de `../YCSB-cpp/core` ; le clone YCSB
reste inchangé.

## Organisation

```text
hashkv/
├── include/hashkv/kv_store.h   API publique
├── src/kv_store.cc             buckets, records disque, allocateur
├── src/ycsb_adapter.cc         binding YCSB « hashkv »
├── tests/kv_store_test.cc      persistance, collisions, scans, concurrence
├── tests/churn_bench.cc        suppressions et réutilisation des espaces
├── Makefile
├── ycsb.properties
├── bench.sh                    workload A
└── bench_all.sh                workloads A à F
```

## Architecture

```text
RAM                                      DISQUE (un fichier)

buckets[hash(key) % N] ──offset──► [next | tailles | key | value]
                                         │
                                         └──offset──► [next | key | value]

free_by_size = {taille → offset} ──► anciennes régions réutilisables
ordered_index = {key → offset} ────► créé au premier Scan seulement
```

L'array de buckets est en RAM. Chaque bucket pointe par offset vers une liste
chaînée de records dans le fichier. Un delete ou un update trop grand rend son
ancienne région à l'allocateur. Celui-ci choisit la plus petite région assez
grande avec un `std::multimap` ; la liste est reconstruite avec les buckets au
redémarrage en parcourant le fichier.

Les records sont écrits en un seul `pwrite`. La recherche d'une clé courte lit
son en-tête et sa clé en un seul `pread`. Les lectures et scans peuvent
s'exécuter en parallèle avec un `std::shared_mutex` ; les écritures restent
sérialisées. Le premier scan crée un index trié en RAM, ensuite maintenu lors
des insertions, modifications et suppressions. Les workloads sans scans ne
paient ni sa construction ni sa mémoire.

## Compiler et tester

```sh
cd hashkv
make
make test
```

Le build ne dépend pas du sous-module HdrHistogram. Il produit
`build/kv_store_test` et `build/ycsb_hashkv`.

## Benchmarks

```sh
./bench_all.sh                  # A-F : 100k records, 100k opérations, 4 threads
./bench_all.sh 50000 50000 4   # tailles personnalisées
./bench.sh 100000 100000 4     # workload A seul

make build/churn_bench
./build/churn_bench 30000       # delete puis réutilisation de 15k régions
```

Chaque workload est chargé dans un fichier neuf. Le chemin de données par
défaut est sous `/tmp` et le script l'affiche à la fin. La synthèse des mesures
et les gains des itérations sont dans [BENCHMARK.md](BENCHMARK.md).

Pour activer les chronos internes :

```sh
make build/ycsb_hashkv_profile
./build/ycsb_hashkv_profile -load -run -db hashkv \
  -P ../YCSB-cpp/workloads/workloada -P ycsb.properties \
  -threads 4 -p recordcount=20000 -p operationcount=20000 \
  -p hashkv.path=/tmp/hashkv-profile-example.data \
  -p hashkv.destroy=true
```

Ce binaire expose le nombre et le temps cumulé des `pread`/`pwrite`, des
recherches, des scans et de l'attente du verrou. Les chronos sont désactivés
dans le binaire de benchmark normal.

## Limites actuelles

- Format binaire natif, non portable entre architectures/endianess.
- Pas de WAL ni de garantie de récupération après un crash au milieu d'un write.
- Un seul processus doit ouvrir le fichier à la fois.
- Les écritures sont encore sérialisées par un verrou global.
- Les régions libres ne sont ni découpées ni fusionnées, ce qui peut fragmenter
  le fichier.
- Le premier scan parcourt les listes et crée un index trié de toutes les clés
  en RAM ; cette mémoire croît avec le nombre de clés.

Ce sont les prochains points à travailler pour améliorer la durabilité, la
concurrence en écriture et l'utilisation de l'espace disque.

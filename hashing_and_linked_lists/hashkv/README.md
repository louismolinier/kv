# HashKV — prototype autonome

Ce dossier est à côté de `YCSB-cpp`. Le binaire YCSB dédié est construit ici
en liant l'adaptateur HashKV aux sources de `../YCSB-cpp/core` ; le clone YCSB
reste inchangé.

## Organisation

```text
hashkv/
├── include/hashkv/kv_store.h   API publique
├── src/kv_store.cc             buckets, records disque, allocateur
├── src/wal.h, src/wal.cc        journal durable et reprise
├── src/ycsb_adapter.cc         binding YCSB « hashkv »
├── tests/kv_store_test.cc      persistance, collisions, scans, concurrence
├── tests/wal_recovery_test.cc  crash, journal tronqué, migration
├── tests/churn_bench.cc        suppressions et réutilisation des espaces
├── tests/space_bench.cc        découpage et fusion des espaces libres
├── Makefile
├── ycsb.properties
├── bench.sh                    workload A
└── bench_all.sh                workloads A à F
```

## Architecture

```text
RAM                                      DISQUE

buckets[hash(key) % N] ──offset──► data: [next | tailles | key | value]
                                         │
                                         └──offset──► [next | key | value]

Put/Erase ──lot, pwrite, fsync──► data.wal: [op | key | value | checksum]…

free_by_size = {(taille, offset)} ──► meilleure région réutilisable
free_by_offset = {offset → taille} ──► voisins à fusionner
ordered_index = {key → (offset, taille valeur)} ──► créé au premier Scan
scan_cache = {offset → valeur} ──► optionnel, borné, Scan seulement
```

L'array de buckets est en RAM. Chaque bucket pointe par offset vers une liste
chaînée de records dans le fichier. Un delete ou un update trop grand rend son
ancienne région à l'allocateur. Celui-ci choisit la plus petite région assez
grande avec un `std::set`, la découpe si le reste peut contenir un en-tête,
et fusionne les régions libres adjacentes. Les index sont reconstruits au
redémarrage en rejouant le journal.

Les records sont écrits en un seul `pwrite`. `Get` précharge jusqu'à 1,5 Kio
pour obtenir en un `pread` l'en-tête, la clé et une valeur courte ; les grandes
valeurs utilisent une seconde lecture. Les lectures et scans peuvent
s'exécuter en parallèle avec un `std::shared_mutex` ; les écritures restent
sérialisées. Le premier scan crée un index trié en RAM, ensuite maintenu lors
des insertions, modifications et suppressions. Les workloads sans scans ne
paient ni sa construction ni sa mémoire.
Chaque entrée de l'index retient la taille de la valeur, ce qui permet de lire
un résultat de scan avec un seul `pread`.
Pour les scans répétés, un cache direct et borné des valeurs peut être activé ;
il est désactivé par défaut et invalidé lors d'une modification du record.

Chaque `Put` ou `Erase` confirmé est ajouté au fichier `<path>.wal` avec un
checksum et synchronisé par `fsync` **avant** toute modification du fichier
principal. Plusieurs écrivains concurrents peuvent partager le même `fsync` :
leurs frames sont écrites dans l'ordre en un seul `pwrite` (32 opérations au
maximum par lot). La courte attente de 50 µs pour former un lot n'est activée
qu'après observation de concurrence et cesse après 32 lots solitaires ; un
écrivain isolé ne la paie donc normalement pas. Dans l'adaptateur YCSB, le
verrou du read-modify-write est réparti par clé pour permettre les lots sur
des clés indépendantes. Pendant le `fsync` du WAL, les lecteurs continuent à
voir l'état précédent ; le lot est appliqué sous verrou exclusif avant le
retour des appels d'écriture. Un verrou distinct ordonne WAL, `Flush` et
checkpoint. À l'ouverture, le journal validé reconstruit le
fichier et les index ; une dernière frame incomplète est ignorée. Une
corruption au milieu du WAL est signalée au lieu d'être silencieusement
appliquée. La validation et le rejeu lisent le WAL par blocs de 64 Kio ; un
filtre de présence temporaire de 64 Kio évite des recherches disque pour les
clés certainement nouvelles pendant la reconstruction. Une opération
dont l'appel a réussi survit ainsi à un arrêt pendant l'écriture des données,
sous les garanties de `fsync` du système de fichiers. Une opération interrompue
entre la synchronisation du WAL et son retour à l'appelant peut aussi
réapparaître au redémarrage.

Le premier `Open` d'un ancien fichier sans WAL publie une photographie
initiale atomique. `Store::Checkpoint()` remplace ensuite le journal par une
nouvelle photographie des clés vivantes ; l'application doit l'appeler
périodiquement pour éviter une croissance illimitée du WAL. Le fichier
principal et son `.wal` doivent toujours être conservés ensemble.

## Compiler et tester

```sh
cd hashkv
make
make test
```

Le build ne dépend pas du sous-module HdrHistogram. `make test` vérifie aussi
la reprise après troncature du fichier de données, une fin de WAL incomplète,
un crash de processus après `fsync` du WAL (seul ou en lot) et la migration
d'un ancien fichier. Le test WAL couvre aussi les frames traversant une
frontière du tampon de lecture.

## Benchmarks

```sh
./bench_all.sh                  # A-F : 100k records, 100k opérations, 4 threads
./bench_all.sh 50000 50000 4   # tailles personnalisées
./bench.sh 100000 100000 4     # workload A seul

make build/churn_bench
./build/churn_bench 30000       # delete puis réutilisation de 15k régions
make build/space_bench
./build/space_bench 1000        # grosses régions libérées puis petites valeurs
```

Chaque workload est chargé dans un fichier neuf. Le chemin des deux fichiers
de données est sous `/tmp` par défaut ; le script les affiche à la fin. La
synthèse des mesures et les gains des itérations sont dans
[BENCHMARK.md](BENCHMARK.md).
Pour une nouvelle base surtout lue, `-p hashkv.buckets=262144` peut raccourcir
les listes : le tableau de buckets occupe alors 2 Mio au lieu de 0,5 Mio.
Il faut réouvrir un fichier existant avec le même nombre de buckets ; la
valeur par défaut reste donc 65 536.

Pour un workload très riche en scans, `-p hashkv.scan_cache_slots=32768`
active un cache de valeurs de 4 Kio maximum, sans modifier le format du
fichier. Sur E à 50 000/50 000, il apporte environ 15 % de débit en plus,
mais porte la mémoire maximale mesurée d'environ 11 à 51 Mo. Il reste donc
désactivé par défaut ; le plafond théorique à 32 768 slots est d'environ
128 Mio de valeurs plus les métadonnées. Les scripts acceptent aussi
`HASHKV_SCAN_CACHE_SLOTS=32768`.

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
`fsync` du WAL, des recherches, des scans et de l'attente du verrou. Les
chronos sont désactivés dans le binaire de benchmark normal.

## Limites actuelles

- Format binaire natif, non portable entre architectures/endianess.
- Le WAL est rejoué intégralement à chaque ouverture ; le checkpoint est
  manuel, pas encore automatique. Cela augmente le temps d'ouverture et
  l'espace disque si l'application n'appelle pas `Checkpoint()`.
- Le verrou exclusif du fichier empêche plusieurs processus d'ouvrir la même
  base en écriture simultanément.
- Le WAL groupe les synchronisations, mais l'application des écritures au
  fichier principal reste sérialisée par un verrou global.
- Une réduction de valeur *en place* conserve pour l'instant la taille de son
  slot ; cette portion n'est récupérée qu'à la suppression ou au déplacement
  du record.
- Le premier scan parcourt les listes et crée un index trié de toutes les clés
  en RAM ; cette mémoire croît avec le nombre de clés.

Le WAL protège contre les écritures interrompues, pas contre la corruption
arbitraire du support ni contre la perte simultanée des deux fichiers. Les
prochains travaux portent surtout sur le checkpoint automatique et la
concurrence en écriture.

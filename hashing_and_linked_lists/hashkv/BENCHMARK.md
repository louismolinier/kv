# Benchmarks et itérations

Mesures locales sur macOS arm64 avec Apple Clang 21, `-O2` et quatre threads.
Chaque ligne YCSB est un run unique : les opérations aléatoires, le cache du
système et la charge de la machine font varier les résultats.

## Itération reprise du WAL (23 septembre 2026)

La validation et le rejeu du WAL lisent maintenant le journal séquentiellement
par blocs de 64 Kio, au lieu de plusieurs `pread` par petite frame. Le rejeu
réutilise ses chaînes de clé et de valeur. Un filtre de présence de 64 Kio,
construit et jeté pendant `Open`, saute la recherche sur disque d'une clé
**certainement absente**. Ses faux positifs suivent le chemin normal ; il ne
supprime donc aucune vérification nécessaire. Le format et la durabilité du
WAL ne changent pas.

Sur le même WAL de 181 588 611 octets (100 000 clés vivantes), cinq
réouvertures avec `-run`, workload C et `operationcount=0` prenaient
1,45–1,48 s avant cette itération. Avec la lecture par blocs et la
réutilisation des chaînes : 0,99–1,01 s après le premier run. Avec le filtre
en plus : 0,92–0,98 s après le premier run, soit environ 35 % de moins que
le point de départ. Dans le binaire profilé, l'ajout du filtre passe les
lectures du fichier de données de 165 339 à 92 583 appels et de 146 à 72 ms
cumulées ; les 150 148 écritures de reconstruction restent inchangées. Un
tampon de 256 Kio n'a pas amélioré la reprise par rapport à 64 Kio.

Commande de mesure, sur une base existante chargée avec 100 000 records :

```sh
./build/ycsb_hashkv -run -db hashkv \
  -P ../YCSB-cpp/workloads/workloadc -P ycsb.properties \
  -threads 1 -p recordcount=100000 -p operationcount=0 \
  -p hashkv.path=/tmp/ma-base.data -p hashkv.destroy=false \
  -p hashkv.stats=false
```

Vérification du débit normal sur la version finale, A–F avec 100 000
chargements et 100 000 opérations, quatre threads, cache de scan désactivé :

| Workload | Load (ops/s) | Run (ops/s) |
|---|---:|---:|
| A | 32 676 | 63 769 |
| B | 34 945 | 289 375 |
| C | 36 164 | 591 483 |
| D | 35 419 | 332 917 |
| E | 35 303 | 18 110 |
| F | 36 243 | 67 484 |

Ces six runs restent dans la variabilité des mesures précédentes ; le filtre
n'agit pas sur une création de base neuve. Les tests de crash, de checksum et
de frontière de bloc passent aussi sous ASan/UBSan.

## Itération scans et mémoire (23 septembre 2026)

Un cache direct des valeurs lues par `Scan` est maintenant **optionnel** :
`hashkv.scan_cache_slots=0` par défaut, ou une puissance de deux jusqu'à
32 768. Seules les valeurs de 4 Kio ou moins y entrent ; les écritures et
deletes invalident la région concernée. Aucun changement du format disque.

Sur E (50 000 chargements + 50 000 opérations, quatre threads), le cache à
32 768 slots donne 16 986–17 454 ops/s sur deux runs, contre
14 720–14 920 sans cache. Sur le profil plus court (20 000 + 20 000), les
`pread` passent d'environ 996 000 à 263 000, avec 742 003 hits pour
219 691 misses. Contrepartie mesurée avec `/usr/bin/time -l` : la mémoire
maximale passe d'environ **11 à 51 Mo**. Le cache reste donc désactivé par
défaut ; au pire, 32 768 valeurs de 4 Kio représentent environ 128 Mio,
plus les métadonnées. Un essai à 8 192 slots n'apportait qu'environ 3–4 %
sur E.

Série finale A–F avec le cache désactivé, 100 000/100 000 et quatre threads :

| Workload | Load (ops/s) | Run (ops/s) |
|---|---:|---:|
| A | 35 639 | 67 166 |
| B | 36 610 | 296 258 |
| C | 36 382 | 532 064 |
| D | 36 229 | 340 795 |
| E | 37 115 | 16 678 |
| F | 36 596 | 69 792 |

Commande pour reproduire l'option sur tous les workloads du script :

```sh
HASHKV_SCAN_CACHE_SLOTS=32768 ./bench_all.sh 50000 50000 4
```

Autre piste évaluée sans changement de code retenu : A à 100 000/100 000
passe d'environ 32 k load / 68 k run à quatre threads à 36 k / 75 k avec
huit, puis 37 k / 78 k avec seize. Le gain limité indique que la taille de
lot du WAL n'est plus à elle seule le plafond ; l'application des records au
fichier reste sérialisée. Augmenter aveuglément l'attente de formation des
lots n'a donc pas été retenu.

## Itération lecture/concurrence (23 septembre 2026)

Deux changements retenus sans modifier le format disque ni la garantie du
WAL :

- `Get` précharge au plus 1,5 Kio depuis le record. Si la valeur tient dans
  cette lecture, l'en-tête, la clé et la valeur sont obtenus en un `pread` ;
  une valeur plus grande utilise l'ancien chemin en deux lectures.
- Le `fsync` du WAL ne garde plus le verrou exclusif du fichier de données.
  Les lecteurs voient l'ancien état jusqu'à l'application du lot. Un verrou
  de commit sérialise WAL, `Flush` et checkpoint pour préserver leur ordre.

YCSB A–F, 100 000 chargements + 100 000 opérations, quatre threads, un run
par ligne :

| Workload | Load après itération (ops/s) | Run après itération (ops/s) | Run précédent (ops/s) |
|---|---:|---:|---:|
| A | 32 424 | 67 703 | 65 132 |
| B | 33 724 | 296 313 | 235 494 |
| C | 35 573 | 571 011 | 454 858 |
| D | 34 755 | 341 390 | 264 672 |
| E | 35 374 | 18 268 | 16 513 |
| F | 35 835 | 66 809 | 67 505 |

Pour vérifier les effets séparément et réduire le bruit des runs courts :

- C, 100 000 clés et **un million de lectures** : 429–434 k ops/s sans
  préchargement contre 502–586 k avec le tampon de 1,5 Kio (deux runs de
  chaque). Sur le profil 20 000 chargements + 100 000 lectures, les `pread`
  passent de 216 636 à 116 564.
- B, 100 000 clés et **un million d'opérations** : 253–258 k ops/s avec le
  verrou tenu pendant `fsync`, contre 295–297 k quand les lecteurs peuvent
  avancer (deux runs de chaque, préchargement actif dans les deux cas).
- F, 100 000 clés et 300 000 opérations : 64,6 k ops/s sur deux runs avec
  préchargement et verrou séparé, contre 56,0–61,5 k sans préchargement.
  Le léger recul de F dans le tableau A–F court ne se confirme donc pas sur
  ce test plus long.

Un tampon de 1,25 ou 2 Kio et la suppression de sa mise à zéro ont aussi été
essayés ; aucun gain reproductible n'a justifié de les retenir. Les tests de
reprise et le test concurrent `Get`/`Scan`/écritures/`Flush`/checkpoint
passent, y compris sous les sanitizers. Le coût de réouverture reste lié à la
taille du WAL tant que le checkpoint est manuel.

## WAL groupé et durable (23 septembre 2026)

Chaque `Put` et `Erase` confirmé appartient à un lot de WAL synchronisé par
`fsync` **avant** l'écriture des données. Les écrivains concurrents peuvent
partager un `fsync` ; le WAL du lot est écrit en un seul `pwrite`. Une fenêtre
maximale de 50 µs est utilisée quand la concurrence a été observée récemment,
et désactivée après 32 lots solitaires. Le binding YCSB verrouille les updates
par clé, et non plus globalement, afin que des clés indépendantes puissent
former un lot.

Les six workloads ont chacun chargé 100 000 records puis exécuté 100 000
opérations dans un nouveau fichier avec quatre threads :

```sh
./bench_all.sh 100000 100000 4
```

| Workload | Load groupé (ops/s) | Run groupé (ops/s) | Run WAL non groupé | Run sans WAL, historique |
|---|---:|---:|---:|---:|
| A | 33 864 | 65 132 | 26 577 | 124 004 |
| B | 34 618 | 235 494 | 167 637 | 319 293 |
| C | 35 032 | 454 858 | 434 071 | 465 848 |
| D | 33 787 | 264 672 | 194 894 | 361 934 |
| E | 34 774 | 16 513 | 13 268 | 17 550 |
| F | 34 107 | 67 505 | 32 630 | 100 757 |

Le load non groupé était de 16–18 k ops/s : il atteint maintenant 33–35 k.
Sur un profil A plus court (5 000 chargements + 5 000 opérations), le WAL
passe de 7 517 `fsync` / 318 ms à **1 924 `fsync` / 123 ms**. Avec un seul
thread sur A (10 000 + 10 000 opérations), désactiver l'attente inutile fait
passer le load de 7 761 à 16 074 ops/s et le run de 15 697 à 31 264 ops/s.
Ces essais sont des runs uniques distincts, pas des médianes. La durabilité
reste plus coûteuse que l'ancienne version sans WAL, surtout sur les mixes
d'écritures : le group commit amortit les synchronisations, il ne les
supprime pas. `Store::Checkpoint()` n'est pas inclus dans ces mesures.

Les tests simulent un fichier de données tronqué, une fin de WAL incomplète,
un arrêt de processus après `fsync` du WAL (opération seule et lot ordonné),
et une corruption au milieu du journal. Ils vérifient aussi la migration d'un
fichier sans WAL, le checkpoint et l'ordre des commits concurrents. Les
tests passent également sous ASan/UBSan et ThreadSanitizer.

L'allocateur conserve son efficacité spatiale : le test `space_bench 1000`
montre toujours **0 octet** de croissance après remplacement de grosses
valeurs par de petites. Dans `churn_bench 30000`, les 15 000 réinsertions
réutilisent 15 000 régions et prennent 0,70 s, `fsync` inclus.

## Historique sans WAL (22 septembre 2026)

Ces mesures utilisaient des I/O bufferisées, sans `fsync` par opération. Elles
ne donnent pas une garantie de récupération après crash.

Chaque workload a chargé **100 000 records**, puis exécuté **100 000
opérations** dans un nouveau fichier. Commande :

```sh
./bench_all.sh 100000 100000 4
```

| Workload | Mix principal | Load (ops/s) | Run (ops/s) |
|---|---|---:|---:|
| A | 50 % read, 50 % update | 64 203 | 124 004 |
| B | 95 % read, 5 % update | 64 781 | 319 293 |
| C | 100 % read | 64 587 | 465 848 |
| D | 95 % read, 5 % insert | 64 808 | 361 934 |
| E | 95 % scan, 5 % insert | 64 711 | 17 550 |
| F | 50 % read, 50 % read-modify-write | 64 020 | 100 757 |

Le scan E reste le plus coûteux : il lit les valeurs de la plage demandée sur
disque et sa première opération construit l'index trié en RAM. Le temps de run
E pour ces 100 000 opérations est de 5,70 s, contre 7,86 s avant cette
itération (17 550 contre 12 726 ops/s, soit ×1,38). Les autres workloads
restent proches de leurs mesures précédentes.

## Comparaisons avant/après aux mêmes tailles

Les trois premiers workloads ci-dessous utilisent les mêmes paramètres avant
et après : A/C ont 50 000 records et 50 000 opérations, E a 5 000 records et
1 000 opérations. E a été réduit dans la référence initiale car son ancien
scan complet rendait une charge plus grande impraticable.

| Mesure | Premier jet | Série d'optimisations précédente | Gain |
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
6. Mémoriser la taille de valeur dans l'index trié pour que chaque résultat
   de scan n'ait besoin que d'un `pread`. Sur E avec 5 000 records et 1 000
   opérations, les appels de lecture profilés passent de 104 579 à 57 849,
   et le temps cumulé de scan de 172 à 91 ms. Les débits de ces deux runs
   profilés sont 12 038 et 15 386 ops/s.
7. Découper les régions libres et fusionner les voisines. Après suppression de
   1 000 records de 8 Kio puis insertion de 4 000 records de 1 Kio, le
   fichier ne grandit plus, contre +3 198 000 octets auparavant. Le test de
   churn à taille fixe reste voisin de 0,035 s (0,036 s mesuré ici).

Un essai de dimensionnement de l'array RAM sur C (100 000 records, un million
de lectures, quatre threads) a donné 429 899 ops/s avec 65 536 buckets contre
496 665 avec 262 144 (+15,5 %). Cette option coûte 2 Mio de RAM au lieu de
0,5 Mio. Elle est disponible via `-p hashkv.buckets=262144` pour une base neuve,
mais le défaut n'est pas changé : le nombre de buckets fait partie du format
du fichier et un changement implicite empêcherait de rouvrir les bases déjà
créées.

Les scénarios de churn et de fragmentation restent exécutables avec :

```sh
make build/churn_bench
./build/churn_bench 30000
make build/space_bench
./build/space_bench 1000
```

Les workloads YCSB A–F utilisent ici des valeurs de taille stable et ne
mesurent presque pas l'allocateur. Les deux microbenchmarks le sollicitent
séparément. Leur temps actuel inclut toutefois les `fsync` du WAL et n'est
donc pas comparable aux chronos historiques ci-dessus. Les comparaisons YCSB
restent indicatives : elles ne sont pas des médianes de répétitions.

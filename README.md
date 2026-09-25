<div align="center">

# Hestia

<div align="center">
  <img align="center" src="https://img.shields.io/badge/version-1.0.0 (80e4f7f)-blue">
  <img align="center" src="https://img.shields.io/badge/java-8+-blue">
  <img align="center" src="https://img.shields.io/badge/redis-8 (JSON + Search)-red">
  <img align="center" src="https://img.shields.io/maintenance/yes/9999">
</div>

<br>

Stockez des objets Java dans Redis, partagés entre autant de processus que vous voulez, sans jamais penser à l'atomicité.
<br><br>
Écritures atomiques et idempotentes, fusion automatique des modifications concurrentes, caches locaux synchronisés, verrous distribués.

[Pourquoi](#pourquoi) • [Bench](#bench) • [Installation](#installation) • [Utilisation](#utilisation) • [Build](#build)

</div>

## Pourquoi

Partager un objet entre plusieurs processus avec Redis, c'est d'habitude choisir entre deux maux :

- **Relire, modifier, réécrire tout l'objet.** Simple, mais deux processus qui écrivent en même temps s'écrasent : une des deux modifications est perdue, sans erreur. Et chaque écriture renvoie tout l'objet, que Redis doit reparser en entier.
- **`WATCH` / `MULTI` ou un verrou autour de chaque écriture.** Correct, mais chaque conflit oblige à tout relire et tout renvoyer, et le débit s'effondre dès que plusieurs écrivains visent le même objet.

Hestia supprime ce choix. Vous modifiez vos objets Java normalement et vous appelez `save` :

- **Seul ce qui a changé part sur le réseau.** L'objet est comparé à l'état dans lequel il a été lu, et Hestia n'envoie que la différence.
- **Les modifications concurrentes fusionnent au lieu de s'écraser.** Les nombres partent en delta exact (`+100`, `-30`), les listes de valeurs sont fusionnées élément par élément, les champs marqués `@RedisJsonOverwrite` gardent la dernière valeur écrite.
- **Chaque écriture est atomique.** Le patch est appliqué par un seul script Lua, vérifié entièrement avant d'écrire : jamais d'état à moitié écrit, y compris quand Redis manque de mémoire.
- **Chaque écriture est idempotente.** Un retry après une coupure réseau ne s'applique jamais deux fois.
- **Les échanges sont regroupés automatiquement.** Les lectures simultanées partent dans un même pipeline, deux lectures du même objet n'en font qu'une, et les écritures partent en parallèle sur le pool de connexions, groupées en pipeline quand il est saturé.
- **Les caches locaux restent à jour.** Chaque processus peut garder une copie en mémoire, mise à jour après chaque écriture, versionnée pour ne jamais revenir en arrière, et réparée par un rafraîchissement incrémental.
- **Un verrou expiré ne peut plus écrire.** Une écriture faite sous un verrou perdu est refusée par Redis lui-même.

## Bench

Mesuré sur Redis 8.6.2 et Java 8, le client et Redis tournant dans deux conteneurs Linux sur le même réseau Docker (24 cœurs). Chaque valeur est la médiane de 5 exécutions. Les documents contiennent 150 entrées (16 Ko) ou 1 200 entrées (130 Ko). La colonne « CPU Redis » vient de `INFO commandstats` : c'est le temps que Redis passe réellement sur chaque écriture, là où un serveur partagé sature en premier.

### Mise à jour d'un champ, un seul écrivain

| Document | Approche | Envoyé | CPU Redis | p50 | p99 | Débit |
|---|---|---|---|---|---|---|
| 16 Ko | `JSON.SET` du document complet | 16,2 Ko | 108 µs | 0,42 ms | 0,67 ms | 2 247 ops/s |
| 16 Ko | **Hestia** | **164 o** | **40 µs** | **0,35 ms** | 0,68 ms | **2 565 ops/s** |
| 130 Ko | `JSON.SET` du document complet | 133,5 Ko | 739 µs | 1,63 ms | 2,81 ms | 571 ops/s |
| 130 Ko | **Hestia** | **165 o** | **49 µs** | **1,12 ms** | **1,96 ms** | **833 ops/s** |

Avec un seul écrivain et un réseau local sans limite de débit, la latence est proche sur un petit document. Mais Hestia envoie **100 fois moins de données** et coûte **2,7 à 15 fois moins de CPU à Redis**, et l'écart grandit avec la taille du document : le coût d'un patch ne dépend que de ce qui change.

### 16 écrivains sur des objets différents de 130 Ko

| Approche | Durée | Débit | CPU Redis occupé |
|---|---|---|---|
| `JSON.SET` du document complet | 3 858 ms | 1 037 ops/s | 92 % |
| **Hestia** | **1 067 ms** | **3 750 ops/s** | **20 %** |

Dès que plusieurs processus écrivent, Redis devient le goulot : réécrire des documents complets le sature à 92 %. Avec Hestia, **le même Redis encaisse 3,6 fois plus d'écritures** en restant à un cinquième de sa capacité. Ici, la limite de Hestia est le CPU du client (sérialiser et comparer 130 Ko), pas Redis.

### 8 écrivains concurrents sur le même objet (2 000 incréments)

| Approche | Valeur finale | Écritures perdues | Rejeux | Durée | Débit |
|---|---|---|---|---|---|
| Relire puis `JSON.SET` | 275 | **1 725** | 0 | 414 ms | 4 827 ops/s |
| `WATCH` / `MULTI` (optimiste) | 2 000 | 0 | 10 248 | 2 016 ms | 992 ops/s |
| **Hestia** | **2 000** | **0** | **0** | 525 ms | **3 808 ops/s** |

Relire puis réécrire va vite parce qu'il ne protège rien : **86 % des écritures sont perdues**, sans aucune erreur. La méthode optimiste est correcte mais rejoue chaque conflit (10 248 fois ici). Hestia est correcte sans aucun rejeu, et **près de 4 fois plus rapide** que la seule autre approche correcte.

### Envoi des écritures simultanées

Chaque écriture Hestia part directement sur une connexion libre du pool. Quand toutes les connexions sont occupées, les écritures en attente partent groupées en pipeline sur la prochaine connexion libérée. Comparaison avec les deux stratégies classiques, en débit (médiane de 5 exécutions, `WriteBenchmark`) :

| Charge | Réseau | Une commande par écriture | Un seul pipeline | **Hestia** |
|---|---|---|---|---|
| 64 écrivains, objets de 2 Ko | local | 12 081 ops/s | 15 744 ops/s | **20 448 ops/s** |
| 64 écrivains, objets de 2 Ko | +1 ms d'aller-retour | 7 313 ops/s | 10 771 ops/s | **15 987 ops/s** |
| 16 écrivains, objets de 130 Ko | local | 3 717 ops/s | 3 893 ops/s | **3 910 ops/s** |
| 16 écrivains, objets de 130 Ko | +1 ms d'aller-retour | 3 430 ops/s | 2 977 ops/s | 3 319 ops/s |

Avec beaucoup de petites écritures, Hestia envoie **2,2 fois plus** qu'une commande par écriture dès que Redis est à 1 ms. Avec peu d'écrivains, chaque écriture a sa propre connexion, comme en envoi direct : les trois stratégies se valent, et l'écart restant est dans le bruit de mesure. La latence de 1 ms est ajoutée par un proxy [toxiproxy](https://github.com/Shopify/toxiproxy) entre le client et Redis.

### Lecture de 2 000 objets depuis 16 threads

| Approche | Durée | Débit |
|---|---|---|
| Un `JSON.GET` par lecture (pool de connexions) | 56 ms | 35 716 ops/s |
| **Hestia** (lectures regroupées en pipeline) | **20 ms** | **97 974 ops/s** |
| Hestia avec `readFlushInterval` à 50 ms | 86 ms | 23 146 ops/s |
| **Hestia**, 2 000 lectures du même objet (dédupliquées) | 14 ms | 146 140 ops/s |

Chaque appelant reçoit sa propre instance, même quand la lecture a été partagée. Un délai de regroupement (`readFlushInterval`) ajoute sa durée à chaque lecture : il ne sert qu'à dédupliquer davantage quand beaucoup de lectures identiques arrivent en rafale.

### Reproduire

Le code est dans `src/bench`. `gradlew bench` démarre son propre Redis avec Testcontainers. Pour mesurer contre un Redis existant, passer son hôte et son port à `fr.paladium.hestia.bench.HestiaBenchmark` ; `WriteBenchmark` prend en plus un libellé.

Sous Docker Desktop (Windows, macOS), la redirection de ports ajoute une latence d'environ 40 ms aux requêtes de plus de 16 Ko, ce qui fausse les comparaisons. Pour des chiffres fiables, lancer le client dans un conteneur sur le même réseau que Redis, comme pour les résultats ci-dessus.

## Installation

### Prérequis

| Élément | Version |
|---|---|
| Java | 8 ou plus |
| Redis | 8 ou plus (modules JSON et Search inclus), ou Redis Stack 7 |
| Gson | 2.8.6 ou plus |

### Dépendance

```gradle
repositories {
    maven {
        url = "http://repository.palagitium.dev/artifactory/Paladium-DEVENV"
        credentials {
            username = System.getenv('MAVEN_REPO_USER') ?: project.findProperty('MAVEN_REPO_USER')
            password = System.getenv('MAVEN_REPO_PASS') ?: project.findProperty('MAVEN_REPO_PASS')
        }
    }
}

dependencies {
    implementation "fr.paladium:hestia:1.0.0"
}
```

Chaque version publie trois jars :

| Jar | Contenu | Quand l'utiliser |
|---|---|---|
| `hestia-1.0.0.jar` | Hestia seule, dépendances déclarées dans le POM | cas général, via Maven ou Gradle |
| `hestia-1.0.0-all.jar` | Hestia avec Jedis, commons-pool, org.json, slf4j et reflections relocalisés sous `fr.paladium.hestia.libs` | environnements sans gestion de dépendances, ou qui embarquent déjà une autre version de Jedis |
| `hestia-1.0.0-sources.jar` | sources | IDE |

Gson n'est jamais embarqué : il fait partie de l'API publique et doit être fourni par l'application.

## Utilisation

### Client

```java
final RedisClient client = RedisClient.create(RedisConfig.create("127.0.0.1")
    .gson(gson)
    .metrics(RedisMetricConfig.create("accounts"))
    .typeResolver(MyTypeRegistry::resolve));
```

| Option | Défaut | Rôle |
|---|---|---|
| `port` | `6379` | port Redis |
| `password` | aucun | mot de passe Redis |
| `database` | `0` | base Redis |
| `poolSize` | `16` | connexions simultanées maximum |
| `gson` | `new Gson()` | Gson utilisé pour lire et écrire vos objets. Il n'est pas modifié : Hestia en dérive une copie qui ignore les champs `@RedisJsonTransient`. |
| `metrics` | désactivé | métriques envoyées en TimeSeries Redis sous ce préfixe |
| `typeResolver` | identité | donne la classe réelle d'un objet polymorphe, pour que les annotations des sous-classes soient respectées |

### Modèle

```java
public class Account {

    private final String id;

    private long balance;
    @RedisIndex private String owner;
    @RedisJsonVersion private long version;
    @RedisJsonOverwrite private long lastLogin;
    @RedisJsonTransient private String displayName;

    @RedisJsonSnapshot
    private transient JsonElement snapshot;

}
```

| Annotation | Effet |
|---|---|
| `@RedisJsonVersion` | champ `long` de version, incrémenté à chaque sauvegarde. Obligatoire pour un cache. |
| `@RedisJsonSnapshot` | champ `transient JsonElement` qui garde l'état chargé de l'objet, base du diff. Recommandé : chaque instance fusionne alors ses propres modifications, et les sauvegardes d'instances différentes s'exécutent en parallèle. |
| `@RedisJsonOverwrite` | la valeur est écrasée au lieu d'être envoyée en delta. À mettre sur tout nombre **affecté** (`=`) plutôt qu'**accumulé** (`+=`) : horodatage, priorité, valeur recalculée. S'applique à tout ce qui est en dessous. |
| `@RedisJsonTransient` | le champ n'est pas écrit dans Redis, mais reste lu s'il y est encore et reste sérialisé partout ailleurs. À la différence de `transient`, qui l'exclut de toute sérialisation. |
| `@RedisIndex` | champ indexé par RediSearch. |

### Store

```java
final RedisStore<Account> accounts = RedisStore.create(client, RedisStoreConfig.create(Account.class, Account::getId));
accounts.index();

accounts.save(account);
accounts.fetch("42");
accounts.fetchAll();
accounts.find("@owner:{alice}");
accounts.delete(account);
```

`fetch*` et `find` vont dans Redis et renvoient un `CompletableFuture`. Les clés sont `account:<id>` : le nom du store vient de la classe, en minuscules.

| Option de `RedisStoreConfig` | Défaut | Rôle |
|---|---|---|
| `threads` | `128` | threads du store |
| `batchSize` | `100` | documents par `JSON.MGET` |
| `patchAttempts` | `5` | tentatives d'un patch en cas d'erreur transitoire |
| `patchBackoff` | `250 ms` | attente avant la première nouvelle tentative, doublée ensuite |
| `readFlushInterval` | `0` | délai maximum avant l'envoi d'une lecture. À `0`, une lecture part immédiatement si rien n'est en cours, et les lectures arrivées pendant un envoi partent ensemble au suivant. Au-delà, les lectures attendent ce délai pour être regroupées et dédupliquées. |
| `writeFlushInterval` | `0` | délai maximum avant l'envoi d'une écriture. À `0`, une écriture part directement sur le thread qui sauvegarde tant qu'une connexion est libre, et le surplus part groupé en pipeline. Au-delà, les écritures attendent ce délai pour partir groupées. |
| `keyFilter` | id sans `:` | clés prises en compte par `fetchAll` et `fetchVersions` |

Les `RedisStoreListener` permettent d'annuler une sauvegarde ou une suppression (`onPreSave`, `onPreDelete`), d'initialiser un objet chargé (`onPostLoad`) ou de réagir après écriture (`onPostSave`, `onPostDelete`).

### Cache

```java
final RedisCache<Account> cache = RedisCache.create(accounts, RedisCacheConfig.create());
cache.start().join();

cache.get("42");
cache.getAll();
```

`get` et `getAll` lisent la mémoire locale, sans appel réseau. Les valeurs du cache sont en lecture seule : pour modifier un objet, le relire avec `fetch` puis le sauvegarder. Les `RedisCacheListener` sont notifiés après chaque mise à jour (`onPostUpdate`) ou suppression (`onPostRemove`). Le cache se rafraîchit aussi toutes les minutes (`refreshInterval`), en ne relisant que les objets dont la version a changé.

### Transport

Le cache ne dépend d'aucun système de messagerie : il publie et reçoit des `CacheMessage` (`id`, `json`, `version`, `origin`) à travers l'interface `CacheTransport`.

```java
public interface CacheTransport extends AutoCloseable {

    public void close();
    public void publish(final @NonNull CacheMessage message);
    public void subscribe(final @NonNull Consumer<CacheMessage> consumer);

}
```

Sans configuration, `RedisCacheTransport` utilise le pub/sub Redis sur le canal `<store>:sync`. Pour passer par un autre bus (Kafka, RabbitMQ, NATS…), il suffit d'implémenter l'interface :

```java
RedisCache.create(accounts, RedisCacheConfig.create().transport(new KafkaCacheTransport(producer, consumer)));
```

Un message dont la `json` est `null` signifie une suppression. Les messages émis par le cache lui-même sont ignorés grâce à `origin`.

### Verrous

```java
client.getLock().withLockWaiting("account:transfer:" + id, token -> accounts.fetch(id).thenCompose(account -> {
    account.setBalance(account.getBalance() - amount);
    return accounts.save(account, token);
}));
```

Passer le `token` à `save` active le fencing : si le verrou a expiré entre-temps (pause GC, coupure réseau), l'écriture est refusée par Redis et la sauvegarde échoue avec `RedisLockLostException`.

À l'arrêt, fermer dans l'ordre : `cache.close()`, `accounts.close()`, `client.close()`.

## Build

```sh
gradlew test              # Tests unitaires, sans Redis ni Docker.
gradlew integrationTest   # Tests d'intégration contre Redis 8 (Docker requis).
gradlew bench             # Bench contre Redis 8 (Docker requis).
gradlew build             # Tests unitaires et d'intégration, puis les jars dans build/libs.
gradlew publish           # Publication sur le dépôt Maven.
```

Gradle tourne en Java 17 et compile en Java 8. Si le JDK 8 local n'est pas détecté, ajouter `-Porg.gradle.java.installations.paths=<chemin du JDK 8>`, sinon Gradle le télécharge.

## Release

Chaque push lance le workflow `Build` : tests unitaires, tests d'intégration, construction et vérification de la publication, avec un rapport de tests dans l'onglet Checks.

Pour publier une version, créer une release GitHub avec un tag `vX.Y.Z`. Le workflow `Release` rejoue les deux suites de tests, construit les jars avec cette version, les publie, les attache à la release puis met à jour la version dans `build.gradle` et `README.md` sur `main`. Secrets requis : `MAVEN_REPO_USER` et `MAVEN_REPO_PASS`.
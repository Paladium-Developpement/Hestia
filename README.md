<div align="center">

# Hestia

<div align="center">
  <img align="center" src="https://img.shields.io/badge/version-1.1.0 (e6964f2)-blue">
  <img align="center" src="https://img.shields.io/badge/java-8+-blue">
  <img align="center" src="https://img.shields.io/badge/redis-8 (JSON + Search)-red">
  <img align="center" src="https://img.shields.io/maintenance/yes/9999">
</div>

<br>

Stockez des objets Java dans Redis, partagés entre autant de processus que vous voulez : plus rapide que Redis utilisé à la main, et sans jamais perdre une écriture.
<br><br>
Écritures atomiques, fusion automatique des modifications concurrentes, caches locaux synchronisés, verrous distribués.

[Pourquoi](#pourquoi) • [Bench](#bench) • [Utilisation](#utilisation) • [Build](#build)

</div>

## Pourquoi

Vous modifiez vos objets Java normalement, vous appelez `save`, et Hestia s'occupe du reste, plus vite qu'à la main :

- **3,6× plus d'écritures et 2,7× plus de lectures** qu'avec des `JSON.SET` / `JSON.GET` classiques.
- **15× moins de CPU Redis** : seul ce qui change part sur le réseau, pas l'objet entier.
- **Aucune écriture perdue** : les modifications de plusieurs processus fusionnent au lieu de s'écraser.
- **Atomique** : jamais d'état à moitié écrit, même en cas de coupure.
- **Caches locaux synchronisés** entre tous les processus.
- **Verrous distribués** qui refusent toute écriture une fois expirés.

## Bench

| | Redis classique | Hestia |
|---|---|---|
| Écritures perdues | 86 % | **0 %** |
| Débit en écriture | 1 037 ops/s | **3 750 ops/s** |
| Débit en lecture | 35 716 ops/s | **97 974 ops/s** |
| Taille d'une écriture | 16 Ko | **164 o** |
| CPU Redis par écriture | 739 µs | **49 µs** |
| Charge Redis | 92 % | **20 %** |

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
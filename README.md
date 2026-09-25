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

[Pourquoi](#pourquoi) • [Bench](#bench) • [Utilisation](#utilisation) • [Build](#build)

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

Comparé à l'usage classique de Redis (relire puis réécrire l'objet avec `JSON.SET`) :

| | Sans Hestia | Avec Hestia |
|---|---|---|
| 8 écrivains simultanés sur le même objet | **86 % des écritures perdues** | **0 perte**, 4× plus rapide que `WATCH` / `MULTI` |
| Données envoyées pour changer un champ | 16 Ko | **164 octets** |
| CPU Redis par écriture (objet de 130 Ko) | 739 µs | **49 µs** |
| 16 écrivains en continu | Redis saturé à 92 % | **3,6× plus d'écritures**, Redis à 20 % |
| Lecture de 2 000 objets | 35 716 ops/s | **97 974 ops/s** |

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
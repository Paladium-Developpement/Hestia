<div align="center">

# Hestia

<div align="center">
  <img align="center" src="https://img.shields.io/badge/version-1.0.0 (3b80a25)-blue">
  <img align="center" src="https://img.shields.io/badge/java-8+-blue">
  <img align="center" src="https://img.shields.io/badge/redis-8 (JSON + Search)-red">
  <img align="center" src="https://img.shields.io/maintenance/yes/9999">
</div>

<br>

Shared Object Storage pour services Java parallèles, adossé à Redis.
<br><br>
Patchs atomiques et idempotents, caches synchronisés versionnés et verrous distribués. Aucune dépendance Minecraft.

[Principe](#principe) • [Installation](INSTALLATION.md) • [Utilisation](#utilisation) • [Transport](#transport) • [Build](#build)

</div>

## Principe

Hestia stocke des objets Java complexes en JSON dans Redis et permet à plusieurs services de les modifier **en même temps** sans jamais perdre d'écriture.

- **Diff → patch** : à chaque sauvegarde, seul ce qui a changé depuis le dernier état connu de l'objet est envoyé.
- **Fusion sans perte** : les nombres partent en delta exact (`+100`, `-30`), les listes de valeurs sont fusionnées élément par élément. Deux serveurs qui modifient le même objet voient leurs changements s'additionner.
- **Atomique** : un patch est appliqué par un seul script Lua. Il est vérifié entièrement avant d'écrire, donc jamais à moitié (y compris en cas d'OOM).
- **Idempotent** : chaque patch porte un identifiant. Un retry après une coupure réseau ne s'applique jamais deux fois.
- **Versionné** : chaque objet porte un numéro de version, utilisé par les caches pour ne jamais remplacer une donnée récente par une ancienne.
- **Caches synchronisés** : chaque service garde une copie locale, tenue à jour par pub/sub (ou le transport de votre choix) et réparée par un rafraîchissement périodique incrémental.
- **Verrous avec fencing** : une écriture faite sous un verrou expiré est refusée par Redis lui-même.

## Installation

Voir [INSTALLATION.md](INSTALLATION.md) : dépendance Gradle, installation sur un serveur Minecraft, configuration et dépannage.

## Utilisation

### Client

```java
final RedisClient client = RedisClient.create(RedisConfig.create("127.0.0.1")
    .gson(gson)
    .metrics(RedisMetricConfig.create("faction"))
    .typeResolver(JsonBindingAdapterFactory::resolve));
```

`typeResolver` indique la classe réelle d'un objet polymorphe (`@type`), pour que les annotations des sous-classes soient respectées.

### Modèle

```java
public class Faction {

    private final UUID uuid;

    private long balance;
    @RedisIndex private String name;
    @RedisJsonVersion private long version;
    @RedisJsonOverwrite private long lastSeen;
    @RedisJsonTransient private String description;

    @RedisJsonSnapshot
    private transient JsonElement snapshot;

}
```

| Annotation | Effet |
|---|---|
| `@RedisJsonVersion` | champ `long` de version, incrémenté à chaque sauvegarde. Obligatoire pour un cache. |
| `@RedisJsonSnapshot` | champ `transient JsonElement` qui garde l'état chargé de l'objet, base du diff. Recommandé dès que plusieurs copies d'un même objet peuvent coexister. |
| `@RedisJsonOverwrite` | la valeur est écrasée au lieu d'être envoyée en delta. À mettre sur tout nombre **affecté** (`=`) plutôt qu'**accumulé** (`+=`) : timestamp, priorité, valeur recalculée. S'applique à tout ce qui est en dessous. |
| `@RedisJsonTransient` | le champ n'est pas écrit dans Redis, mais reste lu s'il y est encore et reste sérialisé partout ailleurs (paquets, copies). À la différence de `transient`, qui l'exclut de toute sérialisation. |
| `@RedisIndex` | champ indexé par RediSearch. |

### Store

```java
final RedisStore<Faction> factions = RedisStore.create(client, RedisStoreConfig.create(Faction.class, faction -> faction.getUuid().toString()));

factions.save(faction);
factions.fetch(uuid.toString());
factions.fetchAll();
factions.find("@name:{paladium}");
factions.delete(faction);
```

Les clés sont `faction:<id>` (le nom du store vient de la classe, en minuscules). Les lectures sont regroupées et dédupliquées toutes les 50 ms, chaque appelant reçoit sa propre instance. Les écritures d'une même clé sont exécutées dans l'ordre.

Les listeners (`RedisStoreListener`) permettent d'annuler une sauvegarde (`onPreSave`), d'initialiser un objet chargé (`onPostLoad`) ou de réagir après écriture (`onPostSave`, `onPostDelete`).

### Cache

```java
final RedisCache<Faction> cache = RedisCache.create(factions, RedisCacheConfig.create());
cache.start().join();

cache.get(uuid.toString());
cache.getAll();
```

Les valeurs du cache sont en lecture seule : pour modifier un objet, le relire avec `store.fetch` puis le sauvegarder. Les listeners (`RedisCacheListener`) sont notifiés après chaque mise à jour (`onPostUpdate`) ou suppression (`onPostRemove`).

### Verrous

```java
client.getLock().withLockWaiting("faction:bank:" + uuid, token -> factions.fetch(id).thenCompose(faction -> {
    faction.setBalance(faction.getBalance() - amount);
    return factions.save(faction, token);
}));
```

Passer le `token` à `save` active le fencing : si le verrou a expiré entre-temps, l'écriture est refusée (`RedisLockLostException`).

## Transport

Le cache ne dépend d'aucun système de messagerie : il publie et reçoit des `CacheMessage` (`id`, `json`, `version`, `origin`) à travers l'interface `CacheTransport`.

```java
public interface CacheTransport extends AutoCloseable {

    public void close();
    public void publish(final @NonNull CacheMessage message);
    public void subscribe(final @NonNull Consumer<CacheMessage> consumer);

}
```

Sans configuration, `RedisCacheTransport` utilise le pub/sub Redis sur le canal `<store>:sync`. Pour passer par RabbitMQ ou un autre bus, il suffit d'implémenter l'interface et de la donner au cache :

```java
RedisCache.create(factions, RedisCacheConfig.create().transport(new RabbitCacheTransport(network)));
```

Un message dont la `json` est `null` signifie une suppression. Les messages émis par le cache lui-même sont ignorés grâce à `origin`.

## Build

```sh
gradlew test              # Tests unitaires, sans Redis ni Docker.
gradlew integrationTest   # Tests d'intégration contre Redis 8 (Docker requis, via Testcontainers).
gradlew build             # Les deux, puis les jars dans build/libs.
gradlew publish           # Met en ligne sur repository.palagitium.dev
```

## Release

Chaque push lance le workflow `Build` : tests unitaires, tests d'intégration, construction des jars, avec un rapport de tests dans l'onglet Checks. Pour publier une version, créer une release GitHub avec un tag `vX.Y.Z` : le workflow `Release` rejoue les deux suites de tests, construit les jars avec cette version, les publie sur l'Artifactory, les attache à la release puis met à jour la version dans `build.gradle`, `README.md` et `INSTALLATION.md` sur `main`.

Secrets requis sur le repo : `MAVEN_REPO_USER` et `MAVEN_REPO_PASS`.

Le projet cible Java 8. Si Gradle ne trouve pas de JDK 8 local, il le télécharge automatiquement ; sinon, indiquer son chemin avec `-Porg.gradle.java.installations.paths=<chemin>`.
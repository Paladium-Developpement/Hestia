<div align="center">

# Hestia

<div align="center">
  <img align="center" src="https://img.shields.io/badge/version-1.0.0-blue">
  <img align="center" src="https://img.shields.io/badge/java-8+-blue">
  <img align="center" src="https://img.shields.io/badge/redis-8 (JSON + Search)-red">
  <img align="center" src="https://img.shields.io/maintenance/yes/9999">
</div>

<br>

Shared Object Storage pour services Java parallèles, adossé à Redis.
<br><br>
Patchs atomiques et idempotents, caches synchronisés versionnés et verrous distribués. Aucune dépendance Minecraft.

[Principe](#principe) • [Gradle](#gradle) • [Serveur Minecraft](#serveur-minecraft) • [Utilisation](#utilisation) • [Build](#build)

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

## Gradle

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
    compile "fr.paladium:hestia:1.0.0"
}
```

Deux jars sont publiés :

| Jar | Contenu | Usage |
|---|---|---|
| `hestia-1.0.0.jar` | Hestia seule, dépendances Maven classiques | services Java standards |
| `hestia-1.0.0-all.jar` | Hestia + Jedis, commons-pool, org.json, slf4j, reflections (relocalisés sous `fr.paladium.hestia.libs`) | serveurs Minecraft |

Gson n'est jamais embarqué : il est fourni par l'environnement (2.8.6 minimum).

## Serveur Minecraft

Hestia n'est **pas** un mod. Le jar `-all` se place dans `libraries/` et s'ajoute au classpath de lancement :

```sh
java -cp "forge.jar:libraries/hestia-1.0.0-all.jar" net.minecraft.launchwrapper.Launch --tweakClass cpw.mods.fml.common.launcher.FMLServerTweaker
```

Sous Windows, le séparateur est `;`. Hestia doit être présente avant de déployer un mod qui l'utilise.

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
| `@RedisJsonTransient` | le champ n'est pas écrit dans Redis (avec `RedisJsonTransientExclusionStrategy` sur le Gson). |
| `@RedisIndex` | champ indexé par RediSearch. |

### Store

```java
final SharedStore<Faction> factions = SharedStore.create(client, SharedStoreConfig.create(Faction.class, faction -> faction.getUuid().toString()));

factions.save(faction);
factions.get(uuid.toString());
factions.getAll();
factions.findOne("@name:{paladium}");
factions.delete(faction);
```

Les clés sont `faction:<id>` (le nom du store vient de la classe, en minuscules). Les lectures sont regroupées et dédupliquées toutes les 50 ms, chaque appelant reçoit sa propre instance. Les écritures d'une même clé sont exécutées dans l'ordre.

Les listeners (`SharedStoreListener`) permettent d'annuler une sauvegarde (`onPreSave`), d'initialiser un objet chargé (`onLoad`) ou de réagir après écriture (`onPostSave`, `onPostDelete`).

### Cache

```java
final SharedCache<Faction> cache = SharedCache.create(factions, SharedCacheConfig.create());
cache.start().join();

cache.get(uuid.toString());
cache.getAll();
```

Les valeurs du cache sont en lecture seule : pour modifier un objet, le relire avec `store.get` puis le sauvegarder. Par défaut la synchronisation passe par le pub/sub Redis (`faction:sync`). Un autre transport (RabbitMQ…) s'implémente avec `SharedCacheTransport`.

### Verrous

```java
client.getLock().withLockWaiting("faction:bank:" + uuid, token -> factions.get(id).thenCompose(faction -> {
    faction.setBalance(faction.getBalance() - amount);
    return factions.save(faction, token);
}));
```

Passer le `token` à `save` active le fencing : si le verrou a expiré entre-temps, l'écriture est refusée (`RedisLockLostException`).

## Build

```sh
gradlew build         # Compile, teste et construit les jars dans build/libs.
gradlew test          # Lance les tests (Docker requis, Redis 8 via Testcontainers).
gradlew publish       # Met en ligne sur repository.palagitium.dev
```

Le projet cible Java 8. Si Gradle ne trouve pas de JDK 8 local, il le télécharge automatiquement ; sinon, indiquer son chemin avec `-Porg.gradle.java.installations.paths=<chemin>`.
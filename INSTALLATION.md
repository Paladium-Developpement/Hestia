# Installation

Ce guide couvre l'ajout de Hestia à un projet, son installation sur les serveurs et sa première configuration.

## Prérequis

| Élément | Version | Remarque |
|---|---|---|
| Java | 8 ou plus | la lib est compilée en Java 8 |
| Redis | 8 ou plus | les modules JSON et Search sont inclus dans Redis 8. En Redis 7, installer Redis Stack. |
| Gson | 2.8.6 ou plus | jamais embarqué dans Hestia, doit être fourni par l'environnement |

## 1. Ajouter la dépendance

### Mod Forge (ForgeGradle 1.2, Gradle 2.x)

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

Déclarer Hestia en `compile` et **pas** en `shadow` : elle ne doit pas être embarquée dans le jar du mod, elle est fournie par le serveur (étape 2).

### Service Java classique (Gradle récent)

```gradle
dependencies {
    implementation "fr.paladium:hestia:1.0.0"
}
```

Jedis, commons-pool et Gson arrivent en dépendances transitives.

### Jars publiés

| Jar | Contenu | Usage |
|---|---|---|
| `hestia-1.0.0.jar` | Hestia seule | dépendance Maven des projets |
| `hestia-1.0.0-all.jar` | Hestia + Jedis, commons-pool, org.json, slf4j, reflections, relocalisés sous `fr.paladium.hestia.libs` | serveurs Minecraft |
| `hestia-1.0.0-sources.jar` | sources | IDE |

Les trois sont attachés à chaque release GitHub.

## 2. Installer sur un serveur Minecraft

Hestia n'est pas un mod : elle ne va pas dans `mods/` mais sur le classpath de lancement, en un seul exemplaire partagé par tous les mods.

1. Copier `hestia-1.0.0-all.jar` dans le dossier `libraries/` du serveur.
2. Remplacer `java -jar forge.jar` par un lancement explicite (l'option `-cp` est ignorée avec `-jar`) :

```sh
java -cp "forge.jar:libraries/hestia-1.0.0-all.jar" net.minecraft.launchwrapper.Launch --tweakClass cpw.mods.fml.common.launcher.FMLServerTweaker
```

Sous Windows, le séparateur de classpath est `;` au lieu de `:`.

3. Déployer Hestia **avant** tout mod qui l'utilise. Sinon, le mod plante au démarrage avec `NoClassDefFoundError: fr/paladium/hestia/...`.

Pour mettre Hestia à jour, remplacer le jar dans `libraries/` et redémarrer.

## 3. Configurer

```java
final RedisClient client = RedisClient.create(RedisConfig.create("127.0.0.1")
    .port(6379)
    .password(null)
    .database(0)
    .poolSize(16)
    .gson(gson)
    .metrics(RedisMetricConfig.create("faction"))
    .typeResolver(JsonBindingAdapterFactory::resolve));

final SharedStore<Faction> factions = SharedStore.create(client, SharedStoreConfig.create(Faction.class, faction -> faction.getUuid().toString()));
factions.index();

final SharedCache<Faction> cache = SharedCache.create(factions, SharedCacheConfig.create());
cache.start().join();
```

| Option | Défaut | Rôle |
|---|---|---|
| `port` | `6379` | port Redis |
| `password` | aucun | mot de passe Redis |
| `database` | `0` | base Redis |
| `poolSize` | `16` | connexions simultanées maximum |
| `gson` | `new Gson()` | Gson utilisé pour lire et écrire les objets. Ajouter `RedisJsonTransientExclusionStrategy.INSTANCE` pour activer `@RedisJsonTransient`. |
| `metrics` | désactivé | envoie des métriques en TimeSeries Redis sous ce préfixe |
| `typeResolver` | identité | donne la classe réelle d'un objet polymorphe |
| `SharedStoreConfig.threads` | `128` | threads du store |
| `SharedStoreConfig.batchSize` | `100` | documents par `JSON.MGET` |
| `SharedStoreConfig.flushInterval` | `50 ms` | fréquence de regroupement des lectures |
| `SharedStoreConfig.patchAttempts` | `5` | tentatives d'un patch en cas d'erreur transitoire |
| `SharedStoreConfig.keyFilter` | id sans `:` | clés prises en compte par `fetchAll` et `fetchVersions` |
| `SharedCacheConfig.refreshInterval` | `1 min` | rafraîchissement incrémental du cache |
| `SharedCacheConfig.transport` | pub/sub Redis | transport de synchronisation entre services |

À l'arrêt, fermer dans l'ordre : `cache.close()`, `factions.close()`, `client.close()`.

## 4. Vérifier

- Au démarrage, `RedisClient.create` échoue immédiatement avec `RedisConnectionException` si Redis est injoignable.
- `redis-cli MONITOR` montre des `EVAL` à chaque sauvegarde et des `PUBLISH <store>:sync` quand le cache est actif.
- Avec `metrics`, les clés `<préfixe>:<store>.save.success`, `<store>.fetch.total`, etc. apparaissent dans Redis.

## Dépannage

| Symptôme | Cause | Solution |
|---|---|---|
| `NoClassDefFoundError: fr/paladium/hestia/...` | le jar n'est pas sur le classpath | vérifier la commande de lancement et le dossier `libraries/` |
| `NoSuchMethodError: com.google.gson.JsonParser.parseString` | Gson trop ancien (Forge 1.7.10 fournit 2.2.4) | fournir Gson 2.8.6 ou plus sur le classpath |
| `SLF4J: Defaulting to no-operation (NOP) logger` | slf4j est relocalisé dans le jar `-all` | sans conséquence, les logs internes de Jedis sont désactivés |
| `find` ne trouve rien | index absent ou nom de store différent de la classe | appeler `store.index()` et garder le nom par défaut du store |
| `IllegalArgumentException: A shared cache requires a @RedisJsonVersion field` | le modèle n'a pas de champ version | ajouter `@RedisJsonVersion private long version;` |
| `RedisLockLostException` | le verrou a expiré avant l'écriture | allonger le TTL du verrou ou raccourcir la section critique |

## Développer sur Hestia

```sh
gradlew build
```

- Gradle tourne en Java 17 et compile en Java 8. Si le JDK 8 local n'est pas détecté, ajouter `-Porg.gradle.java.installations.paths=<chemin du JDK 8>`. Sinon Gradle le télécharge.
- Les tests d'intégration démarrent `redis:8.6.2` via Testcontainers : Docker doit tourner.
- `gradlew eclipse` génère le projet Eclipse.
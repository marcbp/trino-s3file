# Plugin Trino Java – squelette de départ

Ce dépôt fournit l'ossature minimale pour démarrer le développement d'un plugin Trino écrit en Java.

## Pré-requis

- Java 21 (version utilisée par l'image Docker fournie)
- Apache Maven 3.8+
- Trino 476 côté runtime (l'image Docker embarque `trinodb/trino:476`)

## Construire le plugin

```bash
mvn clean package
```

L'emballage `trino-plugin` produit une archive ZIP dans `target/` contenant le dossier de plugin prêt à copier dans `<TRINO_HOME>/plugin/`.

## Tester avec Docker Compose

Un environnement Trino autonome est disponible via `docker-compose.yml`. Il embarque le plugin construit durant l'image.

```bash
# Construire l'image et démarrer Trino
docker compose up --build
```

Par défaut l'interface Trino Web UI est exposée sur http://localhost:8080. Les fichiers de configuration Trino se trouvent dans `docker/etc`. Le catalogue `s3_file` est livré clé en main (`docker/etc/catalog/s3_file.properties`) afin que la table function soit immédiatement accessible.

### Lancer le client CLI

Un script utilitaire télécharge et démarre automatiquement `trino-cli` (version 476) pointé sur l'instance locale :

```bash
./scripts/trino-cli.sh
```

Variables utiles :
- `TRINO_CLI_SERVER` pour changer l'URL du coordinator (par défaut `http://localhost:8080`).
- `TRINO_CLI_JAR` pour réutiliser un binaire déjà téléchargé.
- `TRINO_CLI_JAVA_OPTS` pour ajouter des options JVM.

## Utiliser la table function CSV

Le plugin expose la table function `system.test` qui lit un fichier CSV hébergé sur S3. L'appelant fournit le chemin S3 via l'argument `path` et peut surcharger le délimiteur via `delimiter` (par défaut `';'`) :

```sql
SELECT *
FROM TABLE(
    s3_file.system.test(
        path => 's3://mybucket/data.csv',
        delimiter => ';'
    )
);
```

### Format attendu du CSV

- La première ligne du fichier contient uniquement les noms de colonnes, séparés par le délimiteur choisi (par défaut `;`).
- Toutes les valeurs sont lues sous forme de chaînes (`VARCHAR`). Les conversions éventuelles sont à réaliser dans la requête Trino.
- Les lignes suivantes contiennent les données, séparées par le même délimiteur (gestion basique des guillemets via OpenCSV).
- Les identifiants AWS doivent être accessibles depuis l'environnement Trino (variables d'environnement, profil par défaut, IAM role…).

Exemple de fichier `données.csv` :

```
id;nom;actif
1;Alice;true
2;Bob;false
```

Le résultat de la requête reproduira ces colonnes avec les types correspondants (VARCHAR).

### Scénario de test local avec MinIO

Le `docker-compose.yml` démarre également un serveur S3 local (MinIO) alimenté par un bucket de démonstration.

1. Construire et démarrer l'environnement :
   ```bash
   docker compose up --build
   ```
2. Déposer un CSV dans MinIO, par exemple :
   ```bash
   aws --endpoint-url http://localhost:9000 \
       s3 mb s3://mybucket
   aws --endpoint-url http://localhost:9000 \
       s3 cp docker/csv/example.csv s3://mybucket/data.csv
   ```

3. Vérifier que les variables d'environnement AWS sont visibles par Trino (cf. `docker-compose.yml`).
   - Par défaut : `AWS_ACCESS_KEY_ID=minio`, `AWS_SECRET_ACCESS_KEY=test`, `AWS_REGION=us-east-1`, `AWS_S3_ENDPOINT=http://minio:9000`.
4. Interroger Trino :
   ```sql
   SELECT *
   FROM TABLE(s3_file.system.test(path => 's3://mybucket/data.csv'));
   ```

Le résultat renvoie les trois lignes du CSV `example.csv` stocké sur MinIO.
## Structure du projet

- `pom.xml` : configuration Maven, dépendances Trino (SPI, toolkit) et packaging spécifique.
- `src/main/java/com/example/trino/s3file/S3FilePlugin` : classe principale implémentant `io.trino.spi.Plugin`.
- `src/main/java/com/example/trino/s3file/S3FileConnectorFactory` : fabrique du connecteur servant la table function.
- `src/main/java/com/example/trino/s3file/S3FileConnector` : connecteur minimal exposant la table function et son `FunctionProvider`.
- `src/main/java/com/example/trino/s3file/S3FileMetadata` / `S3FileTransactionHandle` : implémentations de support requises par le connecteur.
- `src/main/java/com/example/trino/s3file/functions/*` : implémentation complète de la table function `system.test` et de ses processeurs.
- `src/main/resources/trino-plugin.properties` : métadonnées du plugin (nom, dépendances éventuelles).
- `src/test/java/com/example/trino/s3file/S3FilePluginTest` : exemple de tests JUnit 5 pour vérifier le wiring de base.

## Étapes suivantes

1. Étendre `S3FileConnector` pour brancher vos services Trino réels (metadata, split manager, page source, etc.).
2. Introduire un `ConnectorHandleResolver` si vos handles personnalisés doivent être matérialisés côté coordinator.
3. Ajouter les implémentations concrètes (`ConnectorMetadata`, `ConnectorSplitManager`, etc.) en vous appuyant sur `trino-plugin-toolkit`.
4. Enrichir `trino-plugin.properties` et ajouter `etc/catalog/<catalog-name>.properties` si vous souhaitez livrer un catalogue de démonstration.
5. Compléter la suite de tests (unitaires et d'intégration) avant de packager et publier le plugin.

## Ressources utiles

- Documentation officielle : https://trino.io/docs/current/develop/index.html
- Exemples dans le projet open-source Trino (`plugin/` directory) pour s'inspirer de la structure des connecteurs existants.

Bon développement !

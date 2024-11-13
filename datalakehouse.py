# Databricks notebook source
# MAGIC %md
# MAGIC # Chargement des données Covid19

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connexion DataBricks avec Azure storage

# COMMAND ----------

new_sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-08-04T18:26:58Z&st=2024-11-10T11:26:58Z&spr=https,http&sig=ANR8pgKvpaMYsbLa7Xb6uHhGUY7SEXFNdHMj20o9n8s%3D"

storage_account_name = "conatinerdemo23"
containers = {
    "bronze": "/mnt/bronze",
    "silver": "/mnt/silver",
    "gold": "/mnt/gold"
}

def remount_container(container_name, mount_point, sas_token):
    # Démonter le point de montage s'il existe
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.unmount(mount_point)
            print(f"Point de montage {mount_point} démonté avec succès.")
        except Exception as e:
            print(f"Erreur lors du démontage de {mount_point} : {e}")
    
    # Remonter le conteneur 
    try:
        dbutils.fs.mount(
            source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
            mount_point=mount_point,
            extra_configs={f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
        )
        print(f"Montage réussi de {container_name} sur {mount_point} avec le nouveau SAS.")
    except Exception as e:
        print(f"Erreur lors du montage de {container_name} : {e}")

for container_name, mount_point in containers.items():
    remount_container(container_name, mount_point, new_sas_token)

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/inputs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chargement de Fichier parquet dans dataframe Covid_df

# COMMAND ----------

Covid_df=spark.read.parquet('/mnt/bronze/inputs/bing_covid-19_data.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Affichage des données limite de 5 champs dans Dataframe

# COMMAND ----------

display(Covid_df.limit(5)) 

# COMMAND ----------

# MAGIC %md
# MAGIC # Exemple simple de traitement des données

# COMMAND ----------

# MAGIC %md
# MAGIC **> filtre pour exclure ce qui mondial et supprimer les colonnes iso2,iso3,iso_subdivison**

# COMMAND ----------

Covid_update_df=(Covid_df.filter('country_region !="worldwide"')
                         .drop('iso','iso3','iso_subdivision')
                         .drop_duplicates(subset=['id'])
                )

# COMMAND ----------

display(Covid_update_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #  Sauvegarde du dataframe covide_update_df au format delta lake

# COMMAND ----------

delta_path='/mnt/silver/covid_data'
Covid_update_df.write.mode('overwrite').format('delta').save(delta_path)

# COMMAND ----------

dbutils.fs.ls('/mnt/silver/')

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS covid_data USING DELTA LOCATION '{delta_path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `hive_metastore`; select * from `default`.`covid_data` limit 100;

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS covid_data_test ") # TEST

# COMMAND ----------

# MAGIC %md
# MAGIC ## Suppression d'une ligne et mise à jour d'une autre

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE 
# MAGIC FROM default.covid_data
# MAGIC where id = 28

# COMMAND ----------

# MAGIC %sql
# MAGIC update default.covid_data
# MAGIC set confirmed = 66
# MAGIC where id = 442

# COMMAND ----------

# MAGIC %md
# MAGIC > **L'affichage de  l'historique des version de la table covid_data sous forma delta lake qui nous a permet la traçabilité des données**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY default.covid_data

# COMMAND ----------

# MAGIC %md
# MAGIC > **Creation d'une vue temporaire à partir du dataframe covid_update_df**

# COMMAND ----------

Covid_update_df.createOrReplaceTempView('updates')

# COMMAND ----------

# MAGIC %md
# MAGIC > **Merge de Covid_update_df vers la table covid_data pour retrouver la ligne supprimé et remettre à jour la ligne**

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO default.covid_data AS covid_silver
# MAGIC USING updates
# MAGIC ON covid_silver.id = updates.id
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT *

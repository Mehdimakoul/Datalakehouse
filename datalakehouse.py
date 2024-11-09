# Databricks notebook source
# MAGIC %md
# MAGIC # Chargement des données Covid19

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connexion DataBricks avec Azure storage

# COMMAND ----------

storage_account_name = "conatinerdemo23"
sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-11-10T02:51:03Z&st=2024-11-09T18:51:03Z&spr=https,http&sig=YSQ%2Fp68q9SOOiaVBpAOvSfNTtXQJ64VXnU1JVaqtxEo%3D"

containers = {
    "bronze": f"/mnt/bronze",
    "silver": f"/mnt/silver",
    "gold": f"/mnt/gold"
}

def mount_container(container_name, mount_point):
    if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
                source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
                mount_point=mount_point,
                extra_configs={f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
            )
            print(f"Montage réussi sur {mount_point}")
        except Exception as e:
            print(f"Erreur lors du montage du conteneur {container_name}: {e}")
    else:
        print(f"Le conteneur {container_name} est déjà monté sur {mount_point}")

for container, mount_point in containers.items():
    mount_container(container, mount_point)

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

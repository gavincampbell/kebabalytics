# Databricks notebook source
# COMMAND ----------

import urllib

url = "https://github.com/gavincampbell/kebabalytics/raw/refs/heads/main/fixtures/20250709.json"
split = urllib.parse.urlparse(url)
filename = split.path.split("/")[-1]
urllib.request.urlretrieve(url, f"/Volumes/kebabalytics_shared/raw/sales/{filename}")
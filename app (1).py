import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

st.set_page_config(page_title="Prueba BigQuery + Streamlit", page_icon="✅", layout="wide")
st.title("Prueba de conexión a BigQuery")
st.caption("Esta app usa credenciales desde Settings → Secrets (clave: gcp_service_account).")

# Crear cliente BigQuery desde Secrets
@st.cache_resource
def get_bq_client():
    # Asegúrate de haber guardado tus credenciales en Settings → Secrets
    creds_info = dict(st.secrets["gcp_service_account"])
    creds = service_account.Credentials.from_service_account_info(creds_info)
    client = bigquery.Client(credentials=creds, project=creds.project_id)
    return client

@st.cache_data(ttl=600)
def run_query(sql: str) -> pd.DataFrame:
    client = get_bq_client()
    job = client.query(sql)
    return job.result().to_dataframe()

with st.sidebar:
    st.subheader("Config")
    ejemplo = st.selectbox("Consulta de ejemplo", [
        "SELECT 1 AS uno",
        "SELECT CURRENT_DATE() AS hoy",
    ])

st.write("Escribe tu SQL o usa una consulta de ejemplo y haz clic en **Ejecutar**.")
sql = st.text_area("SQL", value=ejemplo, height=120)

if st.button("Ejecutar"):
    try:
        df = run_query(sql)
        st.success("Consulta ejecutada correctamente.")
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error("Error ejecutando la consulta. Revisa los logs y tus Secrets.")
        with st.expander("Detalle del error"):
            st.exception(e)
        st.info("""
Pasos a revisar:
1) Ve a **⋮ → Settings → Secrets** y confirma que existe la sección `[gcp_service_account]` con tu JSON.
2) Verifica que el service account tenga permisos en BigQuery (BigQuery Job User / Data Viewer).
3) Revisa nombres de proyecto/dataset/tabla y sintaxis SQL.
        """)

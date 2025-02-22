"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
import pandas as pd
from zipfile import ZipFile
def clean_campaign_data():
    month_mapping = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }

    input_dir = os.path.join("files", "input")
    zip_files = [file_name for file_name in os.listdir(input_dir)]
    data_frames = []
    
    for zip_file in zip_files:
        zip_path = os.path.join(input_dir, zip_file)
        with ZipFile(zip_path) as zip_ref:
            with zip_ref.open(zip_ref.namelist()[0]) as file:
                df = pd.read_csv(file)
                if "Unnamed: 0" in df.columns:
                    df = df.drop(columns=["Unnamed: 0"])
                data_frames.append(df)

    merged_data = pd.concat(data_frames, ignore_index=True)

    client_df = merged_data[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]]
    client_df["job"] = client_df["job"].str.replace(".", "").str.replace("-", "_")
    client_df["education"] = client_df["education"].replace("unknown", pd.NA)
    client_df["education"] = client_df["education"].apply(lambda x: x.replace("-", "_") if pd.notna(x) else x)
    client_df["education"] = client_df["education"].apply(lambda x: x.replace(".", "_") if pd.notna(x) else x)
    client_df["credit_default"] = client_df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    client_df["mortgage"] = client_df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)

    campaign_df = merged_data[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "month", "day"]]
    campaign_df["month"] = campaign_df["month"].apply(lambda x: month_mapping.get(x.lower(), "00"))
    campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
    
    campaign_df.loc[:, "month"] = campaign_df["month"].astype(str).str.zfill(2)  
    campaign_df.loc[:, "day"] = campaign_df["day"].astype(str).str.zfill(2)  
    
    campaign_df.loc[:, "last_contact_date"] = "2022-" + campaign_df["month"] + "-" + campaign_df["day"]
    campaign_df = campaign_df.drop(["month", "day"], axis=1)

    economic_df = merged_data[["client_id", "cons_price_idx", "euribor_three_months"]]

    output_dir = os.path.join("files", "output")
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    client_df.to_csv(os.path.join(output_dir, "client.csv"), index=False)
    campaign_df.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)
    economic_df.to_csv(os.path.join(output_dir, "economics.csv"), index=False)
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    return


if __name__ == "__main__":
    clean_campaign_data()

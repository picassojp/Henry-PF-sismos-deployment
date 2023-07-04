import requests
import pandas as pd
from pymongo import MongoClient, UpdateOne
import time
import sys
import os


# definición de constantes
#clave para acceder a mongo
password = os.getenv('MONGODB_PASSWORD')

# los paises y los estados (USA) que se buscan filtrar
paises = ["Chile", "Japan"]
us_estados = [
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming",
]

# se establece la url y los parámetros de la request
url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
params = {"format": "geojson", "starttime": "2014-01-01", "endtime": "2014-01-31"}


#### funciones del programa

#función para búsqueda de país dentro de las propiedades de la respuesta de la API
def find_country(place):
    # busca en la lista de paises
    for pais in paises:
        if pais.lower() in place.lower():
            return pais.lower()
    return next(
        ("usa" for estado in us_estados if estado.lower() in place.lower()),
        "other",
    )

def main():
    while True:
        try:
            # Llama a la API
            response = requests.get(url, params=params)
            response.raise_for_status()  # Esto lanza una excepción si la respuesta contiene un código de estado de error HTTP

            # Procesa los datos de la API
            # se guardan los datos de la consulta a la API
            data = response.json()
            
            # se toman los datos del diccionario correspondiente
            features = data["features"]

            filtered_features = []

            for feature in features:
                place = feature["properties"]["place"]
                for location in (
                    paises + us_estados
                ):  # se verifica tanto en países como en estados de EE.UU.
                    if location.lower() in place.lower():
                        filtered_features.append(feature)
                        break  # se termina la busqueda buscar en los otros lugares una vez que encontramos una coincidencia

            # Ahora 'filtered_features' contiene solo los terremotos que ocurrieron en los países y estados especificados

            # Crear una lista vacía para almacenar los datos
            data_list = []

            for feature in filtered_features:
                # Extraer información de properties y geometry
                properties = feature["properties"]
                geometry = feature["geometry"]

                # Crear un diccionario con los datos que necesitamos
                data_dict = {
                    "id": f"{properties['place']}_{properties['mag']}_{properties['time']}",
                    "place": properties["place"],
                    "mag": properties["mag"],
                    "time": pd.to_datetime(properties["time"], unit="ms"),  # convertir el tiempo a formato legible
                    "lon": geometry["coordinates"][0],
                    "lat": geometry["coordinates"][1],
                    "depth": geometry["coordinates"][2],
                }

                # Añadir el diccionario a la lista
                data_list.append(data_dict)

            # Crear un DataFrame a partir de la lista de diccionarios
            df = pd.DataFrame(data_list)

            # se crea la columna con la denominación de cada país según la columna place
            df["country"] = df["place"].apply(find_country)

            try:
                # se conecta a MongoDB
                client = MongoClient(f"mongodb+srv://picassojuanpablo:{password}@cluster0.zet6ttc.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
                db = client["pf-henry"]
                collection = db["api_usgs"]

                # se convierte el DataFrame en una lista de diccionarios para que se pueda almacenar en MongoDB
                data_dict = df.to_dict("records")

                # inserta con la función insert_many() para almacenar los datos en MongoDB
                #collection.insert_many(data_dict)
                
                # Crear una lista de operaciones UpdateOne
                operations = [
                    UpdateOne({"_id": document["_id"]}, {"$set": document}, upsert=True)
                    for document in data_dict
                ]

                # Ejecutar las operaciones con bulk_write
                collection.bulk_write(operations)
                

            except Exception as e:
                print(f"Error al conectarse a MongoDB: {e}", file=sys.stderr)

        except Exception as e:
            print(f"Error al llamar a la API: {e}", file=sys.stderr)

        # Espera una hora antes de la próxima iteración
        time.sleep(3600)


if __name__ == "__main__":
    main()

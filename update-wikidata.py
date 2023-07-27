from wikidataintegrator import wdi_core
import pandas as pd

def update_agencies():

    # Define the SPARQL query
    instance_id = 'P31' # Property for 'instance of'
    subclass_id = 'P279' # Property for 'subclass of'
    value_id = 'Q20857065' # Value for 'United States federal agency'

    query = f"""
    SELECT ?item ?itemLabel ?langAlias WHERE{{  
      ?item p:{instance_id} ?statement0.
      ?statement0 (ps:{instance_id}/(wdt:{subclass_id}*)) wd:{value_id}.
      OPTIONAL {{ ?item skos:altLabel ?langAlias. FILTER (lang(?langAlias) = "en") }}
      ?item rdfs:label ?itemLabel.
      FILTER (lang(?itemLabel) = "en") 
    }}
    """

    # Execute the SPARQL query
    results = wdi_core.WDItemEngine.execute_sparql_query(query)
    
    # Process each result
    data = []
    for result in results["results"]["bindings"]:
        # Extract item, label, and aliases for each entity
        item = result["item"]["value"]
        itemLabel = result["itemLabel"]["value"]
        langAlias = result["langAlias"]["value"] if "langAlias" in result else None
        data.append((item, itemLabel, langAlias))
    
    # Convert the list of dictionaries to a Pandas DataFrame
    df = pd.DataFrame(data, columns=['item', 'itemLabel', 'langAlias'])
    
    # Save the data as a CSV file
    df.to_csv('data/wikidata_agencies.csv', index=False)
    
def update_keys():

    # Define the SPARQL query
    instance_id = 'P31' # Property for 'instance of'
    subclass_id = 'P279' # Property for 'subclass of'
    value_id = 'Q20857065' # Value for 'United States federal agency'

    query = f"""
    SELECT DISTINCT ?item ?itemLabel WHERE{{  
      ?item p:{instance_id} ?statement0.
      ?statement0 (ps:{instance_id}/(wdt:{subclass_id}*)) wd:{value_id}.
      ?item rdfs:label ?itemLabel.
      FILTER (lang(?itemLabel) = "en") 
    }}
    """

    # Execute the SPARQL query
    results = wdi_core.WDItemEngine.execute_sparql_query(query)
    
    # Process each result
    data = []
    for result in results["results"]["bindings"]:
        # Extract item, label, and aliases for each entity
        item = result["item"]["value"]
        itemLabel = result["itemLabel"]["value"]
        data.append((item, itemLabel))
    
    # Convert the list of dictionaries to a Pandas DataFrame
    df = pd.DataFrame(data, columns=['item', 'itemLabel'])
    
    # Save the data as a CSV file
    df.to_csv('data/wikidata_keys.csv', index=False)

update_agencies()
update_keys()
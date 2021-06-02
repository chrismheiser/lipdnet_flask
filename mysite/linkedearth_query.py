#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 30 09:25:09 2018

@author: deborahkhider
@adapted: chrisheiser

Script to batch download LiPD files from the wiki after query
"""

# 1. Query the wiki (note this is taken directly from the Jupyter Notebook on
# GitHub) The query can be changed but doesn't matter in the grand scheme.
import json
import requests
import sys

def query_wiki(opts):
    # %% 1.1 Query terms

    # By archive
    archiveType = opts["archiveType"]

    # By variable
    proxyObsType = opts["proxyObsType"]
    infVarType = opts["infVarType"]

    # By sensor
    sensorGenus = opts["sensorGenus"]
    sensorSpecies = opts["sensorSpecies"]

    # By interpretation
    interpName = opts["interpName"]
    interpDetail = opts["interpDetail"]
    #inferredVariableType : same on wiki

    # Most of the L items on wiki are CORE ontology
    # Other stuff is crowd ontology

    # wiki:Property
    # wiki:Category

    # Everything under search terms or ONLY exact terms?
    # Test by trying to do a query for marine sediment and TREE archiveTypes

    # DONT FORGET SELECT DISTINCT


    # By Age
    ageUnits = opts["ageUnits"]
    ageBound = opts["ageBound"]  # Must enter minimum and maximum age search
    ageBoundType = opts["ageBoundType"]  # Other values include "any", "entire"
    recordLength = opts["recordLength"]

    # By resolution
    # Make sure the resolution makes sense with the age units
    # Will look for records with a max resolution of number entered
    resolution = opts["resolution"]

    # By location
    # Enter latitude boundaries below.
    # If searching for entire latitude band, leave blank.
    # Otherwise, enter both lower and upper bonds!!!!
    # Enter south latitude as negative numbers
    lat = opts["lat"]

    # Enter Longitude boundaries below
    # If searching for entire longitude band, leave blank
    # Otherhwise, enter both lower and upper bonds!!!!
    # Enter west longitude as negative numbers
    lon = opts["lon"]

    # Enter altitude boundaries below
    # If not searching for specific altitude, leave blank
    # Otherwise, enter both lower and upper bonds!!!!
    # Enter depth in the ocean as negative numbers
    # All altitudes on the wiki are in m!
    alt = opts["alt"]

    # %% 1.2 Make sure eveything makes sense
    # Make sure that all conditions are met
    if len(ageBound) == 1:
        sys.exit("You need to provide a minimum and maximum boundary.")

    if ageBound and not ageUnits:
        sys.exit("When providing age limits, you must also enter the units")

    if recordLength and not ageUnits:
        sys.exit("When providing a record length, you must also enter the units")

    if ageBound and ageBound[0] > ageBound[1]:
        ageBound = [ageBound[1], ageBound[0]]

    if not ageBoundType:
        print("No ageBoundType selected, running the query as 'any'")
        ageBoundType = ["any"]

    if len(ageBoundType) > 1:
        sys.exit("Only one search possible at a time.")
        while ageBoundType != "any" and ageBoundType != "entirely" and ageBoundType != "entire":
            print("ageBoundType is not recognized")
            ageBoundType = input("Please enter either 'any', 'entirely', or 'entire': ")

    if recordLength and ageBound and recordLength[0] > (ageBound[1] - ageBound[0]):
        sys.exit("The required recordLength is greater than the provided age bounds")

    if len(resolution) > 1:
        sys.exit("You can only search for a maximum resolution one at a time.")

    if len(lat) == 1:
        sys.exit("Please enter a lower AND upper boundary for the latitude search")

    if lat and lat[1] < lat[0]:
        lat = [lat[1], lat[0]]

    if len(lon) == 1:
        sys.exit("Please enter a lower AND upper boundary for the longitude search")

    if lon and lon[1] < lon[0]:
        lon = [lon[1], lon[0]]

    if len(alt) == 1:
        sys.exit("Please enter a lower AND upper boundary for the altitude search")

    if alt and alt[1] < alt[0]:
        alt = [alt[1], alt[0]]

    # %% 1.3 Query

    url = "http://wiki.linked.earth/store/ds/query"

    query = """PREFIX core: <http://linked.earth/ontology#>
    PREFIX wiki: <http://wiki.linked.earth/Special:URIResolver/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT  distinct ?dataset
    WHERE {
    """

    ### Look for data field
    dataQ = ""
    if archiveType or proxyObsType or infVarType or sensorGenus or sensorSpecies or interpName or interpDetail or ageUnits or ageBound or recordLength or resolution:
        dataQ = "?dataset core:includesChronData|core:includesPaleoData ?data."

    ### Look for variable
    ## measuredVariable
    measuredVarQ = ""
    if proxyObsType or archiveType or sensorGenus or sensorSpecies or interpName or interpDetail or resolution:
        measuredVarQ = "?data core:foundInMeasurementTable / core:includesVariable ?v."

    ## InferredVar
    inferredVarQ = ""
    if infVarType or interpName or interpDetail or resolution:
        inferredVarQ = "?data core:foundInMeasurementTable / core:includesVariable ?v1."

    ### Archive Query
    archiveTypeQ = ""
    if len(archiveType) > 0:
        # add values for the archiveType
        query += "VALUES ?a {"
        for item in archiveType:
            query += "\"" + item + "\" "
        query += "}\n"
        # Create the query
        archiveTypeQ = """
    #Archive Type query
    {
        ?dataset wiki:Property-3AArchiveType ?a.
    }UNION
    {
        ?p core:proxyArchiveType / rdfs:label ?a.
    }"""

    ### ProxyObservationQuery
    proxyObsTypeQ = ""
    if len(proxyObsType) > 0:
        #  add values for the proxyObservationType
        query += "VALUES ?b {"
        for item in proxyObsType:
            query += "\"" + item + "\""
        query += "}\n"
        # Create the query
        proxyObsTypeQ = "?v core:proxyObservationType/rdfs:label ?b."

    ### InferredVariableQuery
    infVarTypeQ = ""
    if len(infVarType) > 0:
        query += "VALUES ?c {"
        for item in infVarType:
            query += "\"" + item + "\""
        query += "}\n"
        # create the query
        infVarTypeQ = """
    ?v1 core:inferredVariableType ?t.
    ?t rdfs:label ?c.
    """
    ### ProxySensorQuery
    sensorQ = ""
    if len(sensorGenus) > 0 or len(sensorSpecies) > 0:
        sensorQ = """
    ?p core:proxySensorType ?sensor.
    """
    ## Genus query
    genusQ = ""
    if len(sensorGenus) > 0:
        query += "VALUES ?genus {"
        for item in sensorGenus:
            query += "\"" + item + "\""
        query += "}\n"
        # create the query
        genusQ = "?sensor core:sensorGenus ?genus."

    ## Species query
    speciesQ = ""
    if len(sensorSpecies) > 0:
        query += "VALUES ?species {"
        for item in sensorSpecies:
            query += "\"" + item + "\""
        query += "}\n"
        # Create the query
        speciesQ = "?sensor core:sensorSpecies ?species."

    ### Proxy system query
    proxySystemQ = ""
    if len(archiveType) > 0 or len(sensorGenus) > 0 or len(sensorSpecies) > 0:
        proxySystemQ = "?v ?proxySystem ?p."

    ### Deal with interpretation
    ## Make sure there is an interpretation to begin with
    interpQ = ""
    if len(interpName) > 0 or len(interpDetail) > 0:
        interpQ = """
    {?v1 core:interpretedAs ?interpretation}
    UNION
    {?v core:interpretedAs ?interpretation}
    """

    ## Name
    interpNameQ = ""
    if len(interpName) > 0:
        query += "VALUES ?intName {"
        for item in interpName:
            query += "\"" + item + "\""
        query += "}\n"
        # Create the query
        interpNameQ = "?interpretation core:name ?intName."

    ## detail
    interpDetailQ = ""
    if len(interpDetail) > 0:
        query += "VALUES ?intDetail {"
        for item in interpDetail:
            query += "\"" + item + "\""
        query += "}\n"
        # Create the query
        interpDetailQ = "?interpretation core:detail ?intDetail."

    ### Age
    ## Units
    ageUnitsQ = ""
    if len(ageUnits) > 0:
        query += "VALUES ?units {"
        for item in ageUnits:
            query += "\"" + item + "\""
        query += "}\n"
        query += """VALUES ?ageOrYear{"Age" "Year"}\n"""
        # create the query
        ageUnitsQ = """
            ?data core:foundInMeasurementTable / core:includesVariable ?v2.
            ?v2 core:inferredVariableType ?aoy.
            ?aoy rdfs:label ?ageOrYear.
            ?v2 core:hasUnits ?units .
            """
    ## Minimum and maximum
    ageQ = ""
    if ageBoundType[0] == "entirely":
        if len(ageBound) > 0 and len(recordLength) > 0:
            ageQ = """
                ?v2 core:hasMinValue ?e1.
                ?v2 core:hasMaxValue ?e2.
                filter(?e1<=""" + str(ageBound[0]) + """&& ?e2>=""" + str(ageBound[1]) + """ && abs(?e1-?e2)>=""" + str(
                            recordLength[0]) + """).
                """
        elif len(ageBound) > 0 and len(recordLength) == 0:
            ageQ = """
                ?v2 core:hasMinValue ?e1.
                ?v2 core:hasMaxValue ?e2.
                filter(?e1<=""" + str(ageBound[0]) + """&& ?e2>=""" + str(ageBound[1]) + """).
                """
    elif ageBoundType[0] == "entire":
        if len(ageBound) > 0 and len(recordLength) > 0:
            ageQ = """
                ?v2 core:hasMinValue ?e1.
                ?v2 core:hasMaxValue ?e2.
                filter(?e1>=""" + str(ageBound[0]) + """&& ?e2<=""" + str(ageBound[1]) + """ && abs(?e1-?e2)>=""" + str(
                            recordLength[0]) + """).
                """
        elif len(ageBound) > 0 and len(recordLength) == 0:
            ageQ = """
                ?v2 core:hasMinValue ?e1.
                ?v2 core:hasMaxValue ?e2.
                filter(?e1>=""" + str(ageBound[0]) + """&& ?e2<=""" + str(ageBound[1]) + """).
                """
    elif ageBoundType[0] == "any":
        if len(ageBound) > 0 and len(recordLength) > 0:
            ageQ = """
                ?v2 core:hasMinValue ?e1.
                filter(?e1<=""" + str(ageBound[1]) + """ && abs(?e1-""" + str(ageBound[1]) + """)>=""" + str(recordLength[0]) + """).
                """
        elif len(ageBound) > 0 and len(recordLength) == 0:
            ageQ = """
                ?v2 core:hasMinValue ?e1.
                filter(?e1<=""" + str(ageBound[1]) + """).
                """

            ### Resolution
    resQ = ""
    if len(resolution) > 0:
        resQ = """
            {
            ?v core:hasResolution/(core:hasMeanValue |core:hasMedianValue) ?resValue.
            filter (xsd:float(?resValue)<100)
            }
            UNION
            {
            ?v1 core:hasResolution/(core:hasMeanValue |core:hasMedianValue) ?resValue1.
            filter (xsd:float(?resValue1)<""" + str(resolution[0]) + """)
            }
            """

    ### Location
    locQ = ""
    if lon or lat or alt:
        locQ = "?dataset core:collectedFrom ?z."

    ## Altitude
    latQ = ""
    if len(lat) > 0:
        latQ = """
            ?z <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?lat.
            filter(xsd:float(?lat)<""" + str(lat[1]) + """ && xsd:float(?lat)>""" + str(lat[0]) + """).
            """

    ##Longitude
    lonQ = ""
    if len(lon) > 0:
        lonQ = """
            ?z <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?long.
            filter(xsd:float(?long)<""" + str(lon[1]) + """ && xsd:float(?long)>""" + str(lon[0]) + """).
            """

    ## Altitude
    altQ = ""
    if len(alt) > 0:
        altQ = """
            ?z <http://www.w3.org/2003/01/geo/wgs84_pos#alt> ?alt.
            filter(xsd:float(?alt)<""" + str(alt[1]) + """ && xsd:float(?alt)>""" + str(alt[0]) + """).
            """

    query += """
        ?dataset a core:Dataset.
        """ + dataQ + """
        """ + measuredVarQ + """
        # By proxyObservationType
        """ + proxyObsTypeQ + """
        """ + inferredVarQ + """
        # By InferredVariableType
        """ + infVarTypeQ + """
        # Look for the proxy system model: needed for sensor and archive queries
        """ + proxySystemQ + """
        # Sensor query
        """ + sensorQ + """
        """ + genusQ + """
        """ + speciesQ + """
        # Archive query (looks in both places)
        """ + archiveTypeQ + """
        # Interpretation query
        """ + interpQ + """
        """ + interpNameQ + """
        """ + interpDetailQ + """
        # Age Query
        """ + ageUnitsQ + """
        """ + ageQ + """
        # Location Query
        """ + locQ + """
        #Latitude
        """ + latQ + """
        #Longitude
        """ + lonQ + """
        #Altitude
        """ + altQ + """
        #Resolution Query
        """ + resQ + """
        }"""

    # Store the query items into a list
    response = requests.post(url, data={'query': query})
    res = json.loads(response.text)

    # Get a list of the query results. These are links to the dataset main page
    results = []
    for item in res['results']['bindings']:
        results.append(item['dataset']['value'])
    print(results)

    # Isolate the dataset name from the full link
    dl_link = []
    dataset_name = []
    for idx, temp in enumerate(results):
        dataset_name.append(temp.split('/')[-1])
        # Use the URL base for wiki download links, and build on the datasetname
        dl_link.append('http://wiki.linked.earth/wiki/index.php/Special:WTLiPD?op=export&lipdid=' + dataset_name[idx])




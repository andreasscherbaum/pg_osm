\set ON_ERROR_STOP true

-- https://nominatim.org/release-docs/develop/api/Search/
CREATE OR REPLACE FUNCTION osm_query_simple(query TEXT)
RETURNS json
LANGUAGE plpython3u
AS $$
    import http.client
    import json
    import urllib.parse
    import urllib.request

    keys = ['lat', 'lon', 'name', 'display_name', 'place_id',
            'osm_type', 'osm_id', 'class', 'type', 'place_rank',
            'importance', 'addresstype', 'licence']

    base_url = "https://nominatim.openstreetmap.org/search"

    params = {
        'format': 'json',
        'addressdetails': 1,
        'q': query,
    }
    query_url = base_url + '?' + urllib.parse.urlencode(params)

    try:
        with urllib.request.urlopen(query_url) as response:
            decoded_response = response.read().decode('utf-8')
            #plpy.info(decoded_response)
            data = json.loads(decoded_response)
            #plpy.debug(data)
    except urllib.error.URLError as e:
        print(f"Error: {e}")
        return None

    ret = {}
    # only interested in the first data set
    data = data[0]
    for k in keys:
        if (k in data):
            ret[k] = data[k]
        else:
            # make sure the key exists in the return data set
            ret[k] = ''
    if ('address' in data):
        ret['address'] = data['address']
    else:
        ret['address'] = {}
    if ('boundingbox' in data):
        ret['boundingbox'] = data['boundingbox']
    else:
        ret['boundingbox'] = []

    #plpy.debug(ret)

    return json.dumps(ret)
$$;

-- https://nominatim.openstreetmap.org/
-- SELECT osm_query_simple('Belgrave Square 23, SW1X 8PZ London, United Kingdom');
-- SELECT osm_query_simple('Embassy of Germany, London, United Kingdom');


CREATE OR REPLACE FUNCTION osm_query(query TEXT)
RETURNS SETOF json
LANGUAGE plpython3u
AS $$
    import http.client
    import json
    import urllib.parse
    import urllib.request

    keys = ['lat', 'lon', 'name', 'display_name', 'place_id',
            'osm_type', 'osm_id', 'class', 'type', 'place_rank',
            'importance', 'addresstype', 'licence']

    base_url = "https://nominatim.openstreetmap.org/search"

    params = {
        'format': 'json',
        'addressdetails': 1,
        'q': query,
    }
    query_url = base_url + '?' + urllib.parse.urlencode(params)

    try:
        with urllib.request.urlopen(query_url) as response:
            decoded_response = response.read().decode('utf-8')
            #plpy.info(decoded_response)
            data = json.loads(decoded_response)
            #plpy.debug(data)
    except urllib.error.URLError as e:
        print(f"Error: {e}")
        return None

    for row in data:
        ret = {}
        for k in keys:
            if (k in row):
                ret[k] = row[k]
            else:
                # make sure the key exists in the return data set
                ret[k] = ''
        if ('address' in row):
            ret['address'] = row['address']
        else:
            ret['address'] = []
        if ('boundingbox' in row):
            ret['boundingbox'] = row['boundingbox']
        else:
            ret['boundingbox'] = []
        #plpy.debug(ret)
        yield json.dumps(ret)

    return
$$;

-- \x
-- SELECT * FROM osm_query('Embassy of Germany');

CREATE OR REPLACE FUNCTION osm_query_address(query TEXT DEFAULT '',
                                             street TEXT DEFAULT '',
                                             house_number TEXT DEFAULT '',
                                             city TEXT DEFAULT '',
                                             zip_code TEXT DEFAULT '',
                                             country TEXT DEFAULT '',
                                             state TEXT DEFAULT '',
                                             county TEXT DEFAULT '')
RETURNS SETOF json
LANGUAGE plpython3u
AS $$
    import http.client
    import json
    import urllib.parse
    import urllib.request

    keys = ['lat', 'lon', 'name', 'display_name', 'place_id',
            'osm_type', 'osm_id', 'class', 'type', 'place_rank',
            'importance', 'addresstype', 'licence']

    base_url = "https://nominatim.openstreetmap.org/search"

    params = {
        'format': 'json',
        'addressdetails': 1,
    }

    structured_query = False
    if (country is not None and len(country) > 0):
        params['country'] = country
        structured_query = True

    if (state is not None and len(state) > 0):
        params['state'] = state
        structured_query = True

    if (county is not None and len(county) > 0):
        params['county'] = county
        structured_query = True

    if (city is not None and len(city) > 0):
        params['city'] = city
        structured_query = True

    if (zip_code is not None and len(zip_code) > 0):
        params['postalcode'] = zip_code
        structured_query = True

    if (street is not None and len(street) > 0):
        local_street = street
        if (house_number is not None and len(house_number) > 0):
            local_street = "{hn} {s}".format(hn = house_number, s = street)
        params['street'] = local_street
        structured_query = True

    if (structured_query is True):
        # this is a structured query, do not use 'q=' but use 'amenity=' instead
        if (len(query) > 0):
            params['amenity'] = query
    else:
        # this is a freestyle unstructured query
        if (len(query) > 0):
            params['q'] = query
        else:
            plpy.info("Please specify a query string!")
            return None
    if (len(params) == 0):
        plpy.info("No query string provided!")
        return None

    query_url = base_url + '?' + urllib.parse.urlencode(params)
    plpy.info(query_url)

    try:
        with urllib.request.urlopen(query_url) as response:
            decoded_response = response.read().decode('utf-8')
            #plpy.info(decoded_response)
            data = json.loads(decoded_response)
            #plpy.debug(data)
    except urllib.error.URLError as e:
        print(f"Error: {e}")
        return None

    for row in data:
        ret = {}
        for k in keys:
            if (k in row):
                ret[k] = row[k]
            else:
                # make sure the key exists in the return data set
                ret[k] = ''
        if ('address' in row):
            ret['address'] = row['address']
        else:
            ret['address'] = []
        if ('boundingbox' in row):
            ret['boundingbox'] = row['boundingbox']
        else:
            ret['boundingbox'] = []
        #plpy.debug(ret)
        yield json.dumps(ret)

    return
$$;

-- \x
-- SELECT osm_query_address(country => 'United Kingdom', zip_code => 'SW1X 8PZ', city => 'London', street => 'Belgrave Square', house_number => '23');

-- WITH data_query AS (
--     SELECT osm_query_address(country => 'United Kingdom', zip_code => 'SW1X 8PZ', city => 'London', street => 'Belgrave Square', house_number => '23') AS data
-- )
-- SELECT data->'lat', data->'lon'
--   FROM data_query;

-- WITH data_query AS (
--     SELECT osm_query_address(country => 'United Kingdom',
--                              zip_code => 'SW1X 8PZ',
--                              city => 'London',
--                              street => 'Belgrave Square',
--                              house_number => '23') AS data
-- )
-- SELECT data
--   FROM data_query;

-- WITH data_query AS (
--     SELECT osm_query_address(query => 'Embassy of Germany',
--                              zip_code => 'SW1X 8PZ',
--                              city => 'London',
--                              street => 'Belgrave Square',
--                              house_number => '23') AS data
-- )
-- SELECT data
--   FROM data_query;

-- WITH data_query AS (
--     SELECT osm_query_address(query => 'Embassy of Germany',
--                              country => 'United Kingdom',
--                              city => 'London') AS data
-- )
-- SELECT data
--   FROM data_query;


-- https://nominatim.org/release-docs/develop/api/Reverse/
CREATE OR REPLACE FUNCTION osm_query_reverse(lat FLOAT, lon FLOAT,
                                             zoom INTEGER DEFAULT 10,
                                             extratags INTEGER DEFAULT 0,
                                             namedetails INTEGER DEFAULT 0)
RETURNS json
LANGUAGE plpython3u
AS $$
    import http.client
    import json
    import urllib.parse
    import urllib.request

    keys = ['lat', 'lon', 'name', 'display_name', 'place_id',
            'osm_type', 'osm_id', 'class', 'type', 'place_rank',
            'importance', 'addresstype', 'licence']

    base_url = "https://nominatim.openstreetmap.org/reverse"

    if (zoom < 3 or zoom > 18):
        plpy.error("Zoom is outside the allowed range (3 .. 18)!")
    if (extratags != 0 and extratags != 1):
        plpy.error("extratags must be 0 or 1!")
    if (namedetails != 0 and namedetails != 1):
        plpy.error("namedetails must be 0 or 1!")

    params = {
        'format': 'json',
        'lat': lat,
        'lon': lon,
        'addressdetails': 1,
        'extratags': extratags,
        'namedetails': namedetails,
        'zoom': zoom,
    }

    query_url = base_url + '?' + urllib.parse.urlencode(params)
    plpy.info(query_url)

    try:
        with urllib.request.urlopen(query_url) as response:
            decoded_response = response.read().decode('utf-8')
            #plpy.info(decoded_response)
            data = json.loads(decoded_response)
            #plpy.debug(data)
    except urllib.error.URLError as e:
        print(f"Error: {e}")
        return None

    if ('error' in data and data['error'] == 'Unable to geocode'):
        return None
    #plpy.info(json.dumps(data))

    ret = {}
    for k in keys:
        if (k in data):
            ret[k] = data[k]
        else:
            # make sure the key exists in the return data set
            ret[k] = ''
    if ('address' in data):
        ret['address'] = data['address']
    else:
        ret['address'] = {}
    if ('boundingbox' in data):
        ret['boundingbox'] = data['boundingbox']
    else:
        ret['boundingbox'] = []
    if ('extratags' in data):
        ret['extratags'] = data['extratags']
    else:
        ret['extratags'] = {}
    if ('namedetails' in data):
        ret['namedetails'] = data['namedetails']
    else:
        ret['namedetails'] = {}

    #plpy.debug(ret)

    return json.dumps(ret)
$$;

-- SELECT * FROM osm_query_reverse(ST_SetSRID(ST_MakePoint('-0.154194'::FLOAT, '51.498126'::FLOAT),4326)::geography);
-- SELECT * FROM osm_query_reverse(51.498126, -0.154194, zoom => 3);
-- SELECT * FROM osm_query_reverse(0.00, -0.00);

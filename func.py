import io
import json
import logging
import pandas
import base64

from fdk import response

import oci
from oci.ai_language.ai_service_language_client import AIServiceLanguageClient

def handler(ctx, data: io.BytesIO=None):

    signer = oci.auth.signers.get_resource_principals_signer()
    resp = do(signer,data)
    return response.Response(
        ctx, response_data=resp,
        headers={"Content-Type": "application/json"}  
    )
    
def nr(dip, txt):
    details = oci.ai_language.models.DetectLanguageEntitiesDetails(text=txt)
    le = dip.detect_language_entities(detect_language_entities_details=details)
    return json.loads(le.data.entities.__repr__())
    
def do(signer, data):
    dip = AIServiceLanguageClient(config={}, signer=signer)
    
    body = json.loads(data.getvalue())
    input_parameters = body.get("parameters")
    col = input_parameters.get("column")
    input_data = base64.b64decode(body.get("data")).decode()
    df = pandas.read_json(input_data, lines=True)
    df[’enr’] = df.apply(lambda row : nr(dip,row[col]), axis = 1)
    #Explode the array of entities into row per entity
    dfe = df.explode(’enr’,True)
    #Add a column for each property we want to return from entity struct
    ret=pandas.concat([dfe,pandas.DataFrame((d for idx, d in dfe[’enr’].iteritems()))], axis=1)
    
    #Drop array of entities column
    ret = ret.drop([’enr’],axis=1)
    ret = ret.drop([col],axis=1)
    
    str=ret.to_json(orient=’records’)
    return str

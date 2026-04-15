import sys
import os
import json
import re
import csv
from io import BytesIO, TextIOWrapper
from pathlib import Path
import boto3
from langchain.agents import create_agent
from langchain.messages import SystemMessage, AIMessage, HumanMessage, ToolMessage
from langchain_core.messages.base import BaseMessage
from langchain_core.tools.base import BaseTool
from langchain_openai import ChatOpenAI
from pydantic import SecretStr
from dotenv import load_dotenv


s3 = boto3.client('s3')

load_dotenv(override=False)

LLM_MODEL='google/gemini-2.5-flash-lite'


def get_s3_parts(s3url):
    return s3url.replace('s3://', '').split('/', 1)


def put_empty_csv_result(bucket: str, key: str) -> None:
    with (BytesIO() as buf, TextIOWrapper(buf, encoding='utf-8', newline='') as f):
        writer = csv.DictWriter(f, fieldnames=['city', 'country', 'zip'])
        writer.writeheader()
        f.flush()

        s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def handler(event, context):

    # key for llm service doesn't exist
    API_KEY = os.getenv('OPENROUTER_API_KEY')
    if API_KEY is None:
        raise ValueError('Env var API_KEY is not set')

    # input params: input csv w/ city and country and output csv result folder
    s3_missing_zipcodes = event['missing_zipcodes_s3']
    s3_output_folder = event['output_folder_s3']

    # read input csv file
    # Get the last CSV file in the s3_missing_zipcodes folder
    buck, prefix = get_s3_parts(s3_missing_zipcodes)
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    resp = s3.list_objects_v2(Bucket=buck, Prefix=prefix)
    csv_keys = [obj['Key'] for obj in resp.get('Contents', []) ]
    if not csv_keys:
        raise FileNotFoundError("No missing zipcodes csv files found in the S3 folder.")
    last_csv_key = sorted(csv_keys)[-1]
    csv_obj = s3.get_object(Bucket=buck, Key=last_csv_key)['Body'].read().decode('utf-8')


    # target result csv file
    buck, key = get_s3_parts(s3_output_folder)
    key = str(Path(key) / 'missing_zipcodes_result.csv')

    # if empty csv file / only header
    # produce empty result
    if len(csv_obj.splitlines()) == 1: 
        put_empty_csv_result(buck, key)
        return

    # create llm model 
    model = ChatOpenAI(
        model=LLM_MODEL,
        temperature=0.4,
        max_retries=2,
        timeout=180,
        base_url="https://openrouter.ai/api/v1",
        api_key=SecretStr(API_KEY),
        reasoning={ "effort": "low" },
    )

    # tools: dict[str, BaseTool] = { }

    ss_websearch = { "type": "web_search" }

    system_instructions = SystemMessage(re.sub(r'\s+', ' ', r"""
    You are a data engineer. Be concise and accurate.
    Provide direct answers only. Do not explain your reasoning.
    Do not include introductory text, supplementary information, subheadings, 
    conversational filler, closing notes, context not explicitly requested.
    """).strip() )


    agent = create_agent(
        model,
        tools=[ss_websearch],
        system_prompt=system_instructions
    )

    query = re.sub(r'\s+', ' ', r"""
    There is a csv data with city and country columns. 
    Find a corresponding zip code or postal code for each city in this csv data.
    Generate a new csv data with columns: city, country, zip.
    If there is no zip code use postal code. If neither use N/A.
    For multiple codes use the most common one (central code).
    Some city names may contain typos.
    Do not fix the city and country names in the new csv output.
    Return only raw CSV content. Follow RFC 4180 standard.
    """).strip()


    h_msg = HumanMessage(
        content_blocks=[
            { "type": "text", "text": query },
            { "type": "text", "text": csv_obj }
        ]
    )


    resp = agent.invoke({"messages": [h_msg]})

    result = resp['messages'][-1].content

    txt = next((item for item in result if item['type'] == 'text'), None)
    
    if txt is not None: 
        s3.put_object(Bucket=buck, Key=key, Body=txt['text'].encode('utf-8') )
    else:
        # empty csv 
        put_empty_csv_result(buck, key)


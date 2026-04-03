import pprint
import sys
import os
import json
from pathlib import Path
from langchain.agents import create_agent
from langchain.messages import SystemMessage, AIMessage, HumanMessage, ToolMessage
from langchain_core.messages.base import BaseMessage
from langchain_core.tools.base import BaseTool
from langchain_openai import ChatOpenAI
from pydantic import SecretStr
from dotenv import load_dotenv

# csv data file
fpath = '../output/empty_zip.csv/part-00000-692767aa-9df7-4be5-a017-3f2d535b255b-c000.csv'

load_dotenv()

LLM_MODEL='google/gemini-2.5-flash-lite'


model = ChatOpenAI(
    model=LLM_MODEL,
    temperature=0.4,
    max_retries=2,
    timeout=180,
    base_url="https://openrouter.ai/api/v1",
    api_key=SecretStr(os.getenv('OPENROUTER_API_KEY') or ""),
    reasoning={ "effort": "low" },
)

# tools: dict[str, BaseTool] = { }

ss_websearch = { "type": "web_search" }

system_instructions = SystemMessage(r"""
You are a data engineer. Be concise and accurate.
Provide direct answers only. Do not explain your reasoning.
Do not include introductory text, supplementary information, subheadings, 
conversational filler, closing notes, context not explicitly requested.
""".replace("\n", ' ').strip())


agent = create_agent(
    model,
    tools=[ss_websearch],
    system_prompt=system_instructions
)

query = r"""
There is a csv data with city and country columns. 
Find a corresponding zip code or postal code for each city in this csv data.
Generate a new csv data with columns: city, country, zip.
If there is no zip code use postal code. If neither use N/A.
For multiple codes use the most common one (central code).
Some city names may contain typos.
Do not fix the city and country names in the new csv output.
Return only raw CSV content. Follow RFC 4180 standard.
""".replace("\n", ' ').strip()


input_csv = Path(fpath)
if not input_csv.exists():
    print(f"Input csv file doesn't exist: {input_csv.absolute()}", file=sys.stderr)
    sys.exit(-1) 


# from io import StringIO
# f = StringIO(input_csv.read_text())
# csv_content = '\n'.join([ f.readline() for _ in range(3) ])
csv_content = input_csv.read_text()


h_msg = HumanMessage(
    content_blocks=[
        { "type": "text", "text": query },
        { "type": "text", "text": csv_content }
    ]
)


resp = agent.invoke({"messages": [h_msg]})

result = resp['messages'][-1].content

txt = next((item for item in result if item['type'] == 'text'), None)
if txt is not None: 
    Path('result.csv').write_text(txt['text'])


